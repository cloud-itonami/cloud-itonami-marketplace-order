(ns orderops.edge.worker
  "The HOST for the order actor — the half `marketplace.persist` refuses
  to be.

  `org-database-policy.edn` puts D1 transport, ref selection, CACAO,
  encryption, blind indexing, visibility policy and secrets in the host,
  and leaves datoms, queries and domain schema to the application.
  `marketplace.persist/store` throws without an injected database API so
  the actor cannot come up durable-looking while writing to nothing.
  This namespace is what does the injecting.

  ## What is NOT yet in place, stated plainly

  `kotobase.storage.d1-worker/database` supplies the engine's four
  required security controls itself, and its Worker-path defaults are
  PERMISSIVE: `encrypt-fn`/`decrypt-fn` are identity, `visible?` is
  `(constantly true)`, and `blind-fn` just prints. So the data here is
  protected by D1's own at-rest encryption and by this Worker's auth
  gate — NOT by application-level encryption, blind indexing or a
  visibility policy, even though the policy names those as host
  responsibilities.

  That is a real gap, not a design choice. Closing it means passing real
  controls into `engine/open`, which the provider does not currently
  expose a seam for. Until then nothing here should hold data that needs
  more than transport-level protection, and this actor holds order
  records with a redacted buyer reference — no addresses, no contact
  details — which is why it is a defensible place to start.

  ## load -> compute -> flush

  The D1 provider (`kotobase.storage.d1-worker`) is Promise-based; the
  actor is synchronous, and making it async would ripple through every
  `Store` protocol in the fleet for no benefit. So each request:

    1. `await` the current state out of D1
    2. run the actor synchronously against a recording db-api
    3. `await` ONE transact of everything it wrote

  One transact, so a request's writes land together or not at all, and
  concurrent requests are made safe by kotobase's own
  `kotobase_refs.revision` CAS rather than by anything invented here.

  ## Writes are gated, reads are not

  Anyone may read an order back. Placing one needs a bearer token
  (`ORDER_WRITE_TOKEN`, a Cloudflare secret) — a public unauthenticated
  write endpoint on a durable store is not a demo, it is an invitation.

  Bracket access (`aget`) throughout for `:advanced-optimization`
  safety."
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [kotobase.storage.d1-worker :as d1]
            [marketplace.order :as order]
            [marketplace.persist :as persist]
            [orderops.advisor :as advisor]
            [orderops.governor :as governor]
            [orderops.phase :as phase]
            [orderops.store :as store]))

(def ^:private ref-name "cloud-itonami/marketplace-order/production")

(defn- json [body status]
  (js/Response. (js/JSON.stringify (clj->js body))
                #js {:status status
                     :headers #js {"content-type" "application/json; charset=utf-8"
                                   "cache-control" "no-store"}}))

;; ───────────────────── the async boundary ─────────────────────

(defn- load-tx
  "Everything this actor has ever written, as flat tx-data.

  `datomsD1` is the whole-database read; the actor's own `:mp.<kind>/`
  attribute scoping is what keeps another actor's documents out of its
  view when they share a database."
  [db]
  (-> (d1/datoms-edn! db ref-name (pr-str {}))
      (.then (fn [edn]
               (let [ds (reader/read-string (str edn))]
                 (vec (if (map? ds) (:datoms ds) ds)))))
      (.catch (fn [_] []))))

(defn- flush-tx!
  "One transact of everything the request wrote. Nothing when it wrote
  nothing — an empty transact would still bump the ref revision and make
  a read look like a write in the audit trail."
  [db tx-data]
  (if (seq tx-data)
    (d1/transact-edn! db ref-name (pr-str (vec tx-data)))
    (js/Promise.resolve nil)))

(defn- ->attr
  "Attributes come back from the store as STRINGS -- `kotobase-peer`'s
  `->quad` does `(str a)`, so `:mp.offer/id` is written and
  `\":mp.offer/id\"` is read. Without turning it back into a keyword the
  seeded entities are keyed by string while `get-doc` queries by
  keyword, every lookup misses, and the actor concludes the catalog is
  empty -- which is exactly what it did on the first live request."
  [a]
  (cond
    (keyword? a) a
    (string? a)  (keyword (if (str/starts-with? a ":") (subs a 1) a))
    :else        (keyword (str a))))

(defn- datoms->tx
  "Normalise what the store hands back into the same `[e a v]` triples
  `persist` writes, so a round trip is symmetric.

  The actual read shape, confirmed against the live database rather than
  assumed, is `{:e \"mp.seller/x\" :a \":mp.seller/id\" :v_edn
  \"\\\"x\\\"\" :added true}` — the attribute is a STRING and the value is
  EDN-ENCODED. Two conversions are therefore load-bearing:

    - `:a` back to a keyword, or the seeded entities are keyed by string
      while `get-doc` queries by keyword and every lookup misses
    - `:v_edn` read as EDN, or every value arrives wrapped in its own
      quotes and no comparison matches

  Both were guessed wrong first; this is what the store actually
  returns. `:added false` retractions are dropped."
  [datoms]
  (vec
   (keep (fn [d]
           (cond
             (and (map? d) (contains? d :v_edn))
             (when (not= false (:added d))
               [(str (:e d)) (->attr (:a d))
                (try (reader/read-string (str (:v_edn d)))
                     (catch :default _ (:v_edn d)))])

             (and (map? d) (:s d))                 [(str (:s d)) (->attr (:p d)) (:o d)]
             (and (map? d) (contains? d :e))       [(str (:e d)) (->attr (:a d)) (:v d)]
             (and (sequential? d) (= 3 (count d))) (let [[e a v] d] [(str e) (->attr a) v])
             (and (sequential? d) (= 4 (count d))) (let [[_ e a v] d] [(str e) (->attr a) v])
             :else nil))
         datoms)))

;; ───────────────────── running the actor ─────────────────────

(defn- run-actor
  "One synchronous actor pass over a seeded store. Returns
  `{:disposition .. :audit [..] :store st}`.

  The full langgraph StateGraph is `orderops.operation`, run by the
  operator against this same store; the edge runs the SAME governor and
  phase gate on a single request, which is the structural subset that
  makes sense at request time — the shape `partners.edge.intake`
  documents."
  [st context request]
  (let [proposal (advisor/-advise (advisor/mock-advisor) st request)
        verdict (governor/check request context proposal st)
        base (phase/verdict->disposition verdict)
        {:keys [disposition reason]} (phase/gate (:phase context) request base)]
    (case disposition
      :commit
      (let [record {:op (:op proposal) :order-id (:order-id request)
                    :value (:value proposal) :payload (:value proposal)}]
        (store/commit-record! st record)
        (store/append-ledger! st {:t :committed :op (:op request)
                                  :order-id (:order-id request)})
        {:disposition :commit :verdict verdict})

      :escalate
      (do (store/append-ledger! st {:t :approval-requested :op (:op request)
                                    :order-id (:order-id request)
                                    :reason (or reason :high-stakes)})
          {:disposition :escalate :verdict verdict :reason reason})

      (do (store/append-ledger! st (governor/hold-fact request context verdict))
          {:disposition :hold :verdict verdict :reason reason}))))

(defn- with-store
  "load -> f -> flush. `f` gets a durable-backed store and returns a map."
  [db f]
  (-> (load-tx db)
      (.then (fn [datoms]
               (let [api (persist/recording-db-api (datoms->tx datoms))
                     st (store/kotobase-store {:db-api api})
                     result (f st)]
                 (-> (flush-tx! db (persist/recorded api))
                     (.then (fn [_] (assoc result :flushed (count (persist/recorded api)))))))))))

;; ───────────────────── routes ─────────────────────

(defn- authorised? [request env]
  (let [want (aget env "ORDER_WRITE_TOKEN")
        got (some-> (.get (aget request "headers") "authorization")
                    (str/replace #"^Bearer " ""))]
    (and want (not (str/blank? (str got))) (= want got))))

(defn- place-order [db body]
  (with-store
    db
    (fn [st]
      (let [req {:op :place-order
                 :order-id (get body "order-id")
                 :patch {:buyer (get body "buyer")
                         :lines (mapv (fn [l] {:offer-id (get l "offer-id")
                                               :qty (get l "qty")})
                                      (get body "lines" []))}}
            ctx {:actor-id "orderops-edge" :phase 3
                 :now (get body "now" "2026-06-01T00:00:00Z")
                 :needs-shipping? (get body "needs-shipping" true)
                 :destination (get body "destination")}
            out (run-actor st ctx req)]
        {:order-id (:order-id req)
         :disposition (:disposition out)
         :violations (mapv :rule (get-in out [:verdict :violations]))
         :durable? (store/durable? st)}))))

(defn- read-order [db oid]
  (with-store
    db
    (fn [st]
      (if-let [o (store/order-record st oid)]
        {:order-id oid
         :buyer (:order/buyer o)
         :sellers (:order/sellers o)
         :currency (:order/currency o)
         :total-minor (order/total-minor o)
         :status (order/overall-status o)
         :fully-delivered? (order/fully-delivered? o)
         :durable? (store/durable? st)}
        {:error "unknown order" :order-id oid}))))

(defn- seed-reference-data [db]
  (with-store
    db
    (fn [st]
      (let [d (store/demo-data)]
        (doseq [[k m] [[:buyer :buyer/id] [:seller :seller/id] [:offer :offer/id]]]
          (doseq [v (vals (get d (case k :buyer :buyers :seller :sellers :offer :offers)))]
            (persist/put-doc! (persist/ctx (:st st) k m) v)))
        {:seeded {:buyers (count (:buyers d))
                  :sellers (count (:sellers d))
                  :offers (count (:offers d))}}))))

(defn- handle [request env]
  (let [url (js/URL. (aget request "url"))
        path (aget url "pathname")
        method (aget request "method")]
    (cond
      (= path "/debug/datoms")
      ;; Diagnostic: the RAW shape D1 hands back, before and after
      ;; normalisation. Kept because guessing at a store's datom shape
      ;; across a build-and-deploy cycle is far more expensive than
      ;; looking at it once -- but GATED, because it dumps seller and
      ;; buyer records and an unauthenticated dump of those is a leak,
      ;; not a debugging aid.
      (if-not (authorised? request env)
        (js/Promise.resolve (json {:error "unauthorised"} 401))
        (-> (d1/datoms-edn! (aget env "DB") ref-name (pr-str {}))
          (.then (fn [edn]
                   (let [ds (reader/read-string (str edn))
                         ds (if (map? ds) (:datoms ds) ds)
                         norm (datoms->tx ds)]
                     (json {:count (count ds)
                            :raw-sample (mapv pr-str (take 4 ds))
                            :normalised-sample (mapv pr-str (take 4 norm))}
                           200))))
            (.catch (fn [e] (json {:error (str e)} 500)))))

      (= path "/health")
      (-> (load-tx (aget env "DB"))
          (.then (fn [ds] (json {:ok true :service "cloud-itonami-marketplace-order"
                                 :store "cloudflare-d1 / kotobase.core"
                                 :ref ref-name
                                 :datoms-loaded (count ds)}
                                200)))
          (.catch (fn [e] (json {:ok false :error (str e)} 500))))

      (and (= method "POST") (= path "/admin/seed"))
      (if-not (authorised? request env)
        (js/Promise.resolve (json {:error "unauthorised"} 401))
        (-> (seed-reference-data (aget env "DB"))
            (.then #(json % 200))
            (.catch (fn [e] (json {:error (str e)} 500)))))

      (and (= method "POST") (= path "/orders"))
      (if-not (authorised? request env)
        (js/Promise.resolve (json {:error "unauthorised"} 401))
        (-> (.json request)
            (.then (fn [b] (place-order (aget env "DB") (js->clj b))))
            (.then #(json % 200))
            (.catch (fn [e] (json {:error (str e)} 500)))))

      (and (= method "GET") (.startsWith path "/orders/"))
      (-> (read-order (aget env "DB") (.slice path (count "/orders/")))
          (.then (fn [r] (json r (if (:error r) 404 200))))
          (.catch (fn [e] (json {:error (str e)} 500))))

      :else (js/Promise.resolve (json {:error "not found"} 404)))))

(def app
  (clj->js {:fetch (fn [request env _ctx] (handle request env))}))

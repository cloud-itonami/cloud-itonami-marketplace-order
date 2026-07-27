(ns orderops.edge.worker
  "The HOST for the order actor — talking to kotobase.net through the
  real client, not around it.

  ## What this replaced, and why

  The first version of this namespace embedded `kotobase-storage-d1` and
  drove a Cloudflare D1 binding directly. It worked, and it was the
  wrong layer. It bypassed the service's authorization entirely (no
  CACAO, no tenant isolation), it used only `transact` and `datoms`
  while hand-rolling a two-pattern query stub in place of `q`/`pull`,
  and it loaded the WHOLE database on every request. Calling that
  \"implemented on the kotobase Datomic API\" was generous at best.

  `kotoba-lang/kotobase-client` already exists and does this properly:
  `q` / `pull` / `datoms` / `transact` over
  `kotobase.net/xrpc/ai.gftd.apps.kotobase.datomic.*`, minting a CACAO
  per request with a fresh nonce, retrying transient 5xx, and carrying
  the graph scope the apex requires. Everything this host needs is
  there, so this host writes none of it.

  Per the `build-actor` convention CACAO is NEVER implemented here: the
  client mints it, and `kotoba-lang/org-chainagnostic-cacao` owns the
  primitives. This namespace supplies only the actor's own Ed25519 seed,
  which is a Cloudflare secret and is not in git.

  ## Prefetch, not full load

  The actor is synchronous; the client is Promise-based. Rather than
  make every `Store` protocol in the fleet async, the host resolves
  reads BEFORE running the actor — and it can do so precisely, because a
  request NAMES what it touches: a buyer, some offer ids, and through
  those offers some sellers. So the host issues a handful of small
  queries instead of dragging the entire database across the wire, which
  is what the previous version did on every single request.

  Bracket access (`aget`) throughout for `:advanced-optimization`
  safety."
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [kotobase.client :as kb]
            [marketplace.order :as order]
            [marketplace.persist :as persist]
            [orderops.advisor :as advisor]
            [orderops.governor :as governor]
            [orderops.phase :as phase]
            [orderops.store :as store]))

(def ^:private db-name "marketplace-order")
(def ^:private default-endpoint "https://kotobase.net")

(defn- json [body status]
  (js/Response. (js/JSON.stringify (clj->js body))
                #js {:status status
                     :headers #js {"content-type" "application/json; charset=utf-8"
                                   "cache-control" "no-store"}}))

;; ───────────────────────── identity ─────────────────────────

(defn- b64->bytes [s]
  (let [bin (js/atob s)
        n (.-length bin)
        out (js/Uint8Array. n)]
    (dotimes [i n] (aset out i (.charCodeAt bin i)))
    out))

(defn- client-for
  "Build the kotobase client from the actor's own seed.

  Returns nil when the seed is absent, and the host then fails closed
  rather than falling back to anything local — the same discipline
  `marketplace.persist/store` applies to its database API."
  [env]
  (when-let [seed (aget env "KOTOBASE_SECRET_KEY")]
    (kb/make-client {:endpoint (or (aget env "KOTOBASE_ENDPOINT") default-endpoint)
                     :secret-key (b64->bytes seed)
                     :operator-did (or (aget env "KOTOBASE_OPERATOR_DID")
                                       "did:web:kotobase.net")
                     :auth-profile :apex})))

;; ───────────────────────── reads ─────────────────────────

(defn- rows-edn
  "Rows out of a `datomic.q` response.

  The live apex returns them under `rows`; the client's own
  `empty-on-404` default names `rows_edn`. Both are read here rather
  than picking one, because a key mismatch here does not error — it
  silently returns nothing, which reads exactly like an empty database.
  That cost a full diagnose-and-deploy cycle on this actor already."
  [res]
  (let [rows (or (aget res "rows") (aget res "rows_edn") #js [])]
    (mapv (fn [r]
            (let [cell (if (and (array? r) (pos? (alength r))) (aget r 0) r)]
              (if (string? cell) (reader/read-string cell) cell)))
          (array-seq rows))))

(defn- datoms->docs
  "Fold a `datomic.datoms` response into `{kind {id doc}}`.

  Entity ids are deterministic (`marketplace.persist/doc-eid` =
  `mp.<kind>/<id>`), so the kind and id come straight off `:e` and the
  document comes off the one `:mp.<kind>/doc` cell."
  [res]
  (reduce
   (fn [acc d]
     (let [e (str (aget d "e"))
           a (str (aget d "a"))
           [_ kind id] (re-matches #"^mp\.([^/]+)/(.+)$" e)]
       (if (and kind (= a (str ":mp." kind "/doc")))
         (assoc-in acc [kind id]
                   (persist/dec* (reader/read-string (str (aget d "v_edn")))))
         acc)))
   {}
   (array-seq (or (aget res "datoms") #js []))))

(defn- load-graph
  "One `:eavt` scan, folded into every document in the actor's graph.

  ## This is a whole-graph read, and it should not be

  The intended shape is a bounded read — `datomic.q` pushing the filter
  server-side, or an `:eavt` scan bounded by the entity id. Neither
  returns data on the live apex today, and that was measured, not
  assumed:

    - a write reports `ok:true` with an advancing commit CID and a
      growing `novelty_size`
    - an unbounded `datoms` scan of the SAME graph returns those datoms
    - `datomic.q` against that same graph returns `rows: []`
    - `datomic.fold`, which compacts novelty into the indexed snapshot
      `q` reads, answers `MethodNotImplemented`

  net-kotobase only very recently began routing fold
  (`3a94150 route datomic.fold/view to backend and D1`), so this is a
  service-side gap in flight. Until `q` or a bounded scan works, one
  full scan per request is the read that actually returns data — and
  saying so here is better than a bounded-looking call that silently
  returns nothing, which is exactly how this actor lost an afternoon.

  It is still ONE round trip, not one per document."
  [client]
  (-> (kb/datoms client db-name ":eavt")
      (.then datoms->docs)))

(defn- prefetch
  "The documents this request needs, selected from one graph scan.

  The SELECTION is still precise — a buyer, the offers named, and the
  sellers behind those offers — even though the read that feeds it is
  currently unbounded (see `load-graph`). When a bounded read works, only
  `load-graph` changes; this stays as it is."
  [client {:keys [buyer offer-ids order-id]}]
  (-> (load-graph client)
      (.then (fn [docs]
               (let [offers (vec (keep #(get-in docs ["offer" %]) (distinct (or offer-ids []))))
                     sellers (vec (keep #(get-in docs ["seller" %])
                                        (distinct (map :offer/seller offers))))
                     b (get-in docs ["buyer" buyer])
                     o (when order-id (get-in docs ["order" order-id]))]
                 {:buyers (if b [b] [])
                  :offers offers
                  :sellers sellers
                  :orders (if o [o] [])})))))

(defn- seed-tx
  "Turn prefetched documents into the tx-data `persist`'s recording api
  seeds from — the same triples it writes, so the round trip is
  symmetric by construction."
  [{:keys [buyers offers sellers orders]}]
  (vec
   (concat
    (mapcat #(persist/doc->tx :buyer :buyer/id %) buyers)
    (mapcat #(persist/doc->tx :offer :offer/id %) offers)
    (mapcat #(persist/doc->tx :seller :seller/id %) sellers)
    (mapcat #(persist/doc->tx :order :order/id %) orders))))

;; ───────────────────────── writes ─────────────────────────

(defn- flush-tx!
  "One transact of everything the request wrote.

  Nothing when it wrote nothing: an empty transact would still advance
  the head and make a read look like a write in the audit trail.
  Retries are OFF — a re-applied append with a fresh ordinal would
  duplicate a ledger entry, exactly the hazard the client's own
  `transact` docstring warns about."
  [client tx-data]
  (if (seq tx-data)
    (kb/transact client db-name (pr-str (vec tx-data)))
    (js/Promise.resolve nil)))

;; ───────────────────────── running the actor ─────────────────────────

(defn- run-actor
  "One synchronous actor pass over a seeded store.

  The full langgraph StateGraph is `orderops.operation`, run by the
  operator against this same store; the edge runs the SAME governor and
  phase gate on a single request — the structural subset that makes
  sense at request time, the shape `partners.edge.intake` documents."
  [st context request]
  (let [proposal (advisor/-advise (advisor/mock-advisor) st request)
        verdict (governor/check request context proposal st)
        base (phase/verdict->disposition verdict)
        {:keys [disposition reason]} (phase/gate (:phase context) request base)]
    (case disposition
      :commit
      (do (store/commit-record! st {:op (:op proposal) :order-id (:order-id request)
                                    :value (:value proposal) :payload (:value proposal)})
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
  "prefetch -> run -> flush."
  [client names f]
  (-> (prefetch client names)
      (.then (fn [docs]
               (let [api (persist/recording-db-api (seed-tx docs))
                     st (store/kotobase-store {:db-api api})
                     result (f st)
                     written (persist/recorded api)]
                 (-> (flush-tx! client written)
                     (.then (fn [_] (assoc result :written (count written))))))))))

;; ───────────────────────── routes ─────────────────────────

(defn- authorised? [request env]
  (let [want (aget env "ORDER_WRITE_TOKEN")
        got (some-> (.get (aget request "headers") "authorization")
                    (str/replace #"^Bearer " ""))]
    (and want (not (str/blank? (str got))) (= want got))))

(defn- place-order [client body]
  (let [oid (get body "order-id")
        offer-ids (mapv #(get % "offer-id") (get body "lines" []))]
    (with-store
      client {:buyer (get body "buyer") :offer-ids offer-ids}
      (fn [st]
        (let [req {:op :place-order :order-id oid
                   :patch {:buyer (get body "buyer")
                           :lines (mapv (fn [l] {:offer-id (get l "offer-id")
                                                 :qty (get l "qty")})
                                        (get body "lines" []))}}
              ctx {:actor-id "orderops-edge" :phase 3
                   :now (get body "now" "2026-06-01T00:00:00Z")
                   :needs-shipping? (get body "needs-shipping" true)
                   :destination (get body "destination")}
              out (run-actor st ctx req)]
          {:order-id oid
           :disposition (:disposition out)
           :violations (mapv :rule (get-in out [:verdict :violations]))})))))

(defn- advance-order
  "Record a sub-order transition observed elsewhere.

  Only the order itself is prefetched: `orderops.governor` resolves the
  legality of the move from the stored order and `kotoba.okaimono`'s
  transition table, and needs neither the buyer nor the catalog to do
  it. `:cancel-sub-order` routes here too and always escalates -- the
  governor, not this handler, is what makes that so."
  [client body op]
  (let [oid (get body "order-id")
        seller (get body "seller")
        to (some-> (get body "to") keyword)]
    (with-store
      client {:order-id oid}
      (fn [st]
        (let [req {:op op :order-id oid :patch {:seller seller :to to}}
              ctx {:actor-id "orderops-edge" :phase 3
                   :now (get body "now" "2026-06-01T00:00:00Z")}
              out (run-actor st ctx req)]
          {:order-id oid
           :disposition (:disposition out)
           :violations (mapv :rule (get-in out [:verdict :violations]))})))))

(defn- read-order [client oid]
  (-> (.then (load-graph client) #(get-in % ["order" oid]))
      (.then (fn [o]
               (if o
                 {:order-id oid
                  :buyer (:order/buyer o)
                  :sellers (:order/sellers o)
                  :currency (:order/currency o)
                  :total-minor (order/total-minor o)
                  :status (order/overall-status o)
                  :fully-delivered? (order/fully-delivered? o)}
                 {:error "unknown order" :order-id oid})))))

(defn- seed-reference-data [client]
  (let [d (store/demo-data)
        tx (vec (concat
                 (mapcat #(persist/doc->tx :buyer :buyer/id %) (vals (:buyers d)))
                 (mapcat #(persist/doc->tx :seller :seller/id %) (vals (:sellers d)))
                 (mapcat #(persist/doc->tx :offer :offer/id %) (vals (:offers d)))))]
    (-> (flush-tx! client tx)
        (.then (fn [_] {:seeded {:buyers (count (:buyers d))
                                 :sellers (count (:sellers d))
                                 :offers (count (:offers d))}
                        :triples (count tx)})))))

(defn- no-client []
  (json {:error "KOTOBASE_SECRET_KEY not configured"
         :detail "this host fails closed rather than falling back to local storage"}
        503))

(defn- handle [request env]
  (let [url (js/URL. (aget request "url"))
        path (aget url "pathname")
        method (aget request "method")
        client (client-for env)]
    (cond
      (= path "/health")
      (js/Promise.resolve
       (json {:ok (some? client)
              :service "cloud-itonami-marketplace-order"
              :store "kotobase.net via kotoba-lang/kotobase-client"
              :db db-name
              :auth "CACAO minted per request by the client (:apex profile)"
              :did (:did client)}
             (if client 200 503)))

      (nil? client) (js/Promise.resolve (no-client))

      ;; Seeds the reference documents this actor READS but does not
      ;; own -- seller credentials from `-marketplace-onboarding` and
      ;; catalog offers. In a full deployment those actors write them
      ;; into this graph themselves and this route goes away.
      (and (= method "POST") (= path "/admin/seed"))
      (if-not (authorised? request env)
        (js/Promise.resolve (json {:error "unauthorised"} 401))
        (-> (seed-reference-data client)
            (.then #(json % 200))
            (.catch (fn [e] (json {:store-error (str (or (aget e "message") e))} 502)))))

      (and (= method "POST") (= path "/orders"))
      (if-not (authorised? request env)
        (js/Promise.resolve (json {:error "unauthorised"} 401))
        (-> (.json request)
            (.then (fn [b]
                     (let [body (js->clj b)
                           op (keyword (or (get body "op") "place-order"))]
                       (case op
                         :place-order (place-order client body)
                         (:advance-sub-order :cancel-sub-order) (advance-order client body op)
                         (js/Promise.resolve {:error "unknown-op" :op (str op)})))))
            (.then #(json % 200))
            (.catch (fn [e] (json {:store-error (str (or (aget e "message") e))} 502)))))

      (and (= method "GET") (.startsWith path "/orders/"))
      (-> (read-order client (.slice path (count "/orders/")))
          (.then (fn [r] (json r (if (:error r) 404 200))))
          (.catch (fn [e] (json {:store-error (str (or (aget e "message") e))} 502))))

      :else (js/Promise.resolve (json {:error "not found"} 404)))))

(def app
  (clj->js {:fetch (fn [request env _ctx] (handle request env))}))

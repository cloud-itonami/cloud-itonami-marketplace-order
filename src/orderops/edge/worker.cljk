(ns orderops.edge.worker
  "The order actor's Worker — routes only.

  Everything that is not specific to orders lives in `marketplace.edge`:
  the kotobase client, the prefetch/run/flush bracket, the ledger
  ordinal, the fail-closed check for a missing seed. This file used to
  hold all of it, and five more actors were about to copy it.

  What is specific to orders, and stays here:

    - which documents a request NAMES. Placing an order names a buyer,
      some offers, and through those offers some sellers. A transition
      names only the order — the governor resolves its legality from the
      stored order and `kotoba.okaimono`'s transition table, and needs
      neither the buyer nor the catalog to do it.
    - the request -> actor-request translation, including `:op`, so a
      transition and a cancellation go through the SAME governor as a
      placement instead of every POST being a place-order.

  This actor READS seller credentials and catalog offers; it does not
  write them. `-marketplace-onboarding` issues the credentials and
  `-marketplace-listing` publishes the offers, both into the same
  `marketplace` ref, which is what makes reading them a join rather than
  an API call between services."
  (:require [marketplace.buyer :as buyer]
            [marketplace.edge :as edge]
            [marketplace.order :as order]
            [orderops.advisor :as advisor]
            [orderops.governor :as governor]
            [orderops.phase :as phase]
            [orderops.store :as store]))

;; ───────────────────────── running the actor ─────────────────────────

(defn- run-actor
  "One synchronous actor pass over a seeded store.

  The full langgraph StateGraph is `orderops.operation`, run by the
  operator against this same store; the edge runs the SAME governor and
  phase gate on a single request — the structural subset that makes
  sense at request time."
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

(defn- outcome [oid out]
  {:order-id oid
   :disposition (:disposition out)
   :violations (mapv :rule (get-in out [:verdict :violations]))})

;; ───────────────────────── operations ─────────────────────────

(defn- place-new [st body oid]
  (let [req {:op :place-order :order-id oid
             :patch {:buyer (get body "buyer")
                     :lines (mapv (fn [l] {:offer-id (get l "offer-id")
                                           :qty (get l "qty")})
                                  (get body "lines" []))}}
        ctx {:actor-id "orderops-edge" :phase 3
             :now (get body "now" "2026-06-01T00:00:00Z")
             :needs-shipping? (get body "needs-shipping" true)
             :destination (get body "destination")}]
    (outcome oid (run-actor st ctx req))))

(defn- place-order
  "Place an order, at most once per order id.

  A retried POST — a client timeout, a proxy retry, a user pressing the
  button twice — must not become a second order. The order id is the
  idempotency key, because it is already the thing that identifies this
  order and asking callers to invent a second identifier is asking them
  to get it wrong.

  Three outcomes, and the distinction between the last two matters:

    id unseen                    -> place it.
    id seen, SAME order          -> `:idempotent true`, the stored
                                    order, no second write. A retry is
                                    answered, not punished.
    id seen, DIFFERENT order     -> `order-id-conflict`. This is not a
                                    retry; it is two different orders
                                    claiming one id, and silently
                                    keeping either one loses the other."
  [client body]
  (let [oid (get body "order-id")
        offer-ids (mapv #(get % "offer-id") (get body "lines" []))]
    (edge/with-store
      {:client client
       ;; The sellers cannot be named until the offers are in hand, so
       ;; they are taken wholesale; everything else is exactly what the
       ;; request named -- including the order id itself, so a replay is
       ;; recognised before anything is written.
       :wants {:buyer [(get body "buyer")] :offer offer-ids :credential :all
               :order [oid]}
       :store-fn store/kotobase-store}
      (fn [st]
        (if-let [existing (store/order-record st oid)]
          (let [want (store/build-order st {:order-id oid
                                            :buyer (get body "buyer")
                                            :lines (mapv (fn [l] {:offer-id (get l "offer-id")
                                                                  :qty (get l "qty")})
                                                         (get body "lines" []))
                                            :currency (get body "currency")})]
            (if (and want (= (order/->basket-lines want)
                             (order/->basket-lines existing)))
              {:order-id oid :disposition "commit" :violations [] :idempotent true}
              {:order-id oid :disposition "hold" :violations ["order-id-conflict"]}))
          (place-new st body oid))))))

(defn- advance-order
  "Record a sub-order transition observed elsewhere.

  Only the order is prefetched. `:cancel-sub-order` routes here too and
  always escalates — the governor, not this handler, is what makes that
  so, because a cancellation has money consequences downstream."
  [client body op]
  (let [oid (get body "order-id")]
    (edge/with-store
      {:client client
       :wants {:order [oid]}
       :store-fn store/kotobase-store}
      (fn [st]
        (let [req {:op op :order-id oid
                   :patch {:seller (get body "seller")
                           :to (some-> (get body "to") keyword)}}
              ctx {:actor-id "orderops-edge" :phase 3
                   :now (get body "now" "2026-06-01T00:00:00Z")}]
          (outcome oid (run-actor st ctx req)))))))

(defn- read-order [client oid]
  (-> (edge/read-doc client :order oid)
      (.then (fn [o]
               (if-not o
                 {:error "unknown order" :order-id oid}
                 {:order-id (:order/id o)
                  :buyer (:order/buyer o)
                  :sellers (vec (:order/sellers o))
                  :currency (:order/currency o)
                  :total-minor (order/total-minor o)
                  :status (name (order/overall-status o))
                  :fully-delivered? (order/fully-delivered? o)})))))

(defn- register-buyer
  "Create or update a buyer account.

  Buyers are this actor's to own — an order without a buyer is not an
  order — and `marketplace.buyer` is explicit that `:guest` is a
  first-class level. A guest with only a contact string is a perfectly
  good buyer for a digital order; what refuses is a PHYSICAL order with
  no address in the destination, and the governor is what says so."
  [client body]
  (let [bid (get body "buyer-id")
        b (buyer/account
           {:id bid
            :contact (get body "contact")
            :level (keyword (get body "level" "guest"))
            :country (get body "country")
            :addresses (mapv (fn [a]
                               (buyer/address {:line1 (get a "line1")
                                               :line2 (get a "line2")
                                               :city (get a "city")
                                               :postal-code (get a "postal-code")
                                               :country (get a "country")
                                               :recipient (get a "recipient")}))
                             (get body "addresses" []))})]
    (edge/with-store
      {:client client :wants {:buyer [bid]} :store-fn store/kotobase-store}
      (fn [st]
        (store/put-buyer! st b)
        {:ref bid :disposition "commit" :violations []
         :level (name (:buyer/level b))
         :addresses (count (:buyer/addresses b))}))))

;; ───────────────────────── routes ─────────────────────────

(defn- routes [client request env method path _url]
  (cond
    (and (= method "POST") (= path "/orders"))
    (if-not (edge/authorised? request env)
      (js/Promise.resolve (edge/json {:error "unauthorised"} 401))
      (-> (.json request)
          (.then (fn [b]
                   (let [body (js->clj b)
                         op (keyword (or (get body "op") "place-order"))]
                     (case op
                       :place-order (place-order client body)
                       (:advance-sub-order :cancel-sub-order) (advance-order client body op)
                       (js/Promise.resolve {:error "unknown-op" :op (str op)})))))
          (.then #(edge/json % 200))))

    (and (= method "POST") (= path "/buyers"))
    (if-not (edge/authorised? request env)
      (js/Promise.resolve (edge/json {:error "unauthorised"} 401))
      (-> (.json request)
          (.then #(register-buyer client (js->clj %)))
          (.then #(edge/json % 200))))

    (and (= method "GET") (.startsWith path "/orders/"))
    (-> (read-order client (.slice path (count "/orders/")))
        (.then (fn [r] (edge/json r (if (:error r) 404 200)))))

    (and (= method "GET") (= path "/orders"))
    (-> (edge/read-all client :order)
        (.then (fn [os] (edge/json {:orders (mapv :order/id os)} 200))))

    ;; /escalations and /ledger, implemented once in marketplace.edge.
    ;; Every high-stakes move in this actor escalates rather than committing
    ;; on a machine's say-so; without a way to READ those, each of those gates
    ;; is a black hole.
    :else (edge/ledger-routes client request env method path :orderops)))

(def app
  (clj->js
   {:fetch (fn [request env _ctx]
             (edge/serve "cloud-itonami-marketplace-order" request env routes))}))

(ns orderops.store
  "SSoT for the marketplace order actor -- the first CROSS-ACTOR
  orchestrator in this fleet.

  Every other `cloud-itonami-*` actor is self-contained by design: one
  repo, one business, forkable alone. That is right for a single
  operator, but it left a real hole — nothing tied listing to settlement
  to fulfillment to delivery, so a marketplace order existed nowhere.

  This store is that seam, and it is deliberately a READER of the other
  actors' outputs rather than a second copy of their state:

    sellers      credentials issued by `-marketplace-onboarding`.
                 Read-only here. Whether a seller may trade is that
                 actor's decision and this one never re-litigates it.
    offers       `marketplace.catalog` offers, priced by the seller.
                 Read-only here. Prices come from the catalog, never
                 from the order request — a buyer cannot name their own
                 price by putting it in the payload.
    orders       the one thing this actor OWNS: multi-seller orders as
                 `marketplace.order` parents over per-seller okaimono
                 sub-orders.
    deliveries   confirmations arriving from the courier side
                 (`cloud-itonami-isic-5320`) / the fulfillment actor.

  What this store does NOT hold: money (settlement's), listings
  (listing's), warehouse tasks (fulfillment's). Where those actors need
  something from an order they take a projection —
  `marketplace.order/->basket-lines` for settlement, the sub-orders
  themselves for fulfillment — so an order shape change cannot silently
  alter a payout or a pick list.

  The ledger stays append-only."
  (:require [marketplace.buyer :as buyer]
            [marketplace.catalog :as catalog]
            [marketplace.order :as order]
            [marketplace.persist :as persist]
            [marketplace.seller :as seller]))

(defprotocol Store
  (buyer-account [s buyer-id] "Buyer account, or nil.")
  (all-buyer-accounts [s])
  (seller-credential [s seller-id] "Issued credential from -onboarding, or nil.")
  (all-seller-credentials [s])
  (offer-record [s offer-id] "Catalog offer, or nil.")
  (all-offer-records [s])
  (order-record [s order-id] "Multi-seller order, or nil.")
  (durable? [s] "False for the test-only memory backend.")
  (all-order-records [s])
  (delivered? [s order-id seller] "Courier/fulfillment delivery confirmation.")
  (ledger [s])
  (order-log [s])
  (commit-record! [s record])
  (append-ledger! [s fact])
  (with-orders [s orders]))

;; ----------------------------- demo data -----------------------------

(def product "gtin.05449000000996")

(defn- cred [id payout?]
  (seller/credential
   {:id id :kind :company :legal-name (str "Demo " id) :country "JPN"
    :issuer "did:web:marketplace.example"
    :issued-at "2026-01-01T00:00:00Z" :expires-at "2027-01-01T00:00:00Z"
    :status :issued :payout-bound? payout?
    :evidence {:evidence/verified-checks #{:document-authenticity :sanctions}
               :evidence/aml-status :clear
               :evidence/ekyc-complete? true}}))

(defn- demo-buyers []
  {"buyer-1" (buyer/account {:id "buyer-1" :contact "buyer1@example.test"
                             :level :guest :country "JPN"
                             :addresses [(buyer/address
                                          {:line1 "1-2-3 Shibuya" :city "Tokyo"
                                           :postal-code "150-0002" :country "JPN"
                                           :recipient "山田太郎"})]})
   ;; A guest with no address: fine for a digital order, refused for a
   ;; physical one -- the asymmetry marketplace.buyer exists to express.
   "buyer-2" (buyer/account {:id "buyer-2" :contact "buyer2@example.test"
                             :level :guest})})

(defn demo-data
  "Fixtures covering the happy path and each hard check.

    merchant.alpha  sellable, offer 1200
    merchant.beta   sellable, offer 1100
    merchant.gamma  NOT payout-bound -- an order line naming them is
                    refused before it is ever placed"
  []
  (let [oa (catalog/offer {:product product :seller "merchant.alpha"
                           :price-minor 1200 :currency "JPY" :quantity 40})
        ob (catalog/offer {:product product :seller "merchant.beta"
                           :price-minor 1100 :currency "JPY" :quantity 5})
        og (catalog/offer {:product product :seller "merchant.gamma"
                           :price-minor 900 :currency "JPY" :quantity 3})]
    {:buyers (demo-buyers)
     :sellers {"merchant.alpha" (cred "merchant.alpha" true)
               "merchant.beta"  (cred "merchant.beta" true)
               "merchant.gamma" (cred "merchant.gamma" false)}
     :offers (into {} (map (juxt :offer/id identity) [oa ob og]))
     :orders {}
     :deliveries {}}))

;; ----------------------------- MemStore -----------------------------

(defrecord MemStore [a]
  Store
  (buyer-account [_ id] (get-in @a [:buyers id]))
  (all-buyer-accounts [_] (sort-by :buyer/id (vals (:buyers @a))))
  (durable? [_] false)
  (seller-credential [_ id] (get-in @a [:sellers id]))
  (all-seller-credentials [_] (sort-by :seller/id (vals (:sellers @a))))
  (offer-record [_ id] (get-in @a [:offers id]))
  (all-offer-records [_] (sort-by :offer/id (vals (:offers @a))))
  (order-record [_ id] (get-in @a [:orders id]))
  (all-order-records [_] (sort-by :order/id (vals (:orders @a))))
  (delivered? [_ oid sel] (boolean (get-in @a [:deliveries [oid sel]])))
  (ledger [_] (:ledger @a))
  (order-log [_] (:order-log @a))
  (commit-record! [_ record]
    (swap! a update :order-log conj record)
    (let [{:keys [op value]} record]
      (case op
        :place-order
        (when-let [o (:order value)]
          (swap! a assoc-in [:orders (:order/id o)] o))

        ;; Both advancing and cancelling go through
        ;; marketplace.order/advance-sub-order, which delegates the
        ;; legality of the move to kotoba.okaimono -- so an illegal
        ;; transition cannot be written here even if the governor were
        ;; somehow bypassed.
        (:advance-sub-order :cancel-sub-order)
        (let [{:keys [order-id seller to]} value]
          (swap! a update-in [:orders order-id]
                 (fn [o] (or (and o (order/advance-sub-order o seller to)) o)))
          (when (= :delivered to)
            (swap! a assoc-in [:deliveries [order-id seller]] true)))

        nil))
    record)
  (append-ledger! [_ fact] (swap! a update :ledger conj fact) fact)
  (with-orders [s orders] (when (seq orders) (swap! a assoc :orders orders)) s))

(defn seed-db []
  (->MemStore (atom (assoc (demo-data) :ledger [] :order-log []))))

(defn mem-store [m]
  (->MemStore (atom (merge {:buyers {} :sellers {} :offers {} :orders {} :deliveries {}
                            :ledger [] :order-log []}
                           m))))

;; ----------------------------- durable store -----------------------------

(defrecord KotobaseStore [st seed]
  Store
  (buyer-account [_ id] (persist/get-doc (persist/ctx st :buyer :buyer/id) id))
  (all-buyer-accounts [_] (persist/all-docs (persist/ctx st :buyer :buyer/id)))
  (durable? [_] (not (:persist/memory? st)))
  (seller-credential [_ id] (persist/get-doc (persist/ctx st :seller :seller/id) id))
  (all-seller-credentials [_] (persist/all-docs (persist/ctx st :seller :seller/id)))
  (offer-record [_ id] (persist/get-doc (persist/ctx st :offer :offer/id) id))
  (all-offer-records [_] (persist/all-docs (persist/ctx st :offer :offer/id)))
  (order-record [_ id] (persist/get-doc (persist/ctx st :order :order/id) id))
  (all-order-records [_] (persist/all-docs (persist/ctx st :order :order/id)))
  (delivered? [_ oid sel]
    (boolean (:delivered (persist/get-doc (persist/ctx st :delivery :id)
                                          (str oid "/" sel)))))
  (ledger [_] (persist/read-events (persist/stream-ctx st :ledger)))
  (order-log [_] (persist/read-events (persist/stream-ctx st :order-log)))
  (commit-record! [this record]
    (persist/append-event! (persist/stream-ctx st :order-log) seed record)
    (let [{:keys [op value]} record]
      (case op
        :place-order
        (when-let [o (:order value)]
          (persist/put-doc! (persist/ctx st :order :order/id) o))

        (:advance-sub-order :cancel-sub-order)
        (let [{:keys [order-id seller to]} value]
          (when-let [o (order-record this order-id)]
            (when-let [o' (order/advance-sub-order o seller to)]
              (persist/put-doc! (persist/ctx st :order :order/id) o')
              (when (= :delivered to)
                (persist/put-doc! (persist/ctx st :delivery :id)
                                  {:id (str order-id "/" seller) :delivered true})))))
        nil))
    record)
  (append-ledger! [_ fact]
    (persist/append-event! (persist/stream-ctx st :ledger) seed fact))
  (with-orders [this orders]
    (doseq [o (vals orders)] (persist/put-doc! (persist/ctx st :order :order/id) o))
    this))

(defn kotobase-store
  "A durable store over a HOST-INJECTED database API.

  `db-api` is the four `kotobase.core` operations the host partials onto
  an already-open database; `marketplace.persist/store` throws when it
  is missing or partial, per the policy's
  `:policy/fail-closed-without-host-injection`. This actor therefore
  cannot come up durable-looking but writing to nothing.

  `seq-fn` supplies the ledger ordinal. It is the host's because a count
  would be a read-modify-write two concurrent appends would collide on."
  [{:keys [db-api seq-fn]}]
  (->KotobaseStore (persist/store {:db-api db-api :actor "orderops"})
                   (or seq-fn (let [n (atom 0)] #(swap! n inc)))))

;; ----------------------------- derived views -----------------------------

(defn sellable?
  "Delegates wholly to `marketplace.seller/sellable?`. Admission to trade
  belongs to `-marketplace-onboarding`; duplicating the rule here would
  let the two drift apart."
  [s seller-id now]
  (boolean
   (when-let [c (seller-credential s seller-id)]
     (seller/sellable? c now (:seller/issuer c)))))

(defn resolve-lines
  "Turn a request's `[{:offer-id .. :qty ..} ..]` into order lines, with
  seller and PRICE taken from the catalog offer.

  This is the load-bearing part: the price is read from the offer, never
  from the request. A buyer (or a confused advisor) cannot name their
  own price by putting one in the payload. An unknown offer yields nil
  for that line, which `place-order` turns into a hard refusal rather
  than silently dropping it."
  [s line-requests]
  (mapv (fn [{:keys [offer-id qty]}]
          (when-let [o (offer-record s offer-id)]
            {:seller           (:offer/seller o)
             :offer            offer-id
             :sku              (:offer/product o)
             :name             (:offer/product o)
             :qty              qty
             :unit-price-minor (:offer/price-minor o)}))
        line-requests))

(defn build-order
  "Construct the multi-seller order for a request. nil when any line is
  unresolvable or `marketplace.order/order` refuses it."
  [s {:keys [order-id buyer lines currency cod?]}]
  (let [resolved (resolve-lines s lines)]
    (when (every? some? resolved)
      (order/order {:id order-id :buyer buyer :lines resolved
                    :currency (or currency "JPY") :cod? cod?}))))

(defn ->settlement-basket
  "The projection settlement consumes. Kept here so the seam is visible
  in one place rather than being reconstructed by each caller."
  [o]
  (order/->basket-lines o))

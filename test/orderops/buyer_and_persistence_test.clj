(ns orderops.buyer-and-persistence-test
  "Buyer accounts and durable storage, added to the order actor."
  (:require [clojure.test :refer [deftest is testing]]
            [marketplace.buyer :as buyer]
            [marketplace.order :as order]
            [marketplace.persist :as persist]
            [orderops.advisor :as advisor]
            [orderops.governor :as governor]
            [orderops.store :as store]))

(def now "2026-06-01T00:00:00Z")
(defn- ctx [& {:as over}]
  (merge {:actor-id "order-01" :phase 3 :now now} over))

(defn- offer-of [st seller]
  (->> (store/all-offer-records st)
       (filter #(= seller (:offer/seller %))) first :offer/id))

(defn- place [st & {:keys [buyer-id sellers] :or {buyer-id "buyer-1"
                                                  sellers ["merchant.alpha"]}}]
  {:order-id "ord-1"
   :patch {:buyer buyer-id
           :lines (mapv (fn [s] {:offer-id (offer-of st s) :qty 1}) sellers)}})

(defn- check [st req & [c]]
  (governor/check req (or c (ctx))
                  (advisor/-advise (advisor/mock-advisor) st (assoc req :op :place-order))
                  st))

;; ───────────────────────── the buyer gate ─────────────────────────

(deftest a-guest-with-an-address-can-buy
  (let [st (store/seed-db)
        v (check st (place st) (ctx :destination "JPN"))]
    (is (false? (:hard? v)) (pr-str (:violations v)))
    (is (true? (:ok? v)))))

(deftest an-unknown-buyer-is-a-hard-block
  (let [st (store/seed-db)
        v (check st (place st :buyer-id "buyer-nope"))]
    (is (true? (:hard? v)))
    (is (some #{:buyer-unknown} (mapv :rule (:violations v))))))

(deftest a-physical-order-with-no-address-is-refused
  (testing "buyer-2 is a valid guest with only a contact string — fine for
            a digital order, but a parcel needs somewhere to go"
    (let [st (store/seed-db)
          v (check st (place st :buyer-id "buyer-2") (ctx :needs-shipping? true))]
      (is (true? (:hard? v)))
      (is (some #{:no-shipping-address} (mapv :rule (:violations v)))))))

(deftest the-same-buyer-passes-for-a-digital-order
  (testing "the asymmetry marketplace.buyer exists to express"
    (let [st (store/seed-db)
          v (check st (place st :buyer-id "buyer-2") (ctx :needs-shipping? false))]
      (is (false? (:hard? v)) (pr-str (:violations v))))))

(deftest no-address-in-the-destination-is-refused
  (let [st (store/seed-db)
        v (check st (place st) (ctx :needs-shipping? true :destination "USA"))]
    (is (true? (:hard? v)))
    (is (some #{:no-address-in-destination} (mapv :rule (:violations v))))))

(deftest the-required-level-comes-from-the-operator-not-a-constant
  (testing "marketplace.buyer is explicit that there is no built-in
            'over ¥X needs ID' rule — that threshold is a jurisdiction- and
            product-specific operator decision"
    (let [st (store/seed-db)]
      (is (false? (:hard? (check st (place st) (ctx :destination "JPN")))))
      (let [v (check st (place st) (ctx :destination "JPN"
                                        :require-level :identity-verified))]
        (is (true? (:hard? v)))
        (is (some #{:insufficient-level} (mapv :rule (:violations v))))))))

(deftest a-guest-is-a-first-class-level
  (is (= :guest (:buyer/level (store/buyer-account (store/seed-db) "buyer-1"))))
  (is (true? (buyer/can-purchase? (store/buyer-account (store/seed-db) "buyer-2")))))

;; ───────────────────────── PII ─────────────────────────

(deftest a-proposal-embedding-a-raw-buyer-is-a-hard-block
  (testing "everything a proposal carries lands in an APPEND-ONLY ledger;
            a street address written there cannot be scrubbed later"
    (let [st (store/seed-db)
          raw (store/buyer-account st "buyer-1")
          p (assoc-in (advisor/-advise (advisor/mock-advisor) st
                                       (assoc (place st) :op :place-order))
                      [:value :buyer-record] raw)
          v (governor/check (place st) (ctx :destination "JPN") p st)]
      (is (true? (:hard? v)))
      (is (some #{:proposal-leaks-pii} (mapv :rule (:violations v)))))))

(deftest a-redacted-buyer-passes
  (let [st (store/seed-db)
        red (buyer/redact (store/buyer-account st "buyer-1"))
        p (assoc-in (advisor/-advise (advisor/mock-advisor) st
                                     (assoc (place st) :op :place-order))
                    [:value :buyer-record] red)
        v (governor/check (place st) (ctx :destination "JPN") p st)]
    (is (false? (:hard? v)) (pr-str (:violations v)))
    (is (true? (buyer/redacted? red)))
    (is (nil? (:buyer/contact red)))
    (is (= ["JPN"] (:buyer/address-countries red))
        "the destination country survives; the street does not")))

(deftest the-default-proposal-carries-no-pii
  (let [st (store/seed-db)
        p (advisor/-advise (advisor/mock-advisor) st
                           (assoc (place st) :op :place-order))]
    (is (false? (buyer/leaks-pii? (select-keys p [:summary :rationale :cites :value]))))))

;; ───────────────────────── durable storage ─────────────────────────

(defn- durable []
  (store/kotobase-store {:db-api (persist/mem-db-api)}))

(deftest a-missing-host-database-fails-loudly
  (testing ":policy/fail-closed-without-host-injection — an actor whose
            host wiring is missing must not come up quietly writing to a
            map that vanishes on restart"
    (is (thrown? clojure.lang.ExceptionInfo (store/kotobase-store {})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (store/kotobase-store {:db-api {:transact! identity}})))))

(deftest the-memory-backend-reports-itself-as-not-durable
  (testing "so a production readiness check can refuse it rather than
            discovering at 3am that a service wrote to a map"
    (is (false? (store/durable? (durable))))
    (is (false? (store/durable? (store/seed-db))))))

(deftest orders-round-trip-through-the-durable-store
  (let [st (durable)
        o (order/order {:id "ord-1" :buyer "buyer-1"
                        :lines [{:seller "merchant.alpha" :sku "A1" :name "Cola"
                                 :qty 2 :unit-price-minor 600}]})]
    (store/commit-record! st {:op :place-order :value {:order o}})
    (let [got (store/order-record st "ord-1")]
      (is (= "buyer-1" (:order/buyer got)))
      (is (= ["merchant.alpha"] (:order/sellers got)))
      (is (= 1200 (order/total-minor got))
          "the nested okaimono sub-orders survive the round trip"))))

(deftest a-transition-persists-and-obeys-the-table
  (let [st (durable)
        o (order/order {:id "ord-1" :buyer "buyer-1"
                        :lines [{:seller "merchant.alpha" :sku "A1" :name "Cola"
                                 :qty 1 :unit-price-minor 600}]})]
    (store/commit-record! st {:op :place-order :value {:order o}})
    (store/commit-record! st {:op :advance-sub-order
                              :value {:order-id "ord-1" :seller "merchant.alpha"
                                      :to :confirmed}})
    (is (= :confirmed (order/overall-status (store/order-record st "ord-1"))))
    (testing "an illegal move is not written even at the store layer"
      (store/commit-record! st {:op :advance-sub-order
                                :value {:order-id "ord-1" :seller "merchant.alpha"
                                        :to :delivered}})
      (is (= :confirmed (order/overall-status (store/order-record st "ord-1")))))))

(deftest delivery-confirmations-persist
  (let [st (durable)
        o (-> (order/order {:id "ord-1" :buyer "buyer-1"
                            :lines [{:seller "merchant.alpha" :sku "A1" :name "Cola"
                                     :qty 1 :unit-price-minor 600}]})
              (order/advance-sub-order "merchant.alpha" :confirmed)
              (order/advance-sub-order "merchant.alpha" :packed)
              (order/advance-sub-order "merchant.alpha" :handed-over))]
    (store/commit-record! st {:op :place-order :value {:order o}})
    (is (false? (store/delivered? st "ord-1" "merchant.alpha")))
    (store/commit-record! st {:op :advance-sub-order
                              :value {:order-id "ord-1" :seller "merchant.alpha"
                                      :to :delivered}})
    (is (true? (store/delivered? st "ord-1" "merchant.alpha")))))

(deftest the-ledger-is-append-only-and-ordered
  (let [st (durable)]
    (is (empty? (store/ledger st)))
    (store/append-ledger! st {:t :governor-hold :op :place-order})
    (store/append-ledger! st {:t :committed :op :place-order})
    (is (= [:governor-hold :committed] (mapv :t (store/ledger st))))))

(deftest buyer-accounts-round-trip
  (let [st (durable)
        b (buyer/account {:id "buyer-9" :contact "x@example.test" :level :phone-verified
                          :country "JPN"
                          :addresses [(buyer/address {:line1 "1-1" :city "Tokyo"
                                                      :postal-code "100-0001"
                                                      :country "JPN" :recipient "Y"})]})]
    (persist/put-doc! (persist/ctx (:st st) :buyer :buyer/id) b)
    (let [got (store/buyer-account st "buyer-9")]
      (is (= :phone-verified (:buyer/level got)))
      (is (= 1 (count (:buyer/addresses got))))
      (is (= "Tokyo" (:address/city (first (:buyer/addresses got))))))))

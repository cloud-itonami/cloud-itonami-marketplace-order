(ns orderops.operation-graph-test
  "Integration tests for `orderops.operation/build`.

  The headline test is `the-whole-pipeline-runs-end-to-end`: a basket
  becomes a multi-seller order, each seller's parcel moves at its own
  pace, and the projections settlement and fulfillment consume come out
  the far end. That path did not exist anywhere in this fleet before."
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.graph :as g]
            [marketplace.fulfillment :as ff]
            [marketplace.order :as order]
            [marketplace.settlement :as settle]
            [orderops.operation :as operation]
            [orderops.store :as store]))

(def now "2026-06-01T00:00:00Z")
(def ^:private op-context {:actor-id "order-01" :phase 3 :now now})

(defn- exec
  ([actor tid request] (exec actor tid request op-context))
  ([actor tid request context]
   (g/run* actor {:request request :context context} {:thread-id tid})))

(defn- offer-of [st seller]
  (->> (store/all-offer-records st)
       (filter #(= seller (:offer/seller %))) first :offer/id))

(defn- place! [actor st tid order-id sellers]
  (exec actor tid
        {:op :place-order :order-id order-id
         :patch {:buyer "buyer-1"
                 :lines (mapv (fn [s] {:offer-id (offer-of st s) :qty 1}) sellers)}}))

(defn- advance! [actor _st tid order-id seller to]
  (exec actor tid {:op :advance-sub-order :order-id order-id
                   :patch {:seller seller :to to}}))

(deftest placing-auto-commits-and-prices-from-the-catalog
  (let [st (store/seed-db)
        actor (operation/build st)
        r (place! actor st "t-place" "ord-1" ["merchant.alpha" "merchant.beta"])]
    (is (= :done (:status r)))
    (is (= :commit (:disposition (:state r))))
    (let [o (store/order-record st "ord-1")]
      (is (= 2300 (order/total-minor o)))
      (is (= :placed (order/overall-status o))))))

(deftest an-unsellable-seller-hard-holds-before-any-order-exists
  (let [st (store/seed-db)
        actor (operation/build st)
        r (place! actor st "t-gamma" "ord-1" ["merchant.gamma"])]
    (is (= :done (:status r)) "not :interrupted — no human is asked")
    (is (= :hold (:disposition (:state r))))
    (is (nil? (store/order-record st "ord-1")))
    (is (some #{:seller-not-sellable}
              (map :rule (:violations (first (store/ledger st))))))))

(deftest the-whole-pipeline-runs-end-to-end
  (testing "basket -> multi-seller order -> per-seller movement ->
            the projections settlement and fulfillment consume"
    (let [st (store/seed-db)
          actor (operation/build st)]
      (place! actor st "t-1" "ord-1" ["merchant.alpha" "merchant.beta"])

      (testing "settlement's seam: one basket line per seller, priced from the catalog"
        (let [bl (store/->settlement-basket (store/order-record st "ord-1"))
              plan (settle/settlement-plan
                    {:lines bl :currency "JPY"
                     :fee-schedule (settle/fee-schedule {:commission-bps 1000})
                     :operator "merchant.marketplace-operator"})]
          (is (= 2 (count bl)))
          (is (= 2300 (:plan/gross-minor plan)))
          (is (true? (:plan/conserved? plan)))
          (is (= [1080 990] (mapv :alloc/seller-payout-minor (:plan/allocations plan))))))

      (testing "fulfillment's seam: tasks planned from the sub-order itself"
        (let [sub (order/sub-order (store/order-record st "ord-1") "merchant.alpha")
              tasks (ff/plan-tasks "ord-1" sub :station "ST-1" :robot "amr-07")]
          (is (= [:pick :pack :handover] (mapv :task/kind tasks)))
          (is (empty? (ff/pick-errors sub [{:sku (:offer/product
                                                  (first (store/all-offer-records st)))
                                            :qty 1}])))))

      (testing "each seller moves at their own pace"
        (doseq [s ["merchant.alpha" "merchant.beta"]]
          (advance! actor st (str "t-c-" s) "ord-1" s :confirmed)
          (advance! actor st (str "t-p-" s) "ord-1" s :packed))
        (is (= ["merchant.alpha" "merchant.beta"]
               (order/dispatchable-sellers (store/order-record st "ord-1"))))

        (advance! actor st "t-h-a" "ord-1" "merchant.alpha" :handed-over)
        (advance! actor st "t-d-a" "ord-1" "merchant.alpha" :delivered)
        (let [o (store/order-record st "ord-1")]
          (is (= :partially-delivered (order/overall-status o)))
          (is (false? (order/fully-delivered? o))
              "settlement must NOT release everyone because one seller delivered")
          (is (true? (store/delivered? st "ord-1" "merchant.alpha")))
          (is (false? (store/delivered? st "ord-1" "merchant.beta")))))

      (testing "and only when every parcel lands is the order delivered"
        (advance! actor st "t-h-b" "ord-1" "merchant.beta" :handed-over)
        (advance! actor st "t-d-b" "ord-1" "merchant.beta" :delivered)
        (let [o (store/order-record st "ord-1")]
          (is (= :delivered (order/overall-status o)))
          (is (true? (order/fully-delivered? o))))))))

(deftest an-illegal-transition-hard-holds-and-leaves-the-order-alone
  (let [st (store/seed-db)
        actor (operation/build st)]
    (place! actor st "t-1" "ord-1" ["merchant.alpha"])
    (let [r (advance! actor st "t-bad" "ord-1" "merchant.alpha" :delivered)]
      (is (= :done (:status r)))
      (is (= :hold (:disposition (:state r))))
      (is (= :placed (order/overall-status (store/order-record st "ord-1")))
          "still placed — the illegal move was not half-applied"))))

(deftest cancellation-waits-for-a-human
  (let [st (store/seed-db)
        actor (operation/build st)]
    (place! actor st "t-1" "ord-1" ["merchant.alpha" "merchant.beta"])
    (let [held (exec actor "t-cancel"
                     {:op :cancel-sub-order :order-id "ord-1"
                      :patch {:seller "merchant.alpha" :reason "在庫切れ"}})]
      (is (= :interrupted (:status held)))
      (is (= :placed (:okaimono/status
                      (order/sub-order (store/order-record st "ord-1") "merchant.alpha"))))
      (let [ok (g/run* actor {:approval {:status :approved :by "ops-01"}}
                       {:thread-id "t-cancel" :resume? true})]
        (is (= :commit (:disposition (:state ok))))
        (let [o (store/order-record st "ord-1")]
          (is (= :cancelled (:okaimono/status (order/sub-order o "merchant.alpha"))))
          (testing "and the rest of the order survives — the buyer still has beta coming"
            (is (= :placed (order/overall-status o)))))))))

(deftest a-rejected-cancellation-changes-nothing
  (let [st (store/seed-db)
        actor (operation/build st)]
    (place! actor st "t-1" "ord-1" ["merchant.alpha"])
    (exec actor "t-cx" {:op :cancel-sub-order :order-id "ord-1"
                        :patch {:seller "merchant.alpha" :reason "x"}})
    (let [r (g/run* actor {:approval {:status :rejected :by "ops-01"}}
                    {:thread-id "t-cx" :resume? true})]
      (is (= :hold (:disposition (:state r))))
      (is (= :placed (order/overall-status (store/order-record st "ord-1")))))))

(deftest order-concern-escalates-and-threads-the-real-proposal
  (let [distinctive (str "TEST-CONCERN-" (rand-int 1000000000))
        st (store/seed-db)
        actor (operation/build st)]
    (place! actor st "t-1" "ord-1" ["merchant.alpha"])
    (let [held (exec actor "t-concern"
                     {:op :flag-order-concern :order-id "ord-1"
                      :patch {:concern distinctive}})]
      (is (= :interrupted (:status held)))
      (let [ok (g/run* actor {:approval {:status :approved :by "ops-01"}}
                       {:thread-id "t-concern" :resume? true})]
        (is (= :done (:status ok)))
        (is (= distinctive (:concern (:payload (last (store/order-log st))))))))))

(deftest phase-gates-are-wired-into-the-compiled-graph
  (testing "phase 1 has not enabled status recording yet"
    (let [st (store/seed-db)
          actor (operation/build st)]
      (place! actor st "t-1" "ord-1" ["merchant.alpha"])
      (let [r (exec actor "t-phase1"
                    {:op :advance-sub-order :order-id "ord-1"
                     :patch {:seller "merchant.alpha" :to :confirmed}}
                    (assoc op-context :phase 1))]
        (is (= :hold (:disposition (:state r))))
        (is (= :phase-disabled (:phase-reason (last (store/ledger st))))))))
  (testing "phase 0 writes nothing"
    (let [st (store/seed-db)
          actor (operation/build st)
          r (exec actor "t-phase0"
                  {:op :place-order :order-id "ord-9"
                   :patch {:buyer "b" :lines [{:offer-id (offer-of st "merchant.alpha") :qty 1}]}}
                  (assoc op-context :phase 0))]
      (is (= :hold (:disposition (:state r))))
      (is (empty? (store/order-log st))))))

(ns orderops.governor-test
  (:require [clojure.test :refer [deftest is testing]]
            [marketplace.order :as order]
            [orderops.advisor :as advisor]
            [orderops.governor :as governor]
            [orderops.store :as store]))

(def now "2026-06-01T00:00:00Z")
(def ctx {:actor-id "order-actor" :phase 3 :now now})

(defn- db [] (store/seed-db))

(defn- offer-of [st seller]
  (->> (store/all-offer-records st)
       (filter #(= seller (:offer/seller %))) first :offer/id))

(defn- advise [st op & [req]]
  (advisor/-advise (advisor/mock-advisor) st (merge {:op op} req)))

(defn- check [st op & [req]]
  (governor/check (merge {:op op} req) ctx (advise st op req) st))

(defn- place-req [st & sellers]
  {:order-id "ord-1"
   :patch {:buyer "buyer-1"
           :lines (mapv (fn [s] {:offer-id (offer-of st s) :qty 1}) sellers)}})

;; ───────────────────── price comes from the catalog ─────────────────────

(deftest a-buyer-cannot-name-their-own-price
  (testing "there is no code path that reads a price from the request —
            the unit price is read from the catalog offer"
    (let [st (db)
          req (assoc-in (place-req st "merchant.alpha")
                        [:patch :lines 0 :unit-price-minor] 1)
          p (advise st :place-order req)
          o (get-in p [:value :order])]
      (is (= 1200 (order/total-minor o)) "the offer's price, not the payload's 1")
      (is (false? (:hard? (governor/check req ctx p st)))))))

(deftest an-unknown-offer-is-a-hard-block
  (let [st (db)
        req {:order-id "ord-1" :patch {:buyer "b" :lines [{:offer-id "offer.nope" :qty 1}]}}
        v (governor/check req ctx (advise st :place-order req) st)]
    (is (true? (:hard? v)))
    (is (some #{:unknown-offer} (mapv :rule (:violations v))))))

(deftest an-empty-order-is-a-hard-block
  (let [st (db)
        req {:order-id "ord-1" :patch {:buyer "b" :lines []}}
        v (governor/check req ctx (advise st :place-order req) st)]
    (is (true? (:hard? v)))
    (is (some #{:empty-order} (mapv :rule (:violations v))))))

;; ───────────────────── seller eligibility is read, not decided ─────────────────────

(deftest an-order-naming-an-unsellable-seller-is-refused
  (testing "merchant.gamma is identity-verified but not payout-bound —
            marketplace.seller/sellable? refuses, and this actor consumes
            that answer rather than re-deciding it"
    (let [st (db)
          req (place-req st "merchant.gamma")
          v (governor/check req ctx (advise st :place-order req) st)]
      (is (true? (:hard? v)))
      (is (some #{:seller-not-sellable} (mapv :rule (:violations v)))))))

(deftest one-bad-seller-refuses-the-whole-order
  (let [st (db)
        req (place-req st "merchant.alpha" "merchant.gamma")
        v (governor/check req ctx (advise st :place-order req) st)]
    (is (true? (:hard? v)))
    (is (some #{:seller-not-sellable} (mapv :rule (:violations v))))))

(deftest a-clean-multi-seller-order-passes
  (let [st (db)
        req (place-req st "merchant.alpha" "merchant.beta")
        p (advise st :place-order req)
        v (governor/check req ctx p st)
        o (get-in p [:value :order])]
    (is (false? (:hard? v)) (pr-str (:violations v)))
    (is (true? (:ok? v)))
    (is (= ["merchant.alpha" "merchant.beta"] (:order/sellers o)))
    (is (= 2300 (order/total-minor o)) "1200 + 1100")))

(deftest an-expired-credential-blocks-placing
  (let [st (db)
        req (place-req st "merchant.alpha")
        v (governor/check req (assoc ctx :now "2028-01-01T00:00:00Z")
                          (advise st :place-order req) st)]
    (is (true? (:hard? v)))
    (is (some #{:seller-not-sellable} (mapv :rule (:violations v))))))

;; ───────────────────── transitions obey okaimono ─────────────────────

(defn- with-order [st]
  (let [req (place-req st "merchant.alpha" "merchant.beta")
        p (advise st :place-order req)]
    (store/commit-record! st {:op :place-order :value (:value p)})
    st))

(deftest a-legal-transition-passes
  (let [st (with-order (db))
        req {:order-id "ord-1" :patch {:seller "merchant.alpha" :to :confirmed}}
        v (governor/check (merge {:op :advance-sub-order} req) ctx
                          (advise st :advance-sub-order req) st)]
    (is (false? (:hard? v)) (pr-str (:violations v)))
    (is (true? (:ok? v)) "recording a fact that already happened may auto-commit")))

(deftest an-illegal-transition-is-a-hard-block
  (testing "the transition table belongs to kotoba.okaimono — this actor
            obeys it rather than re-implementing it"
    (let [st (with-order (db))
          req {:order-id "ord-1" :patch {:seller "merchant.alpha" :to :delivered}}
          v (governor/check (merge {:op :advance-sub-order} req) ctx
                            (advise st :advance-sub-order req) st)]
      (is (true? (:hard? v)))
      (is (some #{:illegal-transition} (mapv :rule (:violations v)))))))

(deftest transitions-on-unknown-orders-and-sellers-are-refused
  (let [st (with-order (db))]
    (let [req {:order-id "ord-nope" :patch {:seller "merchant.alpha" :to :confirmed}}
          v (governor/check (merge {:op :advance-sub-order} req) ctx
                            (advise st :advance-sub-order req) st)]
      (is (some #{:order-unknown} (mapv :rule (:violations v)))))
    (let [req {:order-id "ord-1" :patch {:seller "merchant.nobody" :to :confirmed}}
          v (governor/check (merge {:op :advance-sub-order} req) ctx
                            (advise st :advance-sub-order req) st)]
      (is (some #{:seller-not-on-order} (mapv :rule (:violations v)))))))

(deftest a-lapsed-credential-does-not-strand-an-existing-order
  (testing "once an order exists, a seller whose credential later lapses
            must still be able to have their parcel marked delivered —
            refusing would leave the buyer's goods in an un-closeable order"
    (let [st (with-order (db))
          req {:order-id "ord-1" :patch {:seller "merchant.alpha" :to :confirmed}}
          v (governor/check (merge {:op :advance-sub-order} req)
                            (assoc ctx :now "2028-01-01T00:00:00Z")
                            (advise st :advance-sub-order req) st)]
      (is (false? (:hard? v)) (pr-str (:violations v))))))

;; ───────────────────── escalation ─────────────────────

(deftest cancellation-always-escalates
  (testing "a cancellation reaches into a settlement plan that may already
            exist and an escrow that may already be open"
    (let [st (with-order (db))
          req {:order-id "ord-1" :patch {:seller "merchant.alpha" :reason "在庫切れ"
                                         :confidence 0.99}}
          v (governor/check (merge {:op :cancel-sub-order} req) ctx
                            (advise st :cancel-sub-order req) st)]
      (is (false? (:hard? v)) (pr-str (:violations v)))
      (is (true? (:high-stakes? v)))
      (is (false? (:ok? v))))))

(deftest order-concern-always-escalates
  (let [st (with-order (db))
        v (check st :flag-order-concern {:order-id "ord-1"
                                         :patch {:concern "x" :confidence 0.99}})]
    (is (true? (:high-stakes? v)))
    (is (false? (:ok? v)))))

;; ───────────────────── structural checks ─────────────────────

(deftest effect-must-be-propose
  (let [st (db)
        req (place-req st "merchant.alpha")
        v (governor/check req ctx (assoc (advise st :place-order req) :effect :commit) st)]
    (is (true? (:hard? v)))
    (is (some #{:effect-not-propose} (mapv :rule (:violations v))))))

(deftest op-outside-the-allowlist-is-a-scope-violation
  (testing "no op that ships, pays or refunds is ever in the allowlist"
    (doseq [op [:ship-order :refund-order :release-payment]]
      (let [v (governor/check {:op op} ctx {:op op :effect :propose :confidence 0.99} (db))]
        (is (true? (:hard? v)) (str op))
        (is (some #{:op-not-allowed} (mapv :rule (:violations v))) (str op))))))

(deftest scope-exclusion-blocks-claims-of-having-shipped-or-refunded
  (let [st (with-order (db))
        p (advisor/infer st {:op :advance-sub-order :order-id "ord-1"
                             :patch {:seller "merchant.alpha" :to :confirmed}
                             :out-of-scope? true})
        v (governor/check {:op :advance-sub-order :order-id "ord-1"} ctx p st)]
    (is (true? (:hard? v)))
    (is (some #{:scope-excluded} (mapv :rule (:violations v))))))

(deftest default-mock-advisor-proposals-never-self-trip-scope-exclusion
  (testing "legitimate status proposals must talk about deliveries and
            cancellations — the excluded terms are phrased as COMPLETED
            actions so the happy path never self-blocks"
    (let [st (with-order (db))]
      (doseq [[op req]
              [[:place-order (place-req st "merchant.alpha")]
               [:advance-sub-order {:order-id "ord-1" :patch {:seller "merchant.alpha" :to :confirmed}}]
               [:cancel-sub-order {:order-id "ord-1" :patch {:seller "merchant.alpha" :reason "返金希望"}}]
               [:flag-order-concern {:order-id "ord-1" :patch {:concern "配達遅延"}}]]]
        (let [v (governor/check (merge {:op op} req) ctx (advise st op req) st)]
          (is (not-any? #{:scope-excluded} (mapv :rule (:violations v))) (str op)))))))

(deftest low-confidence-escalates
  (let [st (db)
        req (place-req st "merchant.alpha")
        v (governor/check req ctx (assoc (advise st :place-order req) :confidence 0.2) st)]
    (is (false? (:hard? v)))
    (is (true? (:escalate? v)))))

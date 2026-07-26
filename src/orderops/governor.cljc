(ns orderops.governor
  "OrderGovernor -- the compliance layer for the fleet's first
  cross-actor orchestrator.

  Orchestration creates a failure mode the single-actor repos do not
  have: an actor that can *reach into* several domains can also
  contradict them. This governor's job is therefore mostly negative —
  it makes sure the order actor never overrules the actors it composes.

  Six HARD checks, ALL permanent, un-overridable by any human approval:

    1. Unknown offer          -- every line must resolve to a real
                                 catalog offer. The PRICE comes from
                                 that offer, never from the request, so
                                 an unresolvable line is refused rather
                                 than priced from the payload.
    2. Seller not sellable    -- every seller on the order must be
                                 `marketplace.seller/sellable?`. Read
                                 from the credential `-onboarding`
                                 issued; this actor never re-decides who
                                 may trade.
    3. Malformed order        -- delegated to
                                 `marketplace.order/order-errors`
                                 (duplicate seller sub-orders, mixed
                                 currency, an invalid okaimono part).
    4. Illegal transition     -- an `:advance-sub-order` whose move
                                 `kotoba.okaimono` does not allow. The
                                 transition table is the courier's and
                                 the shop's; this actor obeys it.
    5. Effect not :propose    -- any other value is a claim to directly
                                 actuate outside governance.
    6. Scope exclusion        -- any claim to have SHIPPED, DELIVERED,
                                 REFUNDED or PAID, plus any op outside
                                 the closed allowlist. This actor
                                 records that a physical or financial
                                 event happened; it never performs one.
                                 Delivery is the courier's fact,
                                 payment is settlement's.

  Two ESCALATE (SOFT) gates:
    - LLM confidence below the floor.
    - `:cancel-sub-order` and `:flag-order-concern` ALWAYS escalate.
      Cancelling has money consequences downstream (a settlement plan
      already computed, possibly an escrow already open), so it is never
      a machine's unattended call.

  ## Why `:advance-sub-order` may auto-commit

  Moving a sub-order along is recording something that already happened
  in the physical world — the warehouse packed it, the courier took it,
  the buyer received it. Refusing to record a fact until a human agrees
  does not make the fact less true, it just makes the system's view
  stale. The safety is that the transition table decides what is even
  expressible, and that `:delivered` (the one with money consequences)
  is what settlement independently re-checks before releasing anything."
  (:require [clojure.string :as str]
            [kotoba.okaimono :as ok]
            [marketplace.buyer :as buyer]
            [marketplace.order :as order]
            [orderops.store :as store]))

(def confidence-floor 0.6)

(def allowed-ops
  "The closed proposal-op allowlist. CRITICAL: no op that ships, pays or
  refunds is EVER a member -- those belong to the courier, settlement
  and the dispute path respectively."
  #{:place-order :advance-sub-order :cancel-sub-order :flag-order-concern})

(def always-escalate-ops
  #{:cancel-sub-order :flag-order-concern})

(def scope-excluded-terms
  "Case-insensitive substrings marking a proposal as claiming to have
  performed a physical or financial act.

  CRITICAL: every term is phrased as the COMPLETED action ('shipped the
  parcel'), never a bare noun like 'shipment', 'delivery' or 'refund' --
  a bare noun would match inside this actor's own legitimate
  status-recording proposals (whose whole job is to talk about
  deliveries) and self-block the happy path. See
  `orderops.governor-test/default-mock-advisor-proposals-never-self-trip-scope-exclusion`."
  ["shipped the parcel" "have shipped the" "dispatched the courier"
   "delivered the parcel to" "have delivered the parcel"
   "refunded the buyer" "have refunded" "issued the refund"
   "paid the seller" "released the funds" "transferred the funds"
   "cancelled the payment" "reversed the charge"
   "決定的に配達を完了させた" "返金を実行した" "返金した"
   "送金した" "支払いを実行した" "出荷を実行した"])

;; ----------------------------- checks -----------------------------

(defn- order-of
  "The order a proposal is about: the one it carries, or the stored one
  it names."
  [proposal st]
  (or (get-in proposal [:value :order])
      (some->> (get-in proposal [:value :order-id]) (store/order-record st))))

(defn- unknown-offer-violations
  "Every line of a `:place-order` must resolve to a real catalog offer."
  [proposal st]
  (when (= :place-order (:op proposal))
    (let [reqs (get-in proposal [:value :lines] [])
          missing (remove #(store/offer-record st (:offer-id %)) reqs)]
      (cond
        (empty? reqs)
        [{:rule :empty-order :detail "明細の無い注文は作成できない"}]

        (seq missing)
        (mapv (fn [m] {:rule :unknown-offer
                       :detail (str (or (:offer-id m) "(offer-id missing)")
                                    " はカタログに存在しないオファー")})
              missing)))))

(defn- unsellable-seller-violations
  "Every seller on the order must be admitted to trade RIGHT NOW.

  Re-derived from the credential `-marketplace-onboarding` issued, never
  from the proposal. Applies to `:place-order` only: once an order
  exists, a seller whose credential later lapses must still be able to
  have their existing parcel marked delivered — refusing that would
  strand the buyer's goods in an un-closeable order."
  [proposal st now]
  (when (= :place-order (:op proposal))
    (let [o (order-of proposal st)
          sellers (or (:order/sellers o)
                      (->> (get-in proposal [:value :lines] [])
                           (keep #(some->> (:offer-id %) (store/offer-record st) :offer/seller))
                           distinct))]
      (seq
       (for [s sellers
             :when (not (store/sellable? st s now))]
         {:rule :seller-not-sellable
          :detail (str s " は現在出品/取引が許可されていない出品者")})))))

(defn- malformed-order-violations
  [proposal st]
  (when (= :place-order (:op proposal))
    (if-let [o (order-of proposal st)]
      (when-let [errs (seq (order/order-errors o))]
        (mapv (fn [e] {:rule (:order.error/code e)
                       :detail (or (:order.error/detail e)
                                   (name (:order.error/code e)))})
              errs))
      [{:rule :order-unbuildable
        :detail "明細からオーダーを構成できない(数量・価格が不正)"}])))

(defn- illegal-transition-violations
  "An `:advance-sub-order` / `:cancel-sub-order` must name a real order,
  a real seller on it, and a move `kotoba.okaimono` allows.

  The transition table belongs to okaimono — the shop and the courier
  already share it — so this actor asks rather than re-implements."
  [proposal st]
  (when (contains? #{:advance-sub-order :cancel-sub-order} (:op proposal))
    (let [{:keys [order-id seller to]} (:value proposal)
          o (and order-id (store/order-record st order-id))]
      (cond
        (nil? o)
        [{:rule :order-unknown :detail (str (or order-id "(order-id missing)"))}]

        (nil? (order/sub-order o seller))
        [{:rule :seller-not-on-order :detail (str seller)}]

        (nil? (order/advance-sub-order o seller to))
        [{:rule :illegal-transition
          :detail (str (:okaimono/status (order/sub-order o seller))
                       " -> " (pr-str to) " は許可されていない遷移"
                       " (許可: " (pr-str (get ok/transitions
                                               (:okaimono/status (order/sub-order o seller))
                                               #{})) ")")}]))))

(defn- buyer-violations
  "For `:place-order`: the buyer must exist and be allowed to place THIS
  order under the operator's stated requirements.

  `:require-level` and `:needs-shipping?` come from `context`, not from
  a constant here. `marketplace.buyer` is explicit that there is no
  built-in 'over ¥X needs ID' rule, because that threshold is a
  jurisdiction- and product-specific operator decision; baking one in
  would impose it on every deployment.

  A `:guest` with only a contact string is a perfectly good buyer for a
  digital order. What refuses is a PHYSICAL order with no address in the
  destination -- a parcel with nowhere to go."
  [proposal st context]
  (when (= :place-order (:op proposal))
    (let [bid (get-in proposal [:value :buyer])
          b (store/buyer-account st bid)]
      (if-not b
        [{:rule :buyer-unknown
          :detail (str (or bid "(buyer missing)") " は登録されていない買い手")}]
        (when-let [errs (seq (buyer/purchase-errors
                              b {:require-level (:require-level context :guest)
                                 :needs-shipping? (:needs-shipping? context true)
                                 :destination (:destination context)}))]
          (mapv (fn [e] {:rule (:buyer.error/code e)
                         :detail (or (:buyer.error/detail e)
                                     (str (:buyer.error/field e))
                                     (name (:buyer.error/code e)))})
                errs))))))

(defn- pii-violations
  "A proposal must not embed an un-redacted buyer.

  Everything a proposal carries lands in an APPEND-ONLY ledger. A street
  address written there cannot be scrubbed later, and it will also reach
  whatever LLM context the advisor runs in. `marketplace.buyer/redact`
  exists for exactly this, and this check is what makes using it
  non-optional rather than a convention someone forgets."
  [proposal]
  (when (buyer/leaks-pii? (select-keys proposal [:summary :rationale :cites :value]))
    [{:rule :proposal-leaks-pii
      :detail "提案に未マスクの買い手情報が含まれる -- 追記専用台帳には後から消せない"}]))

(defn- effect-not-propose-violations [proposal]
  (when (not= :propose (:effect proposal))
    [{:rule :effect-not-propose
      :detail (str ":effect は :propose のみ許可されるが " (pr-str (:effect proposal)) " が提案された")}]))

(defn- text-blob [proposal]
  (str/lower-case (pr-str (select-keys proposal [:op :summary :rationale :cites :value]))))

(defn- scope-exclusion-violations [proposal]
  (let [op (:op proposal)
        blob (text-blob proposal)]
    (cond
      (not (contains? allowed-ops op))
      [{:rule :op-not-allowed
        :detail (str (pr-str op) " は許可された操作(closed allowlist)に含まれない")}]

      (some #(str/includes? blob %) scope-excluded-terms)
      [{:rule :scope-excluded
        :detail "出荷・配達・返金・送金の実行に触れる提案は永久に禁止 -- 本 actor は事実を記録するだけ"}])))

(defn check
  "Censors an OrderAdvisor proposal. `context` supplies `:now`."
  [_request context proposal store]
  (let [now (:now context)
        hard (into []
                   (concat (unknown-offer-violations proposal store)
                           (buyer-violations proposal store context)
                           (pii-violations proposal)
                           (unsellable-seller-violations proposal store now)
                           (malformed-order-violations proposal store)
                           (illegal-transition-violations proposal store)
                           (effect-not-propose-violations proposal)
                           (scope-exclusion-violations proposal)))
        conf (:confidence proposal 0.0)
        low? (< conf confidence-floor)
        stakes? (boolean (always-escalate-ops (:op proposal)))
        hard? (boolean (seq hard))]
    {:ok?          (and (not hard?) (not low?) (not stakes?))
     :violations   hard
     :confidence   conf
     :hard?        hard?
     :escalate?    (and (not hard?) (or low? stakes?))
     :high-stakes? stakes?}))

(defn hold-fact [request context verdict]
  {:t          :governor-hold
   :op         (:op request)
   :actor      (:actor-id context)
   :order-id   (:order-id request)
   :disposition :hold
   :basis      (mapv :rule (:violations verdict))
   :violations (:violations verdict)
   :confidence (:confidence verdict)})

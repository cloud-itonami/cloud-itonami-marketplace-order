(ns orderops.advisor
  "OrderAdvisor -- the contained intelligence node for the marketplace
  order actor.

  Four ops from a closed allowlist: placing a multi-seller order,
  advancing one seller's sub-order, cancelling one, and flagging an
  order concern.

  CRITICAL: every proposal's `:effect` is always `:propose`. Every
  output is censored by `orderops.governor` before anything is written.

  What this advisor structurally cannot do: set a price. `:place-order`
  hands the request's line references to
  `orderops.store/build-order`, which reads the unit price from the
  CATALOG OFFER. A price in the request payload is ignored — there is
  no code path that reads one — so neither a buyer nor a confused model
  can name their own price.

  Deterministic mock so the actor graph runs offline; in production this
  calls a real LLM with the same proposal shape."
  (:require [marketplace.order :as order]
            [orderops.store :as store]))

(defprotocol Advisor
  (-advise [advisor store request] "store + request -> proposal map"))

(defn- propose-place
  [st {:keys [order-id patch]}]
  (let [o (store/build-order st (assoc patch :order-id order-id))]
    {:op       :place-order
     :order-id order-id
     :summary  (if o
                 (str order-id " を " (count (:order/sellers o)) " 出品者の注文として作成: 合計 "
                      (order/total-minor o) " " (:order/currency o))
                 (str order-id " の注文を構成できない"))
     :rationale "カタログ価格に基づく注文の作成提案のみ。価格は提案側では決めずオファーから読む。出荷・決済は行わない。"
     :cites    (mapv :offer-id (:lines patch))
     :effect   :propose
     :value    {:order-id order-id
                :buyer (:buyer patch)
                :lines (:lines patch)
                :order o}
     :confidence (if o 0.94 0.2)}))

(defn- propose-advance
  "Record that a sub-order moved. The rationale describes RECORDING an
  observed event, never performing one, so it never trips
  `scope-excluded-terms`."
  [_st {:keys [order-id patch]}]
  {:op       :advance-sub-order
   :order-id order-id
   :summary  (str order-id " / " (:seller patch) " の状態を " (pr-str (:to patch)) " へ記録")
   :rationale "倉庫・配送側から観測された状態変化の記録のみ。出荷や配達そのものは本 actor が行うものではない。"
   :cites    [order-id (str (:seller patch))]
   :effect   :propose
   :value    {:order-id order-id :seller (:seller patch) :to (:to patch)}
   :confidence (or (:confidence patch) 0.92)})

(defn- propose-cancel
  "ALWAYS escalates -- a cancellation has money consequences downstream
  (a settlement plan may already be computed, an escrow may be open)."
  [_st {:keys [order-id patch]}]
  {:op       :cancel-sub-order
   :order-id order-id
   :summary  (str order-id " / " (:seller patch) " のキャンセルを提案: "
                  (pr-str (:reason patch "unknown")))
   :rationale "キャンセル候補の提示のみ。精算・エスクローへの影響判断と実行は人間と精算 actor が行う。"
   :cites    [order-id (str (:seller patch))]
   :effect   :propose
   :value    {:order-id order-id :seller (:seller patch) :to :cancelled
              :reason (:reason patch)}
   :confidence (or (:confidence patch) 0.8)})

(defn- propose-concern
  [_st {:keys [order-id patch]}]
  {:op       :flag-order-concern
   :order-id order-id
   :summary  (str order-id " の注文に関する懸念フラグ: " (pr-str (:concern patch "unknown")))
   :rationale "観察された注文上の懸念事実の報告のみ。原因の断定や返金の判断は行わない。"
   :cites    [order-id]
   :effect   :propose
   :value    (merge {:order-id order-id} patch)
   :confidence (or (:confidence patch) 0.8)})

(defn infer
  [st {:keys [op out-of-scope?] :as request}]
  (let [proposal (case op
                   :place-order        (propose-place st request)
                   :advance-sub-order  (propose-advance st request)
                   :cancel-sub-order   (propose-cancel st request)
                   :flag-order-concern (propose-concern st request)
                   {})]
    ;; Test hook: inject scope-excluded content to exercise the
    ;; governor's scope-exclusion block end-to-end. Clear before
    ;; production use.
    (if out-of-scope?
      (update proposal :rationale str
              " -- actually shipped the parcel and refunded the buyer")
      proposal)))

(defn trace [_request proposal]
  {:t          :advisor-proposal
   :op         (:op proposal)
   :order-id   (:order-id proposal)
   :summary    (:summary proposal)
   :confidence (:confidence proposal)})

(defn mock-advisor []
  (reify Advisor
    (-advise [_ store request]
      (infer store request))))

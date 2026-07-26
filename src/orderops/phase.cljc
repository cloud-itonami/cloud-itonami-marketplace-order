(ns orderops.phase
  "Phase 0->3 staged rollout for the marketplace order actor.

    Phase 0  read-only          -- no writes, still governor-gated.
    Phase 1  assisted-placing   -- orders may be placed, every write
                                   needs human approval.
    Phase 2  assisted-tracking  -- adds sub-order status recording,
                                   still approval-gated.
    Phase 3  supervised auto    -- governor-clean, high-confidence
                                   `:place-order` and
                                   `:advance-sub-order` may auto-commit.

  `:cancel-sub-order` and `:flag-order-concern` are deliberately ABSENT
  from every phase's `:auto` set, INCLUDING phase 3.

  ## Recording a fact is not the same as causing one

  This actor's auto set is unusually permissive for the fleet, and the
  reason is worth stating: `:advance-sub-order` records something that
  ALREADY HAPPENED in the physical world — the warehouse packed it, the
  courier collected it, the buyer received it. Refusing to record a fact
  until a human agrees does not make the fact less true; it just makes
  every downstream actor's view stale, and a stale delivery status is
  what strands a seller's money in an escrow that should have released.

  The safety comes from three places that are not this phase table:
  `kotoba.okaimono`'s transition table bounds what is even expressible,
  the governor re-derives seller eligibility and catalog prices, and
  `:delivered` — the transition with money consequences — is
  independently re-checked by the settlement actor before it releases
  anything. Three layers, none of them a human staring at a status
  update.

  `:cancel-sub-order` is the opposite: it reaches forward into a
  settlement plan that may already exist and an escrow that may already
  be open, so it is never a machine's unattended call.
  `orderops.governor`'s own `always-escalate-ops` enforces the same
  invariant independently."
  (:require [orderops.governor :as governor]))

(def read-ops #{})
(def write-ops governor/allowed-ops)

;; NOTE the invariant: `:cancel-sub-order` and `:flag-order-concern` are
;; members of `write-ops` (governor-gated like any write) but are NEVER
;; members of any phase's `:auto` set below. Do not add them there.
(def phases
  "phase -> {:label .. :writes <ops allowed to write> :auto <ops allowed
  to auto-commit when governor-clean>}."
  {0 {:label "read-only"         :writes #{}                :auto #{}}
   1 {:label "assisted-placing"  :writes #{:place-order}      :auto #{}}
   2 {:label "assisted-tracking" :writes #{:place-order :advance-sub-order} :auto #{}}
   3 {:label "supervised-auto"   :writes write-ops
      :auto #{:place-order :advance-sub-order}}})

(def default-phase 3)

(defn gate
  "Adjust a governor disposition for the rollout phase. Returns
  {:disposition kw :reason kw|nil}."
  [phase {:keys [op]} governor-disposition]
  (let [{:keys [writes auto]} (get phases phase (get phases default-phase))]
    (cond
      (= :hold governor-disposition)       {:disposition :hold :reason nil}
      (contains? read-ops op)              {:disposition governor-disposition :reason nil}
      (not (contains? writes op))          {:disposition :hold :reason :phase-disabled}
      (and (= :commit governor-disposition)
           (not (contains? auto op)))      {:disposition :escalate :reason :phase-approval}
      :else                                {:disposition governor-disposition :reason nil})))

(defn verdict->disposition
  "Map an OrderGovernor verdict to a base disposition before the phase
  gate."
  [verdict]
  (cond (:hard? verdict) :hold
        (:escalate? verdict) :escalate
        :else :commit))

# cloud-itonami-marketplace-order

Open Business Blueprint (implemented actor): **the fleet's first
cross-actor orchestrator.**

Every other `cloud-itonami-*` actor is self-contained by design: one
repo, one business, forkable alone. That is right for a single operator,
but it left a real hole — nothing tied listing to settlement to
fulfillment to delivery, so a *marketplace order* existed nowhere. This
actor is that seam.

**OrderAdvisor ⊣ OrderGovernor** on
[`langgraph`](https://github.com/kotoba-lang/langgraph). The order
contract is `marketplace.order` in
[`kotoba-lang/marketplace`](https://github.com/kotoba-lang/marketplace).
Design record:
[ADR-2607264000](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2607264000-marketplace-federated-commerce-layer.edn).

## A parent over per-seller sub-orders

`kotoba.okaimono` already models an order well — lines, totals, a COD
flag, an explicit transition table. But its order belongs to **one
store**. A marketplace basket does not: its lines belong to different
sellers who pick, pack and ship independently and may deliver days
apart.

So a marketplace order is a parent over per-seller `okaimono`
sub-orders. The courier actor
([`isic-5320`](https://github.com/cloud-itonami/cloud-itonami-isic-5320))
and the warehouse consume records they already understand, unchanged.

**Status is derived, never stored.** There is deliberately no setter. A
parent that kept its own status could tell a buyer "delivered" while one
seller's parcel sits in a depot, and `:partially-delivered` is its own
state precisely so that `fully-delivered?` can answer **false** for it —
releasing every seller's money because one delivered is the multi-seller
failure this contract exists to prevent.

## It reads other actors; it never overrules them

Orchestration creates a failure mode the single-actor repos do not have:
an actor that can reach into several domains can also contradict them.
So this one is deliberately a reader.

| Concern | Owner | This actor |
|---|---|---|
| May a seller trade? | `-marketplace-onboarding` credential | reads `marketplace.seller/sellable?` |
| What does it cost? | catalog offer | reads `:offer/price-minor` |
| Is this move legal? | `kotoba.okaimono/transitions` | asks, never re-implements |
| Who gets paid what? | `-marketplace-settlement` | hands over `->basket-lines` |
| Who picks it? | `-marketplace-fulfillment` | hands over the sub-order |

**A buyer cannot name their own price.** There is no code path that
reads a price from the request payload — `resolve-lines` takes it from
the catalog offer. The test
`a-buyer-cannot-name-their-own-price` puts `:unit-price-minor 1` in a
request and asserts the order still totals 1200.

## Six HARD checks (permanent, un-overridable)

| Check | What it catches |
|---|---|
| **Unknown offer** | a line that resolves to nothing in the catalog |
| **Seller not sellable** | any seller on the order not admitted to trade *right now* |
| **Malformed order** | duplicate seller sub-orders, mixed currency, an invalid `okaimono` part |
| **Illegal transition** | a move `kotoba.okaimono` does not allow |
| **Effect not `:propose`** | a proposal claiming to directly actuate |
| **Scope exclusion** | any claim to have shipped, delivered, refunded or paid; any op outside the allowlist |

Seller eligibility is checked on `:place-order` **only**. Once an order
exists, a seller whose credential later lapses must still be able to
have their parcel marked delivered — refusing that would strand the
buyer's goods in an un-closeable order. That asymmetry is tested
(`a-lapsed-credential-does-not-strand-an-existing-order`).

## Recording a fact is not the same as causing one

This actor's auto set is unusually permissive for the fleet, and the
reason matters: `:advance-sub-order` records something that **already
happened** — the warehouse packed it, the courier collected it, the
buyer received it. Refusing to record a fact until a human agrees does
not make the fact less true; it makes every downstream actor's view
stale, and a stale delivery status is what strands a seller's money in
an escrow that should have released.

The safety comes from three places that are not a human staring at a
status update: `okaimono`'s transition table bounds what is expressible,
the governor re-derives eligibility and prices, and `:delivered` — the
transition with money consequences — is independently re-checked by the
settlement actor before it releases anything.

`:cancel-sub-order` is the opposite. It reaches forward into a
settlement plan that may already exist and an escrow that may already be
open, so it **always** escalates.

## Buyer accounts, and the asymmetry with sellers

A seller RECEIVES money, so admitting one is a regulated act carrying
eKYC and AML. A buyer SPENDS their own money on a book. So the buyer
gate asks how little is needed, not how much can be verified:
`:require-level` and `:needs-shipping?` come from context, and there is
no built-in "over ¥X needs ID" rule — that threshold is a jurisdiction-
and product-specific operator decision.

A `:guest` with only a contact string buys a digital order fine. The
same buyer is refused a physical one, because a parcel needs somewhere
to go.

**A proposal embedding an un-redacted buyer is a HARD block.** Everything
a proposal carries lands in an append-only ledger and in whatever LLM
context the advisor runs in, and a street address written there cannot
be scrubbed later. `marketplace.buyer/redact` exists for this; the check
is what makes using it non-optional rather than a convention someone
forgets.

## Durable storage, and the host that provides it

`org-database-policy.edn` puts D1 transport, ref selection, CACAO,
encryption, blind indexing and secrets in the **host**, and leaves
datoms, queries and domain schema to the **application**.
`marketplace.persist/store` throws without an injected database API, so
this actor cannot come up durable-looking while writing to nothing, and
`durable?` reports false for the memory backend so a readiness check can
refuse it.

`orderops.edge.worker` is the host half — a Cloudflare Worker bound to
D1, running the real `kotobase.core` engine through
`kotobase-storage-d1`.

### load → compute → flush

The D1 provider is Promise-based; the actor is synchronous, and making
it async would ripple through every `Store` protocol in the fleet for no
benefit. So each request:

1. `await` the current state out of D1
2. run the actor synchronously against a recording db-api
3. `await` **one** transact of everything it wrote

One transact, so a request's writes land together or not at all, and
concurrent requests are made safe by kotobase's own
`kotobase_refs.revision` CAS rather than by anything invented here.

### Kotoba Component commit boundary

[`src/orderops/commit.kotoba`](src/orderops/commit.kotoba) is the first
application slice moved behind the standard Component boundary. The host
serializes one aggregate value containing the new order state and its immutable
audit facts, then the guest invokes exactly one `storage/transact` with an
expected version. Its application-owned request/result types deliberately
narrow `storage-v1` to conditional aggregate puts: create, replace,
conflict-current, conflict-missing, and provider error. Splitting create from
replace removes the optional-version ambiguity while the current Component
profile still rejects options nested inside capability record payloads. The
guest never retries a non-idempotent write.

This deliberately does not put Kotobase transport, CACAO credentials, database
selection, TLS, or retry policy in the language program. Those remain host
responsibilities. The component imports only the typed `storage-v1` WIT
function and receives no ambient WASI authority.

```bash
clojure -M:component compile \
  src/orderops/commit.kotoba --target component \
  --policy component-policy.edn --output orderops-commit.component.wasm
```

`murakumo.component.edn` pins the resident `orderops` instance to loopback
port `18911` with a storage-only grant. The daemon starts in `compile-only`
mode because this component intentionally exports the parameterized `commit`
function, not a fake parameterless `main`; every real call goes through
`POST /v1/invoke` and produces a signed invocation receipt.
The pinned asher qualification evidence is recorded in
`qualification/asher-20260727.edn`.

### Endpoints

| Route | Auth | |
|---|---|---|
| `GET /health` | — | datom count actually loaded from D1 |
| `POST /orders` | Bearer | place an order through the real governor |
| `GET /orders/:id` | — | read it back |
| `POST /admin/seed` | Bearer | write the reference buyers/sellers/offers |
| `GET /debug/datoms` | Bearer | the raw datom shape the store returns |

### D1 schema

The Worker needs **all** of `kotobase-storage-d1`'s migrations, not just
the first two. `0003_datomic_projection.sql` onward create the tables the
read path uses; without them `datoms` fails with `no such table:
kotobase_projection` and — because an over-broad `.catch` used to return
`[]` — the actor reported an empty catalog and refused every order with
no indication why. That `.catch` now only swallows an empty ref;
anything else propagates to `/health` as `:store-error`.

```bash
for f in orgs/kotoba-lang/kotobase-storage-d1/migrations/*.sql; do
  wrangler d1 execute cloud-itonami-marketplace --remote --file "$f" -y
done
```

Reads are open; writes are gated by `ORDER_WRITE_TOKEN` (a Cloudflare
secret, mirrored to the macOS Keychain). A public unauthenticated write
endpoint on a durable store is not a demo, it is an invitation.

```bash
clojure -M:dev:run   # basket → 2-seller order → settlement + warehouse projections
clojure -M:test      # 43 tests, 119 assertions
clojure -M:lint
npm run deploy       # build + wrangler deploy (D1-bound host)
```

## Rollout phases

| Phase | Writes | Auto-commits |
|---|---|---|
| 0 read-only | — | — |
| 1 assisted-placing | `:place-order` | — |
| 2 assisted-tracking | + `:advance-sub-order` | — |
| 3 supervised-auto | all | `:place-order` `:advance-sub-order` |

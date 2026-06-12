# Divergences from the Ably service — current state

> What this adapter does differently from (or not at all compared to)
> the production Ably service, as it stands today. This is a snapshot of
> the current state, not a change log: gaps that get closed are removed
> from this document. The adapter runs on either centrifuge engine: the
> memory engine (single-node, ephemeral — the default for the conformance
> gate) or the Redis engine (durable message history / channel serials /
> materialized mutable-message state across a restart, and multi-node —
> Phase 5/6). The multi-node bounds below (comet pinning, per-node nonce,
> per-node stats, cosmetic cross-node serial ordering) apply only on the
> Redis engine with more than one node.

## Not supported

Features of the Ably service this PoC simply does not implement:

- **Push notifications** (push-subscribe/push-admin capabilities parse,
  but there is no push machinery).
- **LiveObjects.**
- **Annotations and summaries** (mutable messages cover the AIT need).
- **Delta compression (vcdiff).** Centrifuge's Fossil deltas are
  incompatible; the `delta` channel param is accepted and ignored.
- **Server-side message filtering** (filtered channel subscriptions).
- **Channel enumeration, metachannels, and channel lifecycle metadata.**
- **Production durability / availability guarantees.** The Redis engine
  makes message history, channel serials and materialized mutable-message
  state durable across a restart/redeploy and serves them across nodes
  (Phase 5/6), but this is a PoC of the *sharding & delivery* architecture,
  not the Four Pillars: no failover-without-loss on node death, no
  connection rebalancing, no multi-region, and Redis data loss (AOF/RDB) is
  out of scope (Upstash's concern). On the memory engine all state is in
  RAM and a restart is a fresh world.
- **Stats are fixture-backed, not metered.** The server never records
  its own usage; `GET /stats` serves what `POST /stats` injected (the
  sandbox fixture mechanism the SDK test harness uses). Re-injecting an
  intervalId replaces its record. **Multi-node:** the stats fixture store
  is per-node, so `GET /stats` reflects whichever node the load balancer
  routes to — an accepted PoC divergence (P6.5).

## Behavioral differences

### Protocol & channels

- **The comet/HTTP-fallback transport is poll-only** (matching the
  current production service — xhr_streaming died with protocol v2):
  long-poll responses are complete `[...]\n` bodies, which node's
  streaming-mode client consumes as single chunks. Comet is JSON-only
  by SDK design.
- **Multi-node: comet works cross-node by forwarding per-key requests to
  the owning node (P6.4; see research/14-multinode-comet.md).** Comet
  per-key state (the `cometConn` and its connectionKey registry entry)
  stays in-process on the node that served `/comet/connect` — by design.
  The connectionKey embeds the owning node id
  (`<connectionId>!<centrifugeId>.<nodeId>`; opaque to the SDK, and TM2h
  attribution and the `recover` param read only up to the first `!`), and
  a `/comet/<key>/{recv,send,close,disconnect}` request landing on a
  DIFFERENT node is forwarded to the owner over the broker control
  channel — `node.Survey`, the same mechanism centrifuge's own emulation
  layer uses for its multi-node HTTP transports. No LB pinning is
  required; round-robin works. Failure edges keep the production 410
  contract: a dead/restarted owner (its node id is no longer a cluster
  member) or a forward timeout is `410 GONE`, the SDK reconnects fresh,
  and cross-node resume recovers continuity. Real Ably instead encodes
  placement into opaque connectionKeys with internal routing — same idea,
  different plumbing. Pinned by `TestMultiNodeCometCrossNode_P6_4`,
  `TestMultiNodeCometDeadOwner_P6_4`,
  `TestMultiNodeCometForeignIdentity_P6_4`. **Wiring:** a node's
  `OnSurvey` slot must dispatch the `ably_comet_*` ops. The production app
  muxes them into `survey.NewCaller` via
  `Handler.RegisterCometSurveyWith(surveyCaller.RegisterAsyncHandler)`
  (mux.go), so the channels survey API and comet forwarding share the
  slot; standalone embeddings/the test harness claim the free slot
  directly with `Handler.RegisterCometSurvey`. An embedding that wires
  neither keeps the pre-P6.4 410-reconnect behaviour (fail-fast, no
  hang). Operators MAY layer LB session affinity on `/comet/*` as an
  optimization (cuts forwarding rate); it is never load-bearing for
  correctness.
- **Multi-message publishes are delivered as N single-message frames**,
  each with its own channelSerial (`Message.serial` is always
  `<cs>:000`). Real Ably delivers one frame per atomic batch with
  `<cs>:<idx>` serials. Cursor semantics stay exact; a mid-batch NACK
  leaves the already-published prefix unrolled.
- **Mode-name coverage:** ATTACH params.modes recognizes
  presence/publish/subscribe/presence_subscribe; annotation/object mode
  names degrade to the unrestricted default grant (fail-open;
  capability checks still gate operations).

### Serials & continuity

- **Cursor resolution scans retained history** (≤1000 deep) per
  cursor-bearing ATTACH — O(retention); production wants an offset
  index.
- **A presence-event serial used as a resume cursor is unresolvable**
  (live presence publications are history-free): continuity is treated
  as lost (fresh attach, no RESUMED) — Ably re-syncs presence on resume
  anyway.
- **attachSerial can lag after trailing presence events** (ATTACHED
  reads the newest retained MESSAGE publication).
- **Presence SYNC pages at exactly 100 members** with channelSerial
  cursors `presence:<offset>` / final `presence:`; single-page syncs
  omit channelSerial. Both forms complete SDK syncs.
- **Multi-node: a channelSerial's lexicographic order need not match
  broker offset order (P6.3 audit).** Each node mints with its own
  per-process seriesId from its own per-channel generator, so two nodes
  publishing the same channel concurrently — or one node minting under
  clock skew relative to another — can emit serials whose string order
  disagrees with the order the broker assigns offsets. This is cosmetic:
  the adapter never compares serials lexicographically to order or filter
  — delivery is broker-offset order, and resume/untilAttach resolve a
  serial to its offset by EXACT-MATCH tag lookup (resolveCursor /
  resolveSerialOffset), so cross-node continuity and ordering are correct
  regardless. The residual (the channelSerial VALUE a client observes may
  be non-monotonic across a node boundary) is bounded by same-region
  NTP-synced clocks (skew ≪ 100ms) and D3's seed-from-history, which
  clamps a cold node's generator past the channel high-water on its first
  publish. Verified by TestMultiNodeSerialOrder_P6_3.

### Recovery & auth

- **RTN16-lite recover is unverified.** A UUID-shaped recover claim is
  adopted without a connection registry: connectionId is attribution
  metadata only, but a claimant can publish attributed to the claimed
  id, suppress echo of it, and cancel its pending presence-grace LEAVE.
  Production: signed/verifiable recovery keys + connection state store.
- **Token edge cases:** exp-less Ably-JWTs never expire; bare `x*`
  resource prefixes in capabilities match loosely; TM2h connectionKeys
  are attributed without verifying the connection exists.
- **RSA9d hygiene** (timestamp ±15min, nonce burn, 24h ttl cap) is
  implemented; same-node replay rejection is pinned end-to-end on the
  HTTP requestToken path by `TestRequestToken_RSA8` ("replayed nonce
  rejected 40101"), not just at the unit layer. The nonce cache is still
  in-memory per node, so cross-node replay within the tolerance window
  remains possible (P6.2b bound, detailed in the `nonceCache` comment).
- **Token revocation** (RSA17): on the Redis engine a revoke is fanned
  out cross-node over a reserved broker feed — every node applies it to
  its local store and live-disconnects matching sessions (P6.2a); on the
  memory engine it is single-node. `revocationKey:` targeting is accepted
  but ineffective (tokens here never carry the claim — `clientId:` is the
  effective specifier). Sessions adopting a clientId via mid-connection
  AUTH are not re-indexed for live disconnect; connect-time enforcement
  still applies.

### Presence

- **The presence member store is adapter-owned.** On the Redis engine it
  is cross-node — Redis-backed via a centrifuge presence manager, so
  HAS_PRESENCE/SYNC on one node reflect members entered on another (P6.1);
  on the memory engine it is single-node in-process. (The adapter rolls its
  own store because centrifuge's node-level presence mutators are
  unexported.) A dead node's members leave the shared set within the
  manager TTL (~60s); grace-window re-entry suppression is node-local
  (a redundant LEAVE event is possible across nodes — see presence.go).
- **Presence history rides a client-unreachable shadow channel**
  (`:presence:<ch>`), REST-only, no pagination Link headers.

### Mutable messages (AIT)

- **The mutableMessages rule is a namespace convention**
  (`mutable:`/`ai:` prefixes), standing in for the per-channel-rule
  flag a provisioning API would set.
- **The materialized store never evicts.**
- **Append concatenates only plain strings**; same-chain encoded
  payloads replace rather than byte-concatenate. AIT only appends plain
  token deltas.
- **Mutation ops commit store state before the broker publish**; a
  publish failure NACKs with state already moved (single-node: publish
  failures are node-shutdown-only).
- **GET …/versions returns a single page** (no Link pagination).

### REST & history

- **maxMessageSize accounting is marginally stricter than Ably's**
  (summed marshaled envelopes after normalization, including base64
  inflation for binary).
- **start/end history bounds are a post-filter on the fetched window** —
  filtered-out publications still consume page capacity.
- **untilAttach pagination re-resolves from_serial per page**: an
  attach-point publication evicted mid-walk ends pagination early.
  Backwards pages over a partially size-evicted window are truncated at
  the cursor; a FORWARDS read whose cursor offset was evicted silently
  skips ahead to the oldest retained publication — a gap, where real
  Ably's persistent storage would still have the messages.
- **Client-supplied non-zero publish timestamps are honored** (SDKs
  never send them); they could distort time-window rewind scans.
- **Batch publish msgpack bodies** normalize through a generic JSON
  round-trip: binary data arrives as bare base64 without the encoding
  segment (the single-channel path handles binary canonically).
- **CORS allow-origin echoes the requesting origin** (centrifugo
  middleware) where Ably sends `*`; functionally equivalent for
  browsers.

## Test-observability quirks (documented, not bugs)

- **Loopback ping floor:** HEARTBEAT echoes are delayed 1ms so ably-js's
  `responseTime > 0` assertion holds on loopback.
- **ably-go quirks** (SDK-side, wire is canonical): delta-gated receive
  decode, RawToString msgpack decoding.
- **Order-dependent ably-js presence trio** and the AIT
  `multiple subscribers` race are test-side timing assumptions; all
  pass in isolation (details in SCORECARD.md).

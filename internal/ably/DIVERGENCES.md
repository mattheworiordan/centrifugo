# Divergences from the Ably service — current state

> What this adapter does differently from (or not at all compared to)
> the production Ably service, as it stands today. This is a snapshot of
> the current state, not a change log: gaps that get closed are removed
> from this document. Single-node PoC throughout — the broker is
> centrifuge's memory engine.

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
- **Clustering / multi-node / durable storage.** One node, all state in
  memory: message history, presence, materialized mutable messages,
  token nonces, revocations, stats. A restart or redeploy is a fresh
  world. History retention mimics Ably's tiers (persistent channels
  1000 msgs / 24h; everything else ~2 min) but in RAM.
- **Stats are fixture-backed, not metered.** The server never records
  its own usage; `GET /stats` serves what `POST /stats` injected (the
  sandbox fixture mechanism the SDK test harness uses). Re-injecting an
  intervalId replaces its record.

## Behavioral differences

### Protocol & channels

- **The comet/HTTP-fallback transport is poll-only** (matching the
  current production service — xhr_streaming died with protocol v2):
  long-poll responses are complete `[...]\n` bodies, which node's
  streaming-mode client consumes as single chunks. Comet is JSON-only
  by SDK design.
- **Comet per-key requests are bearer-authorized**: any valid app
  credential plus the (unguessable, per-session `uuid!uuid`)
  connectionKey may send/recv/close a session — the request identity is
  not re-bound to the session's identity on every request as the
  production service does. SDK reauth flows work in-band via AUTH
  frames over /send, so no SDK behavior depends on the difference.

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
  implemented but the nonce cache is in-memory per node.
- **Token revocation** (RSA17) is in-memory per node;
  `revocationKey:` targeting is accepted but ineffective (tokens here
  never carry the claim — `clientId:` is the effective specifier).
  Sessions adopting a clientId via mid-connection AUTH are not
  re-indexed for live disconnect; connect-time enforcement still
  applies.

### Presence

- **The presence member store is adapter-owned, single-node**
  (centrifuge's node-level presence mutators are unexported).
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

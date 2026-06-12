package ably

// D4 — materialized-state durability. The materialized store is in-process,
// so after a restart (or an A4b-class eviction) a read of a mutable message
// misses. This rebuilds the materialized state lazily from the op stream in
// broker history: every create/update/append/delete is a publication on the
// channel, so replaying them in offset order reconstructs the state. On the
// Redis engine that history survives a restart (D1), so the materialized
// state is durable; on memory it is empty after a restart (no-op).
//
// Bounded-window caveat: broker history is exact-MAXLEN-trimmed at
// persistedHistorySize (1000). A message whose op stream exceeds 1000 ops
// has its create + early appends trimmed, so this replay reconstructs only a
// suffix. Snapshot-compaction (periodic full-state snapshots published to
// history) lifts that bound — tracked as a D4 follow-on; this delivers the
// common-case (<=1000 ops) durability and the D7 restart-survival proof.

import (
	"encoding/json"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
)

// rebuildMaterialized lazily reconstructs a mutable channel's materialized
// state from broker history the first time it is needed in this process.
// Safe to call on every materialized read (REST get/versions and realtime
// rewind): it rebuilds at most once per channel — the store tracks loaded
// channels and a single goroutine owns the replay (concurrent replays would
// double-apply ops) — and a channel already populated by live traffic is
// marked loaded without a history read.
func rebuildMaterialized(node *centrifuge.Node, store *materializedStore, channel string) {
	if !mutableChannel(channel) || store.markLoadedIfAbsent(channel) {
		return // already loaded (or populated by live traffic), or not mutable
	}
	res, err := node.History(brokerChannel(channel), centrifuge.WithLimit(persistedHistorySize))
	if err != nil {
		// History unavailable: revert the claim so a later read retries
		// rather than caching an empty result.
		store.clearLoaded(channel)
		return
	}
	// Transient-empty window (accepted): a concurrent reader that lost the
	// load claim saw markLoadedIfAbsent==true and returned before this replay
	// populated the store, so its lookup may 404 once until the replay lands.
	// That self-healing miss is strictly preferable to the alternative
	// (load-then-mark), which would let two goroutines replay and double-apply
	// appends — corrupting the concatenated state.
	//
	// Publications are oldest-first: replay create -> ops in offset order,
	// exactly as live traffic would have applied them. Reuses the store's
	// create/mutate so the materialization rules (append concat, update
	// replace, version capping) are identical to the live path. An op whose
	// create was trimmed (the >1000 bound) is dropped by mutate's not-found
	// path — a documented partial reconstruction, not a crash.
	for _, pub := range res.Publications {
		var m protocol.Message
		if err := json.Unmarshal(pub.Data, &m); err != nil {
			continue
		}
		switch m.Action {
		case protocol.MessageActionCreate:
			store.create(channel, &m)
		case protocol.MessageActionUpdate, protocol.MessageActionAppend, protocol.MessageActionDelete:
			store.mutate(channel, m.Serial, m.Action, m.Data, m.Encoding, m.Extras, m.Version)
		}
	}
}

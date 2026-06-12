package ably

// P6.2a — cross-node revocation fan-out. A revoke accepted on one node must
// (1) refuse the revoked token's NEW connections on every node (connect-time
// enforcement consults the per-node revocationStore) and (2) live-disconnect
// matching sessions on every node. The revoking node already does both
// synchronously for itself (serveRevokeTokens); to reach the other nodes it
// publishes each entry to a client-unreachable reserved feed channel with
// persistent history, and every node runs a sweep that pulls entries issued
// elsewhere and applies them locally (add to the store + enforce).
//
// Why a feed channel rather than centrifuge's Notify/OnNotification: the
// node-level OnNotification handler is a single slot already owned by the app
// (usage stats), so the adapter cannot claim it. node.Publish/node.History
// over a reserved ':' channel is the same proven primitive the presence
// shadow channel uses and works cross-node on the Redis broker.
//
// Bound: a node applies an out-of-node revoke within revocationSyncInterval
// (the revoking node is immediate). Single-node (memory engine) skips the
// feed+sweep entirely — synchronous enforcement already covers it.

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// revocationFeedChannel carries revocation events for cross-node fan-out. The
// ':' prefix makes it unattachable by any Ably client (40010).
const revocationFeedChannel = ":ably:revocations"

// pubTagRevokeOrigin tags a feed publication with the issuing node's ID so a
// node skips re-applying its OWN entries (already applied synchronously).
const pubTagRevokeOrigin = "ro"

// revocationSyncInterval bounds how long an out-of-node revoke takes to apply
// on a given node (connect-time + live-disconnect). The revoking node is
// immediate; this is the cross-node lag.
const revocationSyncInterval = time.Second

// wireRevocation is the JSON shape of a revocationEntry on the feed.
type wireRevocation struct {
	KeyName      string `json:"k"`
	Typ          string `json:"t"`
	Value        string `json:"v"`
	IssuedBefore int64  `json:"ib"`
	AppliesAt    int64  `json:"aa"`
}

func (e revocationEntry) wire() wireRevocation {
	return wireRevocation{KeyName: e.keyName, Typ: e.typ, Value: e.value, IssuedBefore: e.issuedBefore, AppliesAt: e.appliesAt}
}

func (w wireRevocation) entry() revocationEntry {
	return revocationEntry{keyName: w.KeyName, typ: w.Typ, value: w.Value, issuedBefore: w.IssuedBefore, appliesAt: w.AppliesAt}
}

// identity is a stable key for dedup: two feed entries with the same identity
// are the same revoke and must be applied at most once per node.
func (w wireRevocation) identity() string {
	return w.KeyName + "|" + w.Typ + "|" + w.Value + "|" +
		strconv.FormatInt(w.IssuedBefore, 10) + "|" + strconv.FormatInt(w.AppliesAt, 10)
}

// publishRevocation fans a revocation entry out to all nodes via the reserved
// feed channel (persistent history so a freshly-booted node catches up). No-op
// single-node: nothing consumes the feed, so don't pay the publish.
func (h *Handler) publishRevocation(e revocationEntry) {
	if !h.multiNode {
		return
	}
	data, err := json.Marshal(e.wire())
	if err != nil {
		log.Error().Err(err).Msg("ably revocation: marshal feed entry")
		return
	}
	if _, err := h.node.Publish(brokerChannel(revocationFeedChannel), data,
		centrifuge.WithTags(map[string]string{pubTagRevokeOrigin: h.node.ID()}),
		centrifuge.WithHistory(persistedHistorySize, persistedHistoryTTL)); err != nil {
		log.Error().Err(err).Msg("ably revocation: publish to cross-node feed")
	}
}

// startRevocationSync pulls revocations issued on OTHER nodes from the feed
// and applies them locally until stop is closed. Multi-node only.
func (h *Handler) startRevocationSync(stop <-chan struct{}) {
	if !h.multiNode {
		return
	}
	// seen is fresh per process: a restarting node re-reads the retained feed
	// and re-applies historical revokes once (bounded by the 1000-entry
	// history window). That is intentional — it rebuilds connect-time
	// enforcement for pre-boot revokes; the re-enforce finds no matching live
	// sessions on a just-booted node, so it is a harmless no-op.
	seen := make(map[string]bool) // sweep-goroutine-local; no lock needed
	go func() {
		ticker := time.NewTicker(revocationSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				h.syncRevocations(seen)
			}
		}
	}()
}

// syncRevocations reads the feed and applies entries issued on other nodes
// that this node has not yet applied. Idempotent: origin-skip drops this
// node's own entries (applied synchronously) and the seen set drops entries
// already applied on a prior sweep.
func (h *Handler) syncRevocations(seen map[string]bool) {
	res, err := h.node.History(brokerChannel(revocationFeedChannel), centrifuge.WithLimit(persistedHistorySize))
	if err != nil {
		return // transient; retried next tick
	}
	self := h.node.ID()
	for _, pub := range res.Publications {
		if pub.Tags[pubTagRevokeOrigin] == self {
			continue // this node issued it and already applied it synchronously
		}
		var w wireRevocation
		if json.Unmarshal(pub.Data, &w) != nil {
			continue
		}
		id := w.identity()
		if seen[id] {
			continue
		}
		seen[id] = true
		e := w.entry()
		h.revocations.add(e)
		h.scheduleRevocationEnforcement(e)
	}
}

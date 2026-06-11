package ably

// A3 (HIGH, AIT-critical): the materialized store is bounded. Per entry
// the version history is capped (the AIT O(N²) fix); whole entries carry a
// retention TTL and are evicted.

import (
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// A long AIT append stream no longer retains a full-state snapshot per
// token: the version history is capped, while the latest materialized
// state stays complete.
func TestMaterializedVersionsCapped_A3(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	const channel = "ai:cap-test" // mutable → persistent retention tier
	store.create(channel, &protocol.Message{Serial: "s1", Name: "n", Data: "x", Timestamp: 100})

	const appends = maxVersionsRetained + 20
	want := "x"
	for i := 0; i < appends; i++ {
		delta := fmt.Sprintf("-%d", i)
		want += delta // plain-string, empty-chain appends concatenate
		_, prob := store.mutate(channel, "s1", protocol.MessageActionAppend, delta, "", nil,
			&protocol.MessageVersion{Serial: fmt.Sprintf("v%d", i)})
		require.Nil(t, prob)
	}

	// Latest state is the FULL concatenation — correctness preserved.
	state, ok := store.get(channel, "s1")
	require.True(t, ok)
	require.Equal(t, want, state.Data, "latest materialized state is the full stream")

	// Version history is bounded to the cap (not appends+1).
	versions, ok := store.versions(channel, "s1")
	require.True(t, ok)
	require.Equal(t, maxVersionsRetained, len(versions),
		"version history capped at the last N (the AIT O(N^2) fix)")
	require.Equal(t, want, versions[len(versions)-1].Data, "newest version reflects final state")
}

// The summed-byte guard drops oldest snapshots even under the count cap
// when individual snapshots are large.
func TestMaterializedVersionBytesCapped_A3(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	const channel = "ai:bytes-test"
	store.create(channel, &protocol.Message{Serial: "s1", Name: "n", Data: "", Timestamp: 100})

	// Each append adds ~100KB; three of them exceed the 256KB byte budget,
	// so the retained snapshot count stays below the count cap.
	big := make([]byte, 100*1024)
	for i := range big {
		big[i] = 'a'
	}
	for i := 0; i < 5; i++ {
		_, prob := store.mutate(channel, "s1", protocol.MessageActionAppend, string(big), "", nil,
			&protocol.MessageVersion{Serial: fmt.Sprintf("v%d", i)})
		require.Nil(t, prob)
	}
	versions, ok := store.versions(channel, "s1")
	require.True(t, ok)
	require.Less(t, len(versions), maxVersionsRetained,
		"the byte budget capped retention below the count cap")
	require.GreaterOrEqual(t, len(versions), 1, "the most recent snapshot is always kept")
}

// Whole entries are evicted once past their retention deadline.
func TestMaterializedTTLEviction_A3(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	const channel = "ai:ttl-test"
	store.create(channel, &protocol.Message{Serial: "s1", Name: "n", Data: "hi", Timestamp: 100})

	_, ok := store.get(channel, "s1")
	require.True(t, ok, "present before expiry")

	// Sweep with a clock past the 24h persistent retention deadline.
	future := time.Now().Add(25 * time.Hour).UnixMilli()
	require.Equal(t, 1, store.sweepExpired(future), "the expired entry is evicted")

	_, ok = store.get(channel, "s1")
	require.False(t, ok, "gone after eviction")

	// The channel's now-empty maps are cleaned up (memory reclaimed).
	store.mu.Lock()
	_, chOK := store.channels[channel]
	_, orderOK := store.order[channel]
	store.mu.Unlock()
	require.False(t, chOK, "empty channel entry-map removed")
	require.False(t, orderOK, "empty channel order removed")
}

// A live entry is NOT swept (boundary: now < expiresAt).
func TestMaterializedTTLKeepsLiveEntry_A3(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	const channel = "ai:ttl-live"
	store.create(channel, &protocol.Message{Serial: "s1", Name: "n", Data: "hi", Timestamp: 100})

	require.Equal(t, 0, store.sweepExpired(time.Now().UnixMilli()), "a live entry survives a sweep")
	_, ok := store.get(channel, "s1")
	require.True(t, ok)
}

package ably

// A4b: the serialMint evicts idle channel generators + locks to bound the
// channel-keyed maps. Eviction is refcount-guarded (never swaps a lock a
// publisher holds) and pairs with D3 (a re-publish to an evicted channel
// reseeds from the broker high-water, so serials never regress).

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
	"github.com/stretchr/testify/require"
)

func TestSerialMintEvictsIdleGenerators_A4b(t *testing.T) {
	t.Parallel()
	m := newSerialMint(func(string) (int64, int, bool) { return 0, 0, false })
	for _, ch := range []string{"a", "b", "c"} {
		unlock := m.lockChannel(ch)
		m.Mint(ch)
		unlock()
	}
	m.mu.Lock()
	require.Len(t, m.gens, 3)
	m.mu.Unlock()

	// Sweep with a clock past the idle TTL: all three (refs 0, idle) evicted.
	future := time.Now().Add(genEvictIdle + time.Minute).UnixMilli()
	evicted := m.sweepGenerators(future)
	require.Equal(t, 3, evicted)
	m.mu.Lock()
	require.Empty(t, m.gens, "idle generators evicted")
	require.Empty(t, m.locks, "idle locks evicted")
	m.mu.Unlock()
}

// A channel whose lock is HELD is never evicted, even if its lastUsed is
// stale (a publish held longer than the idle TTL) — the refcount protects
// it from having its *sync.Mutex swapped out from under the publisher.
func TestSerialMintRefcountProtectsHeldChannel_A4b(t *testing.T) {
	t.Parallel()
	m := newSerialMint(func(string) (int64, int, bool) { return 0, 0, false })
	unlock := m.lockChannel("held")
	m.Mint("held")
	// Force a stale lastUsed: simulate a publish that has held the lock
	// longer than the idle TTL.
	m.mu.Lock()
	m.lastUsed["held"] = 1
	m.mu.Unlock()

	future := time.Now().Add(genEvictIdle + time.Minute).UnixMilli()
	require.Equal(t, 0, m.sweepGenerators(future), "a held channel is not evicted despite a stale lastUsed")
	m.mu.Lock()
	_, live := m.gens["held"]
	require.True(t, live, "held channel's generator survives (refcount)")
	require.Equal(t, 1, m.refs["held"])
	m.mu.Unlock()
	unlock()
}

// Integration: publish (serial S1 lands in history), evict the generator,
// publish again → the cold mint reseeds from the broker high-water (D3) and
// the new serial is strictly greater. Eviction + reseed end-to-end.
func TestSerialMintEvictedChannelReseeds_A4b(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	const channel = "persisted:a4b-reseed"

	pub := func() {
		resp := restRequest(t, ts, http.MethodPost, "/channels/"+channel+"/messages",
			[]byte(`{"name":"ev","data":"x"}`), map[string]string{"Content-Type": "application/json"})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}
	pub()
	// Evict idle generators (refs 0, forced idle). The sweep also evicts the
	// startup presence-fixture channel, so assert OUR channel specifically is
	// gone rather than a global count.
	ts.handler.mint.sweepGenerators(time.Now().Add(genEvictIdle + time.Minute).UnixMilli())
	ts.handler.mint.mu.Lock()
	_, stillThere := ts.handler.mint.gens[channel]
	ts.handler.mint.mu.Unlock()
	require.False(t, stillThere, "our channel's generator was evicted")
	pub() // cold mint → reseeds from broker high-water

	hist := restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/messages", nil, nil)
	items := decodeMessagesBody(t, hist) // newest first
	require.Len(t, items, 2)
	newCS, _, err := serial.ParseMessageSerial(items[0].Serial)
	require.NoError(t, err)
	oldCS, _, err := serial.ParseMessageSerial(items[1].Serial)
	require.NoError(t, err)
	require.Greater(t, newCS, oldCS, "post-eviction serial reseeds above the prior high-water (no regression)")
}

// Concurrent publishers + an eviction sweep must be race-clean.
func TestSerialMintConcurrentEvictionRaceClean_A4b(t *testing.T) {
	t.Parallel()
	m := newSerialMint(func(string) (int64, int, bool) { return 0, 0, false })
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				unlock := m.lockChannel("hot")
				m.Mint("hot")
				unlock()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			m.sweepGenerators(time.Now().Add(genEvictIdle + time.Minute).UnixMilli())
		}
	}()
	wg.Wait()
	// Still usable after the churn.
	unlock := m.lockChannel("hot")
	require.NotEmpty(t, m.Mint("hot"))
	unlock()
}

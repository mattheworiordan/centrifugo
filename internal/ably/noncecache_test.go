package ably

// B4 (M4): the nonce cache no longer sweeps the whole map on every token
// request. A burst of N distinct nonces triggers at most one full sweep
// (O(N) total, not O(N²)), while replay rejection within the tolerance
// window is preserved.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNonceCacheBurstDoesNotSweepPerCall_B4(t *testing.T) {
	t.Parallel()
	c := newNonceCache()

	const n = 10000
	for i := 0; i < n; i++ {
		require.True(t, c.use(fmt.Sprintf("nonce-%d", i)), "each distinct nonce is fresh")
	}

	// Timing-independent structural assertion: the whole burst happens well
	// inside one nonceSweepInterval, so exactly one full sweep ran — NOT one
	// per call (the O(N²) regression).
	c.mu.Lock()
	sweeps := c.sweeps
	size := len(c.seen)
	c.mu.Unlock()
	require.Equal(t, 1, sweeps, "a burst must trigger at most one full sweep, not one per call")
	require.Equal(t, n, size, "all in-window nonces are retained for replay detection")
}

func TestNonceCacheRejectsReplayWithinWindow_B4(t *testing.T) {
	t.Parallel()
	c := newNonceCache()
	require.True(t, c.use("replay-me"), "first use is fresh")
	require.False(t, c.use("replay-me"), "immediate reuse within the window is a replay")
}

// An entry whose timestamp is older than the tolerance window is treated as
// fresh on access (lazy freshness, independent of when the sweep runs).
func TestNonceCacheStaleEntryTreatedFresh_B4(t *testing.T) {
	t.Parallel()
	c := newNonceCache()
	c.mu.Lock()
	c.seen["old"] = time.Now().Add(-3 * timestampTolerance) // older than 2*tolerance
	c.lastSweep = time.Now()                                // suppress the sweep so we test the per-access path
	c.mu.Unlock()
	require.True(t, c.use("old"), "a nonce outside the window is no longer a replay")
}

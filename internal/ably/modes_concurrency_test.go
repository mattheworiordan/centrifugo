package ably

// B3: attachedModes is written from BOTH goroutines (the centrifuge-writer
// in the opSubscribe/opUnsubscribe replies, the frame-reader on the
// re-attach and reauth-downgrade paths) — it is NOT goroutine-local, as a
// stale comment claimed. Every access holds modesMu; this test locks in
// that the concurrent access is race-free and the final state is never
// corrupted. -race is the teeth: drop modesMu from writeAttached or
// channelModes and this fails.

import (
	"sync"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestAttachedModesConcurrentAccessRaceClean_B3(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	s := newBareSession(ts, &blockingConn{}, capability)

	const channel = "b3-modes"
	const granted = protocol.FlagModeSubscribe | protocol.FlagModePublish
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(3)
	// Centrifuge-writer-side write (the opSubscribe reply path, session.go:1473).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			s.writeAttached(channel, nil, granted, 0)
		}
	}()
	// Frame-reader-side read (publish/enforcement path, channelModes).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = s.channelModes(channel)
		}
	}()
	// Frame-reader-side delete (reauth-downgrade / DETACHED reply path).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			s.modesMu.Lock()
			delete(s.attachedModes, channel)
			s.modesMu.Unlock()
		}
	}()
	wg.Wait()

	// The final value is either cleared (delete won last) or exactly the
	// granted bits (writeAttached won last) — never a torn/garbage value.
	m := s.channelModes(channel)
	require.True(t, m == 0 || m == granted,
		"attachedModes is cleared or the granted value, never corrupted: got %d", m)
}

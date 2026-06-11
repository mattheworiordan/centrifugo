package ably

// A5 (HIGH): a token-expiry timer callback that fires around a successful
// reauth must not tear down the renewed session. Timer.Stop cannot cancel
// an AfterFunc that has already begun running, so each callback is guarded
// by a generation it captured at arm time; a later (re)arm or stop bumps
// the generation and the stale callback no-ops.

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/stretchr/testify/require"
)

func (s *session) isClosedForTest() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed
}

func TestTokenExpiryGenerationGuard_A5(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)

	// Arm under generation 1. The token is far in the future so the real
	// AfterFunc timers won't fire during the test — we invoke the callbacks
	// directly, which is exactly the race the guard must survive: an expiry
	// callback running after a re-arm.
	s.params.tokenExpires = time.Now().Add(time.Hour).UnixMilli()
	s.armTokenExpiry()
	s.authMu.Lock()
	gen1 := s.expiryGen
	s.authMu.Unlock()

	// A successful reauth re-arms the timers → generation advances.
	s.armTokenExpiry()

	// The stale (gen1) expiry callback — the one Timer.Stop could not have
	// cancelled if it were already running — must NO-OP: no teardown, no
	// DISCONNECTED frame.
	s.fireTokenExpiry(gen1)
	require.False(t, s.isClosedForTest(),
		"a stale expiry callback must not tear down a session that just reauthed")
	conn.mu.Lock()
	wroteFrames := len(conn.frames)
	conn.mu.Unlock()
	require.Zero(t, wroteFrames, "the stale callback wrote no DISCONNECTED frame")

	// The CURRENT generation's callback DOES disconnect — a real expiry with
	// no intervening renewal still tears the session down (no regression).
	s.authMu.Lock()
	gen2 := s.expiryGen
	s.authMu.Unlock()
	s.fireTokenExpiry(gen2)
	require.True(t, s.isClosedForTest(), "the current-generation expiry disconnects (40142)")
}

// stopTokenExpiry also invalidates an in-flight callback: a callback armed
// before teardown must not re-disconnect.
func TestTokenExpiryStopInvalidatesCallback_A5(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)

	s.params.tokenExpires = time.Now().Add(time.Hour).UnixMilli()
	s.armTokenExpiry()
	s.authMu.Lock()
	gen := s.expiryGen
	s.authMu.Unlock()

	s.stopTokenExpiry() // bumps the generation

	s.fireTokenExpiry(gen)
	require.False(t, s.isClosedForTest(), "a callback superseded by stopTokenExpiry must no-op")
}

package ably

// B6: parkRecv's defer must re-arm the abandonment reaper ONLY when no poll
// is parked. Re-arming after a recv that was superseded by a successor runs
// the clock while a poll is actively parked — a stray reap of a
// continuously-polling client. This test drives the exact sequence: poll A
// parked, poll B supersedes A, B parked with no successor and no frame; the
// conn must survive past the reap window.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireParked(t *testing.T, cc *cometConn) {
	t.Helper()
	require.Eventually(t, func() bool {
		cc.mu.Lock()
		defer cc.mu.Unlock()
		return cc.waiter != nil
	}, 2*time.Second, 5*time.Millisecond, "a poll should be parked")
}

func TestCometReaperNotArmedWhileParked_B6(t *testing.T) {
	t.Parallel()
	const reap = 200 * time.Millisecond
	cc := newCometConnReap(reap)
	t.Cleanup(func() { _ = cc.close() })

	// Poll A parks.
	aDone := make(chan struct{})
	go func() { cc.parkRecv(context.Background()); close(aDone) }()
	requireParked(t, cc)

	// Poll B supersedes A (A returns empty). B then parks with NO successor
	// and NO frame — the scenario where the buggy stray re-arm (from A's
	// defer) would run the clock and reap B.
	bDone := make(chan struct{})
	go func() { cc.parkRecv(context.Background()); close(bDone) }()
	select {
	case <-aDone:
	case <-time.After(2 * time.Second):
		t.Fatal("superseded poll A never returned")
	}
	requireParked(t, cc) // B is now the parked poll

	// Wait well past the reap window. With the fix the reaper stayed stopped
	// (a poll is parked); with the bug A's defer re-armed it and B is reaped.
	time.Sleep(3 * reap)
	cc.mu.Lock()
	closed := cc.closed
	cc.mu.Unlock()
	require.False(t, closed, "a parked poll preceded by a supersede must never be reaped")

	_ = cc.close()
	<-bDone
}

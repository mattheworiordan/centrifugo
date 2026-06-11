package ably

// A2 (HIGH): the per-channel publish lock must be released BEFORE the
// terminal ACK write. Before the fix, defer unlock() spanned writeAck — a
// stalled transport writer pinned the process-wide per-channel lock for up
// to writeTimeout (5s), stalling every other publisher to that channel.

import (
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// blockingConn is a frameConn whose writeEncoded can be made to block, so
// a test can hold a session inside its ACK write deterministically. The
// session's read loop is never started (publish is driven directly), so
// readFrame is never called.
type blockingConn struct {
	onWrite func() // invoked at the start of each writeEncoded, if set

	mu     sync.Mutex
	frames [][]byte
}

func (c *blockingConn) readFrame() (*protocol.ProtocolMessage, error) {
	return nil, cometClosedError{} // never called: no run loop
}

func (c *blockingConn) writeEncoded(encoded []byte, _ bool) error {
	if c.onWrite != nil {
		c.onWrite()
	}
	c.mu.Lock()
	c.frames = append(c.frames, append([]byte(nil), encoded...))
	c.mu.Unlock()
	return nil
}

func (c *blockingConn) close() error { return nil }

// newBareSession builds a session wired to the test server's shared mint,
// materialized and presence stores, with a UUID-shaped recoverID so
// connectionID() resolves without a live centrifuge client. Enough to
// drive publish() directly.
func newBareSession(ts *realtimeTestServer, conn frameConn, capability auth.Capability) *session {
	return newSession(ts.node, conn, sessionParams{
		capability: capability,
		format:     protocol.FormatJSON,
		recoverID:  "00000000-0000-4000-8000-000000000000",
	}, ts.handler.presence, ts.handler.mint, ts.handler.materialized)
}

func TestPublishLockReleasedBeforeAckWrite_A2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)

	const channel = "a2-lock-test" // ephemeral, non-mutable

	// Session 1: its ACK write blocks until we release the gate. By the time
	// onWrite runs, publish() has already minted + published to the broker;
	// with the fix the channel lock is released, so blocking here holds only
	// the writer, not the lock.
	gate := make(chan struct{})
	blocked := make(chan struct{})
	var blockOnce sync.Once
	slow := &blockingConn{onWrite: func() {
		blockOnce.Do(func() { close(blocked) })
		<-gate
	}}
	s1 := newBareSession(ts, slow, capability)
	go s1.publish(&protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "ev1", Data: "x"}},
	})
	<-blocked // s1 is now stuck inside its ACK write

	// Session 2 publishes to the SAME channel. With the lock released before
	// the write this completes promptly; before the fix it blocks on the
	// channel lock that s1 holds across its stalled write.
	s2done := make(chan struct{})
	fast := &blockingConn{} // returns from writeEncoded immediately
	s2 := newBareSession(ts, fast, capability)
	go func() {
		s2.publish(&protocol.ProtocolMessage{
			Action:    protocol.ActionMessage,
			Channel:   channel,
			MsgSerial: 0,
			Messages:  []*protocol.Message{{Name: "ev2", Data: "y"}},
		})
		close(s2done)
	}()

	select {
	case <-s2done:
		// good: s2 published while s1's writer is still stuck.
	case <-time.After(2 * time.Second):
		close(gate) // unblock s1 so the goroutine can exit
		t.Fatal("second publisher blocked — the channel lock is held across the stalled ACK write")
	}
	close(gate) // release s1

	// Both publications reached the broker in order: the lock still made
	// mint+append atomic (A2 narrows the lock scope without losing T1.2).
	require.Eventually(t, func() bool {
		unlock := ts.handler.mint.lockChannel(channel)
		defer unlock()
		res, herr := ts.node.History(brokerChannel(channel), centrifuge.WithLimit(10))
		return herr == nil && len(res.Publications) == 2
	}, 5*time.Second, 50*time.Millisecond, "both publications retained, serial order == offset order")
}

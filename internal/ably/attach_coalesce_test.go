package ably

// Optimization (companion to D2): a second ATTACH that arrives while the
// channel's first Subscribe is still in flight is COALESCED — attach() queues
// it instead of dispatching a duplicate SubscribeRequest, and the in-flight
// reply serves both. D2 keeps the duplicate harmless (centrifuge code 105
// "already subscribed" is handled as success), but a duplicate still costs a
// wasted broker round-trip on a slow engine (Redis). Coalescing elides it.
//
// CRITICAL: the coalesced ATTACH must be REPLAYED, not dropped. ably-js's
// setOptions({rewind})-driven re-attach races the channel.subscribe()
// auto-attach, so the coalesced ATTACH can be the one carrying rewind —
// dropping it loses the materialized backlog (the updates-deletes "Should
// append to a message over realtime" hang on Redis). See subscribeWaiters.

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	cproto "github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// pendingLen reports the number of in-flight pending ops (test-only).
func (s *session) pendingLen() int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return len(s.pending)
}

// channelFrames decodes the frames written to conn for one channel, counting
// ATTACHED (and how many carried HAS_BACKLOG), ERROR, and collecting MESSAGE
// payloads.
func channelFrames(t *testing.T, conn *blockingConn, channel string) (attached, hasBacklog, errored int, messageData []string) {
	t.Helper()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for _, raw := range conn.frames {
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(raw, protocol.FormatJSON, &m); err != nil {
			continue
		}
		if m.Channel != channel {
			continue
		}
		switch m.Action {
		case protocol.ActionAttached:
			attached++
			if m.Flags&protocol.FlagHasBacklog != 0 {
				hasBacklog++
			}
		case protocol.ActionError:
			errored++
		case protocol.ActionMessage:
			for _, msg := range m.Messages {
				if msg != nil {
					if s, ok := msg.Data.(string); ok {
						messageData = append(messageData, s)
					}
				}
			}
		}
	}
	return attached, hasBacklog, errored, messageData
}

// A coalesced ATTACH carrying rewind=1 on a mutable channel must, when the
// in-flight Subscribe's reply lands, get its OWN ATTACHED (HAS_BACKLOG) and the
// materialized backlog delivered — NOT be dropped. This is the regression that
// pure suppression introduced (the append-over-realtime Redis hang).
func TestCoalescedRewindAttachIsReplayedWithBacklog(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	// A FRESH (non-recovered) connection: rewind is suppressed on a recovered
	// connection (RTL2i, recoverID set), and this exercises the rewind path. The
	// coalesced attach path never calls connectionID(), so the empty recoverID
	// (no live centrifuge client) is safe here.
	s := newSession(ts.node, conn, sessionParams{
		capability: capability,
		format:     protocol.FormatJSON,
	}, ts.handler.presence, ts.handler.mint, ts.handler.materialized)

	const channel = "ai:coalesce_rewind" // mutable namespace

	// Seed materialized state so a rewind=1 has a backlog to serve. The
	// session's materialized store is the test server's shared store.
	s.materialized.create(channel, &protocol.Message{
		Serial: "01000000000000-000@coalescetest:000",
		Name:   "seed",
		Data:   "Hello World",
		Action: protocol.MessageActionCreate,
	})

	// Stand in for the FIRST (bare) ATTACH having dispatched a Subscribe whose
	// reply has not yet landed: a pending opSubscribe plus the claimed
	// in-flight marker. (The bare session has no live centrifuge client;
	// coalescing must NOT touch the client — a nil deref would panic here.)
	id := s.addPending(pendingOp{kind: opSubscribe, channel: channel, modes: protocol.FlagModeSubscribe})
	s.modesMu.Lock()
	s.subscribeWaiters[channel] = nil // mark in-flight, no waiters yet
	s.modesMu.Unlock()

	pendingBefore := s.pendingLen()

	// The SECOND ATTACH (rewind=1) arrives while the first is in flight — the
	// setOptions({rewind}) re-attach. It must coalesce, not dispatch.
	s.attach(&protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Params:  map[string]string{"rewind": "1"},
	})

	// Coalesced: no duplicate Subscribe (no new pending op — and no nil-client
	// panic), no frame yet, and the waiter is queued WITH its rewind intent.
	require.Equal(t, pendingBefore, s.pendingLen(), "a coalesced ATTACH must not register a second opSubscribe")
	attached, _, errored, _ := channelFrames(t, conn, channel)
	require.Zero(t, attached, "a coalesced ATTACH writes no ATTACHED until the in-flight reply lands")
	require.Zero(t, errored)
	s.modesMu.Lock()
	waiters := s.subscribeWaiters[channel]
	s.modesMu.Unlock()
	require.Len(t, waiters, 1, "the coalesced ATTACH is queued as a waiter")
	require.Equal(t, "1", waiters[0].rewindSpec, "the waiter preserves its rewind intent — dropping it caused the Redis hang")

	// The in-flight reply lands (success). The primary gets an ATTACHED (no
	// backlog), and the waiter is replayed: a second ATTACHED with HAS_BACKLOG
	// plus the materialized backlog message.
	s.handleReply(&cproto.Reply{Id: id})

	attached, hasBacklog, errored, msgs := channelFrames(t, conn, channel)
	require.Equal(t, 2, attached, "primary + coalesced waiter each get an ATTACHED")
	require.Zero(t, errored)
	require.Equal(t, 1, hasBacklog, "the rewind waiter's ATTACHED carries HAS_BACKLOG")
	require.Equal(t, []string{"Hello World"}, msgs, "the materialized backlog is delivered to the coalesced attach")

	s.modesMu.Lock()
	_, stillInFlight := s.subscribeWaiters[channel]
	_, attachedRecorded := s.attachedModes[channel]
	s.modesMu.Unlock()
	require.False(t, stillInFlight, "the in-flight marker and waiters are cleared once the reply lands")
	require.True(t, attachedRecorded, "the attachment is recorded")
}

// A plain coalesced ATTACH (no rewind) queues while the first Subscribe is in
// flight and is confirmed by its own ATTACHED when the reply lands — without a
// duplicate SubscribeRequest.
func TestSecondAttachCoalescesWhileSubscribeInFlight(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)

	const channel = "ai:coalesce"
	id := s.addPending(pendingOp{kind: opSubscribe, channel: channel, modes: protocol.FlagModeSubscribe})
	s.modesMu.Lock()
	s.subscribeWaiters[channel] = nil
	s.modesMu.Unlock()
	pendingBefore := s.pendingLen()

	s.attach(&protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: channel})

	require.Equal(t, pendingBefore, s.pendingLen(), "no duplicate SubscribeRequest")
	attached, _, _, _ := channelFrames(t, conn, channel)
	require.Zero(t, attached, "no ATTACHED before the in-flight reply lands")

	s.handleReply(&cproto.Reply{Id: id})

	attached, _, errored, _ := channelFrames(t, conn, channel)
	require.Equal(t, 2, attached, "the single in-flight reply confirms both the primary and the coalesced attach")
	require.Zero(t, errored)
}

// A genuine subscribe failure (not 105) drops the in-flight marker and any
// coalesced waiters, failing the channel for every pending attach via the
// single channel-scoped ERROR.
func TestSubscribeFailureDropsWaiters(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)

	const channel = "ai:failslot"
	id := s.addPending(pendingOp{kind: opSubscribe, channel: channel, modes: protocol.FlagModeSubscribe})
	s.modesMu.Lock()
	s.subscribeWaiters[channel] = nil
	s.modesMu.Unlock()

	// A coalesced waiter is queued behind the in-flight subscribe.
	s.attach(&protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: channel})
	s.modesMu.Lock()
	require.Len(t, s.subscribeWaiters[channel], 1)
	s.modesMu.Unlock()

	// The in-flight subscribe fails (centrifuge permission denied, 103).
	s.handleReply(&cproto.Reply{
		Id:    id,
		Error: &cproto.Error{Code: 103, Message: "permission denied"},
	})

	attached, _, errored, _ := channelFrames(t, conn, channel)
	require.Zero(t, attached)
	require.Equal(t, 1, errored, "a genuine subscribe failure fails the channel with an ERROR")

	s.modesMu.Lock()
	_, stillInFlight := s.subscribeWaiters[channel]
	_, attachedRecorded := s.attachedModes[channel]
	s.modesMu.Unlock()
	require.False(t, stillInFlight, "a failed subscribe drops the in-flight marker and waiters for a retry")
	require.False(t, attachedRecorded, "a failed subscribe records no attachment")
}

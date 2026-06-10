package ably

// Re-ATTACH gap replay (RTL4j territory): an ATTACH presenting a
// channelSerial cursor resumes from that position — the broker replays
// the publications after the cursor atomically with the subscription,
// ATTACHED carries the RESUMED flag (RTL12: its absence signals a
// discontinuity), and the replayed frames arrive in publish order right
// after ATTACHED.

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"

	"github.com/stretchr/testify/require"
)

// publishAndTakeSerial publishes one message and returns the delivered
// frame's channelSerial (the publisher is attached, so the echo arrives).
func publishAndTakeSerial(t *testing.T, conn *websocket.Conn, channel, name string, msgSerial int64) string {
	t.Helper()
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: msgSerial,
		Messages:  []*protocol.Message{{Name: name, Data: "x"}},
	})
	var serial string
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			serial = m.ChannelSerial
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotEmpty(t, serial)
	return serial
}

// RTL4j: re-attaching with the cursor of the last-seen message replays
// exactly the messages published after it, with RESUMED set on ATTACHED.
func TestReattachWithCursorReplaysGap_RTL4j(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// A attaches, sees msg-0, detaches.
	connA := connectRealtime(t, ts)
	writeFrame(t, connA, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "resume-gap-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connA).Action)
	cursor := publishAndTakeSerial(t, connA, "resume-gap-test", "msg-0", 0)
	writeFrame(t, connA, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "resume-gap-test"})
	require.Equal(t, protocol.ActionDetached, readNonHeartbeatFrame(t, connA).Action)

	// B publishes the gap while A is detached.
	connB := connectRealtime(t, ts)
	writeFrame(t, connB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "resume-gap-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connB).Action)
	publishAndTakeSerial(t, connB, "resume-gap-test", "gap-1", 0)
	publishAndTakeSerial(t, connB, "resume-gap-test", "gap-2", 1)

	// A re-attaches from its cursor: RESUMED + the two gap messages in
	// order, then nothing unexpected.
	writeFrame(t, connA, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       "resume-gap-test",
		ChannelSerial: cursor,
	})
	attached := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagResumed, attached.Flags&protocol.FlagResumed, "continuity preserved: RESUMED set")

	first := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionMessage, first.Action)
	require.Equal(t, "gap-1", first.Messages[0].Name)
	second := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionMessage, second.Action)
	require.Equal(t, "gap-2", second.Messages[0].Name)
	require.Less(t, first.ChannelSerial, second.ChannelSerial)
}

// An unresolvable cursor (expired or never minted here) cannot bridge the
// gap: the attach proceeds fresh — no RESUMED flag, no replay — which is
// how the discontinuity is signalled (RTL12).
func TestReattachWithUnresolvableCursorAttachesFresh(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	// Channel has one retained message so history exists but the cursor
	// below never matches it.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "resume-lost-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	publishAndTakeSerial(t, conn, "resume-lost-test", "existing", 0)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "resume-lost-test"})
	require.Equal(t, protocol.ActionDetached, readNonHeartbeatFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       "resume-lost-test",
		ChannelSerial: "00000000000000-000@aaaaaaaaaa",
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Zero(t, attached.Flags&protocol.FlagResumed, "lost continuity: no RESUMED")

	// No replay follows: the next observable frame on this channel is the
	// echo of a fresh publish, not a replayed one.
	serial := publishAndTakeSerial(t, conn, "resume-lost-test", "fresh", 1)
	require.NotEmpty(t, serial)
}

// A cursor at the channel head resumes with an empty gap: RESUMED set,
// nothing replayed.
func TestReattachAtHeadResumesEmpty(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "resume-head-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	cursor := publishAndTakeSerial(t, conn, "resume-head-test", "only", 0)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "resume-head-test"})
	require.Equal(t, protocol.ActionDetached, readNonHeartbeatFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       "resume-head-test",
		ChannelSerial: cursor,
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagResumed, attached.Flags&protocol.FlagResumed)

	// Nothing replayed: the next frame is the echo of a fresh publish.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "resume-head-test",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "after-resume", Data: "x"}},
	})
	var delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.Equal(t, "after-resume", delivered.Messages[0].Name)
}

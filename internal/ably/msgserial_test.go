package ably

// C1: msgSerial validation + count>1 ACK correctness.
//   - Ack-bearing frames (MESSAGE/PRESENCE — the only actions ably-js
//     assigns a msgSerial to) must form a monotonic, contiguous sequence;
//     the first frame seeds the expectation (a resumed connection continues
//     from a nonzero serial), subsequent gaps/regressions/duplicates NACK.
//   - A multi-message frame is ONE PendingMessage and is ACKed with count=1
//     (count is frames-acked, not messages-in-frame — ably-js
//     MessageQueue.completeMessages).

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestMsgSerialMonotonicEnforced_C1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)
	pub := func(serial int64) {
		writeFrame(t, conn, &protocol.ProtocolMessage{
			Action: protocol.ActionMessage, Channel: "c1-serial", MsgSerial: serial,
			Messages: []*protocol.Message{{Name: "ev", Data: "x"}},
		})
	}
	expectAck := func() {
		m := readNonHeartbeatFrame(t, conn)
		require.Equal(t, protocol.ActionAck, m.Action, "expected ACK for a contiguous serial")
	}
	expectNack := func() {
		m := readNonHeartbeatFrame(t, conn)
		require.Equal(t, protocol.ActionNack, m.Action, "expected NACK for a bad serial")
		require.NotNil(t, m.Error)
		require.Equal(t, errCodeBadRequest, m.Error.Code)
	}

	pub(0)
	expectAck() // seed → expected 1
	pub(1)
	expectAck() // contiguous → expected 2
	pub(1)
	expectNack() // regressing/duplicate
	pub(5)
	expectNack() // gapped
	pub(2)
	expectAck() // back on the expected serial (rejects didn't advance it)
}

// The first ack-bearing frame seeds from whatever serial it carries — a
// resumed connection continues its sequence from a nonzero value (RTN15c),
// and the adapter cannot distinguish a real resume from a claim, so it must
// not reject the first frame.
func TestMsgSerialSeedsFromNonzero_C1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)
	pub := func(serial int64) {
		writeFrame(t, conn, &protocol.ProtocolMessage{
			Action: protocol.ActionMessage, Channel: "c1-seed", MsgSerial: serial,
			Messages: []*protocol.Message{{Name: "ev", Data: "x"}},
		})
	}
	pub(7)
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action, "nonzero first serial seeds, not rejected")
	pub(8)
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action, "contiguous after the seed")
	pub(10)
	require.Equal(t, protocol.ActionNack, readNonHeartbeatFrame(t, conn).Action, "gap after the seed is rejected")
}

// A single frame carrying multiple messages is ONE PendingMessage and is
// ACKed with count=1 (NOT len(messages)); all messages are still delivered.
func TestMultiMessageFrameAcksCountOne_C1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "c1-multi"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "c1-multi", MsgSerial: 0,
		Messages: []*protocol.Message{{Name: "a", Data: "1"}, {Name: "b", Data: "2"}},
	})

	acks, delivered := 0, 0
	for acks < 1 || delivered < 2 {
		m := readNonHeartbeatFrame(t, conn)
		switch m.Action {
		case protocol.ActionAck:
			require.Equal(t, 1, m.Count, "a multi-message frame is one PendingMessage → count=1")
			acks++
		case protocol.ActionMessage:
			delivered += len(m.Messages)
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.Equal(t, 1, acks)
	require.Equal(t, 2, delivered)
}

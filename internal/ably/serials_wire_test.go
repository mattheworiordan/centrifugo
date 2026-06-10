package ably

// channelSerial wire behavior (RTL15): every publication carries a
// per-channel monotonic serial — MESSAGE/PRESENCE frames stamp it as
// ProtocolMessage.channelSerial (RTL15b, the client's resume cursor) and
// ATTACHED advertises the latest retained serial as the attach point
// (RTL15a). REST and realtime publishes on one channel draw from the
// same sequence, so lexicographic serial order matches publish order.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// RTL15b: delivered MESSAGE frames carry strictly-increasing
// channelSerials, and each envelope's Message.Serial is the frame serial
// plus the in-batch index suffix ":000".
func TestMessageFramesCarryChannelSerial_RTL15b(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-msg-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	var serials []string
	for i := range 2 {
		writeFrame(t, conn, &protocol.ProtocolMessage{
			Action:    protocol.ActionMessage,
			Channel:   "serial-msg-test",
			MsgSerial: int64(i),
			Messages:  []*protocol.Message{{Name: "ev", Data: "x"}},
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
		require.NotNil(t, delivered)
		require.NotEmpty(t, delivered.ChannelSerial, "MESSAGE frame must carry channelSerial")
		require.Len(t, delivered.Messages, 1)
		require.Equal(t, delivered.ChannelSerial+":000", delivered.Messages[0].Serial,
			"Message.Serial is the frame channelSerial plus the in-batch idx")
		serials = append(serials, delivered.ChannelSerial)
	}
	require.Less(t, serials[0], serials[1], "serials are lexicographically increasing in publish order")
}

// RTL15b: presence events advance the same per-channel sequence —
// PRESENCE frames carry channelSerials interleaved in order with
// messages.
func TestPresenceFramesCarryChannelSerial_RTL15b(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-presence-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "serial-presence-test",
		MsgSerial: 0,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "c"}},
	})
	var presenceSerial string
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionPresence:
			presenceSerial = m.ChannelSerial
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotEmpty(t, presenceSerial, "PRESENCE frame must carry channelSerial")

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "serial-presence-test",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "after", Data: "x"}},
	})
	var messageSerial string
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			messageSerial = m.ChannelSerial
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.Less(t, presenceSerial, messageSerial, "presence and messages share one per-channel sequence")
}

// RTL15a: a fresh channel attaches without a channelSerial; once the
// channel has retained publications, ATTACHED advertises the latest
// serial as the attach point. The re-attach (options-update) path
// reports it too.
func TestAttachedCarriesLatestChannelSerial_RTL15a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-attach-test"})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Empty(t, attached.ChannelSerial, "no publications yet: no attach point")

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "serial-attach-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "ev", Data: "x"}},
	})
	var lastSerial string
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			lastSerial = m.ChannelSerial
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotEmpty(t, lastSerial)

	// Re-attach (same session, options-update path): the attach point is
	// the serial of the message just published.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-attach-test"})
	reattached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, reattached.Action)
	require.Equal(t, lastSerial, reattached.ChannelSerial)

	// Fresh-subscribe path (second connection): same attach point.
	conn2 := connectRealtime(t, ts)
	writeFrame(t, conn2, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-attach-test"})
	attached2 := readNonHeartbeatFrame(t, conn2)
	require.Equal(t, protocol.ActionAttached, attached2.Action)
	require.Equal(t, lastSerial, attached2.ChannelSerial)
}

// REST and realtime publishes on one channel draw from the same
// per-channel generator: serials stay strictly increasing across
// surfaces, and REST history envelopes expose Message.Serial.
func TestRESTAndRealtimeShareSerialSequence(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// REST publish first.
	resp := restRequest(t, ts, http.MethodPost, "/channels/serial-cross-test/messages",
		[]byte(`{"name":"rest-ev","data":"r"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Realtime publish second.
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "serial-cross-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "serial-cross-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "rt-ev", Data: "x"}},
	})
	var rtSerial string
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			rtSerial = m.ChannelSerial
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotEmpty(t, rtSerial)

	// History (backwards: newest first) shows both messages with serials,
	// realtime's strictly above REST's.
	history := restRequest(t, ts, http.MethodGet, "/channels/serial-cross-test/messages", nil, nil)
	require.Equal(t, http.StatusOK, history.StatusCode)
	items := decodeMessagesBody(t, history)
	require.Len(t, items, 2)
	newestSerial := items[0].Serial
	oldestSerial := items[1].Serial
	require.True(t, strings.HasPrefix(newestSerial, rtSerial+":"), "newest history item is the realtime publish")
	require.NotEmpty(t, oldestSerial)
	require.Less(t, oldestSerial, newestSerial, "REST serial sorts strictly below the later realtime serial")
}

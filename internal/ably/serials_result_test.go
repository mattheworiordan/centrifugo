package ably

// PublishResult serials on mutableMessages channels: the realtime ACK
// carries res[0].serials (TR4s) and the REST publish response body
// carries {serials} (RSL1n) — 1:1 in publish order with the frame's
// messages, matching the envelope Message.Serial values, and absent on
// non-mutable channels.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"

	"github.com/stretchr/testify/require"
)

// ackResFrame decodes the wire ACK including the res attribute (the
// bespoke ackFrame is encode-only).
type ackResFrame struct {
	Action    protocol.Action `json:"action"`
	MsgSerial int64           `json:"msgSerial"`
	Count     int             `json:"count"`
	Res       []struct {
		Serials []string `json:"serials"`
	} `json:"res"`
}

// readRawNonHeartbeat reads raw frames (JSON sessions) until one that is
// not a heartbeat — raw because ackFrame's res attribute is not part of
// the shared ProtocolMessage decode shape.
func readRawNonHeartbeat(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, raw, err := conn.ReadMessage()
		require.NoError(t, err)
		var probe struct {
			Action protocol.Action `json:"action"`
		}
		require.NoError(t, json.Unmarshal(raw, &probe))
		if probe.Action != protocol.ActionHeartbeat {
			return raw
		}
	}
}

func readAckRes(t *testing.T, raw []byte) ackResFrame {
	t.Helper()
	var f ackResFrame
	require.NoError(t, json.Unmarshal(raw, &f))
	return f
}

// TR4s: a mutable-channel publish ACK carries one PublishResult whose
// serials match the delivered messages' envelope serials, in order.
func TestMutablePublishAckCarriesSerials_TR4s(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "mutable:ack-serials"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:ack-serials",
		MsgSerial: 0,
		Messages: []*protocol.Message{
			{Name: "m0", Data: "a"},
			{Name: "m1", Data: "b"},
		},
	})
	var ack ackResFrame
	var delivered []*protocol.Message
	for len(delivered) < 2 || ack.Count == 0 {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		switch probe.Action {
		case protocol.ActionAck:
			ack = readAckRes(t, raw)
		case protocol.ActionMessage:
			delivered = append(delivered, probe.Messages...)
		default:
			t.Fatalf("unexpected frame action %d", probe.Action)
		}
	}
	require.Len(t, ack.Res, 1, "one PublishResult per acknowledged frame")
	require.Len(t, ack.Res[0].Serials, 2, "serials 1:1 with messages")
	require.Equal(t, delivered[0].Serial, ack.Res[0].Serials[0])
	require.Equal(t, delivered[1].Serial, ack.Res[0].Serials[1])
	require.Less(t, ack.Res[0].Serials[0], ack.Res[0].Serials[1], "lexicographically ordered")
}

// Non-mutable channels keep the bare ACK: no res attribute.
func TestNonMutablePublishAckOmitsRes(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "plain-ack"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "plain-ack",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "m", Data: "x"}},
	})
	for {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		if probe.Action == protocol.ActionMessage {
			continue
		}
		require.Equal(t, protocol.ActionAck, probe.Action)
		require.NotContains(t, string(raw), `"res"`)
		return
	}
}

// RSL1n: the REST publish response body carries the assigned message
// serials on mutable channels and stays empty elsewhere.
func TestRESTPublishSerials_RSL1n(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	resp := restRequest(t, ts, http.MethodPost, "/channels/mutable:rest-serials/messages",
		[]byte(`[{"name":"a","data":"1"},{"name":"b","data":"2"}]`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var body struct {
		Serials []string `json:"serials"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Len(t, body.Serials, 2)
	require.Less(t, body.Serials[0], body.Serials[1])

	plain := restRequest(t, ts, http.MethodPost, "/channels/plain-rest-serials/messages",
		[]byte(`{"name":"a","data":"1"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, plain.StatusCode)
	var plainBody map[string]any
	require.NoError(t, json.NewDecoder(plain.Body).Decode(&plainBody))
	require.NotContains(t, plainBody, "serials")
}

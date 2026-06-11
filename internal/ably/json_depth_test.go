package ably

// C2: inbound message data/extras are bounded in nesting depth, so a
// deeply-nested "nesting bomb" within a small body is rejected (40000)
// rather than decoded into `any`. Covers the publish core (realtime / REST /
// comet) and the mutation paths.

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// nestedData builds a Go value nested `depth` map levels deep.
func nestedData(depth int) any {
	var v any = "leaf"
	for i := 0; i < depth; i++ {
		v = map[string]any{"a": v}
	}
	return v
}

// nestedJSON builds the JSON text {"a":{"a":...}} nested `depth` levels.
func nestedJSON(depth int) string {
	return strings.Repeat(`{"a":`, depth) + `"leaf"` + strings.Repeat(`}`, depth)
}

func TestRealtimePublishNestingBombRejected_C2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "c2-rt", MsgSerial: 0,
		Messages: []*protocol.Message{{Name: "deep", Data: nestedData(maxJSONDepth + 10)}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeBadRequest, nack.Error.Code)
	require.Contains(t, nack.Error.Message, "nesting")

	// A shallow payload on the same connection still publishes.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "c2-rt", MsgSerial: 1,
		Messages: []*protocol.Message{{Name: "ok", Data: nestedData(3)}},
	})
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action)
}

func TestRESTPublishNestingBombRejected_C2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	body := `{"name":"deep","data":` + nestedJSON(maxJSONDepth+10) + `}`
	resp := restRequest(t, ts, http.MethodPost, "/channels/c2-rest/messages",
		[]byte(body), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

func TestCometSendNestingBombRejected_C2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)
	body := `[{"action":15,"channel":"c2-comet","msgSerial":0,"messages":[{"name":"deep","data":` +
		nestedJSON(maxJSONDepth+10) + `}]}]`
	resp := cometSend(t, ts, key, body)
	require.Equal(t, http.StatusNoContent, resp.StatusCode) // ACK/NACK rides recv
	nack := recvUntil(t, ts, key, 5*time.Second, func(m *protocol.ProtocolMessage) bool {
		return m.Action == protocol.ActionNack
	})
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeBadRequest, nack.Error.Code)
}

// The mutation path is guarded too: the depth check fires before the
// not-found lookup, so a made-up serial still surfaces the depth rejection.
func TestRealtimeMutationNestingBombRejected_C2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "ai:c2-mut", MsgSerial: 0,
		Messages: []*protocol.Message{{
			Serial: "made-up-serial", Action: protocol.MessageActionAppend, Data: nestedData(maxJSONDepth + 10),
		}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeBadRequest, nack.Error.Code)
	require.Contains(t, nack.Error.Message, "nesting")
}

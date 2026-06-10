package ably

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// decodeMsgpackMap decodes raw msgpack bytes into a generic map so tests
// can assert raw wire shape — field presence, not just decoded values.
func decodeMsgpackMap(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var fields map[string]any
	require.NoError(t, msgpack.Unmarshal(data, &fields))
	return fields
}

// TR4j/RTN7b over msgpack: the ackFrame msgpack encoding must emit
// msgSerial and count even at their zero values — the msgpack mirror of
// the JSON wire-shape guarantee (ably-js correlates pending publishes by
// the literal field; an omitted msgSerial never settles the publish).
// This pins vmihailenco's emit-zero-valued-tagged-fields behavior.
func TestAckFrameMsgpackWireShape(t *testing.T) {
	t.Parallel()
	data, err := protocol.MarshalAny(&ackFrame{
		Action:    protocol.ActionAck,
		MsgSerial: 0,
		Count:     1,
	}, protocol.FormatMsgpack)
	require.NoError(t, err)

	fields := decodeMsgpackMap(t, data)
	require.Contains(t, fields, "msgSerial", "ACK must carry msgSerial even when 0")
	require.EqualValues(t, 0, fields["msgSerial"])
	require.Contains(t, fields, "count")
	require.EqualValues(t, 1, fields["count"])
	require.NotContains(t, fields, "error", "error stays omitempty on ACK")

	// Interop: the shared ProtocolMessage decoder reads the frame back.
	var m protocol.ProtocolMessage
	require.NoError(t, protocol.Unmarshal(data, protocol.FormatMsgpack, &m))
	require.Equal(t, protocol.ActionAck, m.Action)
	require.Equal(t, int64(0), m.MsgSerial)
	require.Equal(t, 1, m.Count)
}

// RTL14 over msgpack: the channelErrorFrame msgpack encoding must emit
// the channel attribute even for the empty channel name "" — ably-js
// routes an ERROR to a channel only when the attribute is present
// (pinned in JSON by channelattachempty; this is the msgpack mirror).
func TestChannelErrorFrameMsgpackWireShape(t *testing.T) {
	t.Parallel()
	data, err := protocol.MarshalAny(&channelErrorFrame{
		Action:  protocol.ActionError,
		Channel: "",
		Error:   &protocol.ErrorInfo{Code: errCodeInvalidChannelName, StatusCode: 400, Message: "invalid channel name"},
	}, protocol.FormatMsgpack)
	require.NoError(t, err)

	fields := decodeMsgpackMap(t, data)
	require.Contains(t, fields, "channel", "channel-scoped ERROR must carry the channel attribute even when empty")
	require.EqualValues(t, "", fields["channel"])
	require.Contains(t, fields, "error")

	var m protocol.ProtocolMessage
	require.NoError(t, protocol.Unmarshal(data, protocol.FormatMsgpack, &m))
	require.Equal(t, protocol.ActionError, m.Action)
	require.NotNil(t, m.Error)
	require.Equal(t, errCodeInvalidChannelName, m.Error.Code)
}

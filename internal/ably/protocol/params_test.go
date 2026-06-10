package protocol

// Wire tolerance for channel params: SDKs send scalar param values
// (ably-js resume_rewind_1 sends {rewind: 1} as a JSON number / msgpack
// integer); decoding coerces them to strings instead of rejecting the
// whole frame.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestParamsScalarToleranceJSON(t *testing.T) {
	var m ProtocolMessage
	data := []byte(`{"action":10,"channel":"c","params":{"rewind":1,"delta":"vcdiff","flag":true,"ratio":1.5}}`)
	require.NoError(t, Unmarshal(data, FormatJSON, &m))
	require.Equal(t, ParamsMap{"rewind": "1", "delta": "vcdiff", "flag": "true", "ratio": "1.5"}, m.Params)
}

func TestParamsScalarToleranceMsgpack(t *testing.T) {
	raw, err := msgpack.Marshal(map[string]any{
		"action":  10,
		"channel": "c",
		"params":  map[string]any{"rewind": 1, "delta": "vcdiff", "flag": true},
	})
	require.NoError(t, err)
	var m ProtocolMessage
	require.NoError(t, Unmarshal(raw, FormatMsgpack, &m))
	require.Equal(t, ParamsMap{"rewind": "1", "delta": "vcdiff", "flag": "true"}, m.Params)
}

// The canonical all-strings form keeps decoding unchanged, and the
// encode side emits a plain string map that round-trips.
func TestParamsCanonicalRoundTrip(t *testing.T) {
	for _, format := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(format.String(), func(t *testing.T) {
			frame := &ProtocolMessage{Action: ActionAttached, Channel: "c", Params: ParamsMap{"modes": "subscribe"}}
			raw, err := Marshal(frame, format)
			require.NoError(t, err)
			var decoded ProtocolMessage
			require.NoError(t, Unmarshal(raw, format, &decoded))
			require.Equal(t, frame.Params, decoded.Params)
		})
	}
}

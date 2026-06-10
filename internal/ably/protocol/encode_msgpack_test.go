package protocol

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// decodeMpMap round-trips an encoded value into a generic map so tests
// can assert key presence/absence on the actual wire shape.
func decodeMpMap(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := MarshalAny(v, FormatMsgpack)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, msgpack.Unmarshal(raw, &out))
	return out
}

// The msgpack codec's omitempty would drop "", 0, false and empty
// collections held in the `any` Data field (it unwraps the interface);
// the custom encoder must keep every non-nil payload. Pinned by ably-js
// publishVariations (empty-string data must arrive as "", not absent).
func TestMsgpackKeepsEmptyValuedData(t *testing.T) {
	for name, data := range map[string]any{
		"empty string": "",
		"zero int":     int64(0),
		"zero float":   float64(0),
		"false":        false,
		"empty array":  []any{},
		"empty map":    map[string]any{},
	} {
		t.Run("message "+name, func(t *testing.T) {
			got := decodeMpMap(t, &Message{Name: "n", Data: data})
			_, present := got["data"]
			require.True(t, present, "data %#v must survive the msgpack wire", data)
		})
		t.Run("presence "+name, func(t *testing.T) {
			got := decodeMpMap(t, &PresenceMessage{Action: PresenceEnter, ClientID: "c", Data: data})
			_, present := got["data"]
			require.True(t, present, "data %#v must survive the msgpack wire", data)
		})
	}
}

// Absent payloads stay omitted: nil Data/Extras emit no key at all
// (real Ably omits unset attributes; data:null would decode as null
// rather than undefined in SDKs).
func TestMsgpackOmitsNilData(t *testing.T) {
	got := decodeMpMap(t, &Message{Name: "n"})
	_, dataPresent := got["data"]
	require.False(t, dataPresent)
	_, extrasPresent := got["extras"]
	require.False(t, extrasPresent)

	gotPresence := decodeMpMap(t, &PresenceMessage{Action: PresenceLeave, ClientID: "c"})
	_, dataPresent = gotPresence["data"]
	require.False(t, dataPresent)
	// Action is always emitted, even as 0 (PresenceAbsent is meaningful).
	gotAbsent := decodeMpMap(t, &PresenceMessage{Action: PresenceAbsent, ClientID: "c"})
	require.Contains(t, gotAbsent, "action")
}

// Empty-string data must survive the wire in BOTH formats and round-trip
// back to "" — the full publish-shaped frame, not just the bare struct,
// so the encoder is exercised through the []*Message path sessions use.
func TestEmptyStringDataRoundTripBothFormats(t *testing.T) {
	for _, format := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(format.String(), func(t *testing.T) {
			frame := &ProtocolMessage{
				Action:   ActionMessage,
				Channel:  "ch",
				Messages: []*Message{{Name: "n", Data: ""}},
			}
			raw, err := Marshal(frame, format)
			require.NoError(t, err)
			var decoded ProtocolMessage
			require.NoError(t, Unmarshal(raw, format, &decoded))
			require.Len(t, decoded.Messages, 1)
			require.Equal(t, "", decoded.Messages[0].Data)
		})
	}
}

// Field-set guard: the custom encoders hand-list their struct's fields,
// so adding a field to Message/PresenceMessage without updating the
// encoder would silently drop it from the msgpack wire. This pins the
// msgpack tag set each encoder covers — extend BOTH the struct's encoder
// and this list together.
func TestMsgpackEncodersCoverEveryField(t *testing.T) {
	covered := map[string][]string{
		"Message": {
			"id", "serial", "clientId", "connectionId", "connectionKey",
			"name", "data", "encoding", "extras", "timestamp",
			"action", "version",
		},
		"PresenceMessage": {
			"id", "action", "clientId", "connectionId",
			"data", "encoding", "extras", "timestamp",
		},
	}
	for name, typ := range map[string]reflect.Type{
		"Message":         reflect.TypeOf(Message{}),
		"PresenceMessage": reflect.TypeOf(PresenceMessage{}),
	} {
		t.Run(name, func(t *testing.T) {
			var tags []string
			for i := 0; i < typ.NumField(); i++ {
				tag := typ.Field(i).Tag.Get("msgpack")
				tags = append(tags, strings.Split(tag, ",")[0])
			}
			require.Equal(t, covered[name], tags,
				"%s gained/lost a field — update its EncodeMsgpack and this guard together", name)
		})
	}

	// Self-verifying half: a fully-populated value must emit every pinned
	// key, proving the encoder actually covers the list above (not just
	// that the list matches the struct).
	fullMessage := decodeMpMap(t, &Message{
		ID: "i", Serial: "s", ClientID: "c", ConnectionID: "co",
		ConnectionKey: "k", Name: "n", Data: "d", Encoding: "e",
		Extras: map[string]any{"x": 1}, Timestamp: 1,
		Action: MessageActionUpdate, Version: &MessageVersion{Serial: "v"},
	})
	require.ElementsMatch(t, covered["Message"], mapKeys(fullMessage))
	fullPresence := decodeMpMap(t, &PresenceMessage{
		ID: "i", Action: PresenceEnter, ClientID: "c", ConnectionID: "co",
		Data: "d", Encoding: "e", Extras: map[string]any{"x": 1}, Timestamp: 1,
	})
	require.ElementsMatch(t, covered["PresenceMessage"], mapKeys(fullPresence))
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

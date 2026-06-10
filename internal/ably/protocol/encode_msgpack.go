package protocol

// Custom msgpack encoders fixing vmihailenco/msgpack's omitempty
// semantics for the `any`-typed payload fields. The codec unwraps an
// interface and treats "", 0, false and empty collections as empty, so
// `data:""` (and friends) silently vanish from the msgpack wire —
// encoding/json only nil-checks interfaces, so the JSON wire keeps them.
// Pinned by ably-js publishVariations: an empty-string publish must
// arrive as "" (the SDK otherwise sees undefined). Data/Extras are
// emitted whenever non-nil; absent (nil) payloads stay omitted, matching
// real Ably, which omits the data attribute entirely when unset.
//
// The encoders mirror the struct tags field-for-field;
// TestMsgpackEncodersCoverEveryField pins the field sets so adding a
// struct field without updating its encoder fails the build's tests.

import (
	"github.com/vmihailenco/msgpack/v5"
)

type mpField struct {
	name string
	v    any
}

func encodeMpFields(enc *msgpack.Encoder, fields []mpField) error {
	if err := enc.EncodeMapLen(len(fields)); err != nil {
		return err
	}
	for _, f := range fields {
		if err := enc.EncodeString(f.name); err != nil {
			return err
		}
		if err := enc.Encode(f.v); err != nil {
			return err
		}
	}
	return nil
}

var _ msgpack.CustomEncoder = (*Message)(nil)

// EncodeMsgpack implements msgpack.CustomEncoder.
func (m *Message) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := make([]mpField, 0, 10)
	if m.ID != "" {
		fields = append(fields, mpField{"id", m.ID})
	}
	if m.Serial != "" {
		fields = append(fields, mpField{"serial", m.Serial})
	}
	if m.ClientID != "" {
		fields = append(fields, mpField{"clientId", m.ClientID})
	}
	if m.ConnectionID != "" {
		fields = append(fields, mpField{"connectionId", m.ConnectionID})
	}
	if m.ConnectionKey != "" {
		fields = append(fields, mpField{"connectionKey", m.ConnectionKey})
	}
	if m.Name != "" {
		fields = append(fields, mpField{"name", m.Name})
	}
	if m.Data != nil {
		fields = append(fields, mpField{"data", m.Data})
	}
	if m.Encoding != "" {
		fields = append(fields, mpField{"encoding", m.Encoding})
	}
	if m.Extras != nil {
		fields = append(fields, mpField{"extras", m.Extras})
	}
	if m.Timestamp != 0 {
		fields = append(fields, mpField{"timestamp", m.Timestamp})
	}
	if m.Action != 0 {
		fields = append(fields, mpField{"action", m.Action})
	}
	if m.Version != nil {
		fields = append(fields, mpField{"version", m.Version})
	}
	return encodeMpFields(enc, fields)
}

var _ msgpack.CustomEncoder = (*PresenceMessage)(nil)

// EncodeMsgpack implements msgpack.CustomEncoder. Action is always
// emitted (its tag carries no omitempty: action 0 is a meaningful
// PresenceAbsent, TP2).
func (m *PresenceMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := make([]mpField, 0, 8)
	if m.ID != "" {
		fields = append(fields, mpField{"id", m.ID})
	}
	fields = append(fields, mpField{"action", int64(m.Action)})
	if m.ClientID != "" {
		fields = append(fields, mpField{"clientId", m.ClientID})
	}
	if m.ConnectionID != "" {
		fields = append(fields, mpField{"connectionId", m.ConnectionID})
	}
	if m.Data != nil {
		fields = append(fields, mpField{"data", m.Data})
	}
	if m.Encoding != "" {
		fields = append(fields, mpField{"encoding", m.Encoding})
	}
	if m.Extras != nil {
		fields = append(fields, mpField{"extras", m.Extras})
	}
	if m.Timestamp != 0 {
		fields = append(fields, mpField{"timestamp", m.Timestamp})
	}
	return encodeMpFields(enc, fields)
}

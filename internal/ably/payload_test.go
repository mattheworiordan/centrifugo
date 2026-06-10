package ably

import (
	"encoding/base64"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/stretchr/testify/require"
)

// RSL4d1: binary data normalizes to the canonical JSON-safe form — a
// Base64 string with "base64" appended to the encoding chain (right-to-
// left order per RSL4b: the transport segment is rightmost).
func TestNormalizeMessageData_RSL4d1(t *testing.T) {
	t.Parallel()
	raw := []byte{0x00, 0x01, 0xfe, 0xff}
	b64 := base64.StdEncoding.EncodeToString(raw)

	t.Run("binary_no_encoding", func(t *testing.T) {
		msg := &protocol.Message{Data: raw}
		normalizeMessageData(msg)
		require.Equal(t, b64, msg.Data)
		require.Equal(t, "base64", msg.Encoding)
	})

	t.Run("binary_appends_to_chain", func(t *testing.T) {
		msg := &protocol.Message{Data: raw, Encoding: "utf-8/cipher+aes-128-cbc"}
		normalizeMessageData(msg)
		require.Equal(t, b64, msg.Data)
		require.Equal(t, "utf-8/cipher+aes-128-cbc/base64", msg.Encoding)
	})

	// Strict passthrough: non-binary data and its encoding chain are
	// never touched, whatever the chain says.
	for name, msg := range map[string]*protocol.Message{
		"string_data":        {Data: "hello", Encoding: ""},
		"json_encoding":      {Data: `{"a":1}`, Encoding: "json"},
		"cipher_chain":       {Data: b64, Encoding: "utf-8/cipher+aes-128-cbc/base64"},
		"vcdiff_chain":       {Data: b64, Encoding: "vcdiff/base64"},
		"nil_data":           {Data: nil, Encoding: ""},
		"numeric_data":       {Data: int64(42), Encoding: ""},
		"base64_string_data": {Data: b64, Encoding: "base64"},
	} {
		t.Run("passthrough_"+name, func(t *testing.T) {
			want := *msg
			normalizeMessageData(msg)
			require.Equal(t, want, *msg)
		})
	}
}

// RSL4c1 (inverted RSL4d1): delivery to a msgpack session pops a trailing
// transport "base64" segment and restores raw bytes; chains not ending in
// "base64" — and every inner segment — survive verbatim.
func TestDenormalizeMessageData_RSL4c1(t *testing.T) {
	t.Parallel()
	raw := []byte{0x00, 0x01, 0xfe, 0xff}
	b64 := base64.StdEncoding.EncodeToString(raw)

	t.Run("pops_lone_base64", func(t *testing.T) {
		msg := &protocol.Message{Data: b64, Encoding: "base64"}
		denormalizeMessageData(msg)
		require.Equal(t, raw, msg.Data)
		require.Equal(t, "", msg.Encoding)
	})

	t.Run("pops_trailing_base64_keeps_chain", func(t *testing.T) {
		msg := &protocol.Message{Data: b64, Encoding: "utf-8/cipher+aes-128-cbc/base64"}
		denormalizeMessageData(msg)
		require.Equal(t, raw, msg.Data)
		require.Equal(t, "utf-8/cipher+aes-128-cbc", msg.Encoding)
	})

	// Strict passthrough: no trailing "base64" segment, non-string data,
	// or undecodable Base64 — the message is delivered unchanged (RSL6b
	// territory: never drop, never corrupt; canonical form is still
	// client-decodable).
	for name, msg := range map[string]*protocol.Message{
		"no_encoding":          {Data: "hello", Encoding: ""},
		"json_encoding":        {Data: `{"a":1}`, Encoding: "json"},
		"utf8_only":            {Data: "hello", Encoding: "utf-8"},
		"base64_not_last":      {Data: "hello", Encoding: "base64/json"},
		"base64_infix_name":    {Data: "hello", Encoding: "notbase64"},
		"non_string_data":      {Data: int64(7), Encoding: "base64"},
		"invalid_base64":       {Data: "%%% not base64 %%%", Encoding: "base64"},
		"invalid_base64_chain": {Data: "%%%", Encoding: "utf-8/base64"},
	} {
		t.Run("passthrough_"+name, func(t *testing.T) {
			want := *msg
			denormalizeMessageData(msg)
			require.Equal(t, want, *msg)
		})
	}
}

// Round-trip property: normalize (inbound msgpack → canonical) then
// denormalize (canonical → msgpack delivery) restores the original
// payload and chain exactly.
func TestPayloadNormalizationRoundTrip(t *testing.T) {
	t.Parallel()
	for name, original := range map[string]*protocol.Message{
		"plain_binary":   {Data: []byte{0xde, 0xad, 0xbe, 0xef}},
		"binary_w_chain": {Data: []byte{0x01, 0x02}, Encoding: "utf-8/cipher+aes-128-cbc"},
		"empty_binary":   {Data: []byte{}},
	} {
		t.Run(name, func(t *testing.T) {
			msg := &protocol.Message{Data: append([]byte(nil), original.Data.([]byte)...), Encoding: original.Encoding}
			normalizeMessageData(msg)
			_, isString := msg.Data.(string)
			require.True(t, isString, "canonical form must be JSON-safe")
			denormalizeMessageData(msg)
			require.Equal(t, original.Data, msg.Data)
			require.Equal(t, original.Encoding, msg.Encoding)
		})
	}
}

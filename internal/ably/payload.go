package ably

// Binary-payload normalization between the two wire formats (RTN2a:
// json and msgpack).
//
// The canonical stored form of every published Message is the JSON-safe
// envelope: binary data is carried as a Base64 string with "base64"
// appended to the encoding chain — exactly what a JSON-protocol SDK puts
// on the wire (RSL4d1). The publishing session normalizes inbound
// payloads to this form BEFORE node.Publish, so fan-out, history and
// REST stay format-agnostic. On delivery to a msgpack session the
// transport-level "base64" segment is popped and the data restored to
// raw bytes (the msgpack binary type, RSL4c1), matching what the SDK
// would have received from the real service.
//
// Everything else passes through verbatim. The encoding chain is
// right-to-left (RSL4b): segments other than a trailing transport
// "base64" — "utf-8", "json" (RSL4c3/RSL4d3), "cipher+aes-128-cbc"
// (RSL5), "vcdiff" — belong to the client (RSL6a decodes them
// client-side; crypto in particular depends on the chain surviving
// untouched), so the adapter never inspects or rewrites them.

import (
	"encoding/base64"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

// encodingBase64 is the transport encoding segment appended when a binary
// payload is represented as a Base64 JSON string (RSL4d1).
const encodingBase64 = "base64"

// normalizeMessageData converts an inbound Message to the canonical
// JSON-safe form: binary data ([]byte, decoded from the msgpack binary
// type — a JSON decode never yields []byte) becomes a Base64 string with
// "base64" appended to the encoding chain (RSL4d1). Any other data type,
// and the rest of the encoding chain, pass through untouched.
func normalizeMessageData(msg *protocol.Message) {
	data, ok := msg.Data.([]byte)
	if !ok {
		return
	}
	msg.Data = base64.StdEncoding.EncodeToString(data)
	if msg.Encoding == "" {
		msg.Encoding = encodingBase64
	} else {
		msg.Encoding += "/" + encodingBase64
	}
}

// denormalizeMessageData prepares a canonical-form Message for delivery
// to a msgpack session: a trailing transport "base64" segment is popped
// and the data restored to raw bytes, which the codec encodes as the
// msgpack binary type (RSL4c1). Chains not ending in "base64" pass
// through verbatim — any remaining segments are the client's to decode
// (RSL6a). A payload that fails to decode is delivered unchanged: SDKs
// process a "base64" segment client-side too, so canonical form is
// still decodable (RSL6b territory — never drop, never corrupt).
func denormalizeMessageData(msg *protocol.Message) {
	var remainder string
	switch {
	case msg.Encoding == encodingBase64:
		remainder = ""
	case strings.HasSuffix(msg.Encoding, "/"+encodingBase64):
		remainder = strings.TrimSuffix(msg.Encoding, "/"+encodingBase64)
	default:
		return
	}
	str, ok := msg.Data.(string)
	if !ok {
		return
	}
	raw, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return
	}
	msg.Data = raw
	msg.Encoding = remainder
}

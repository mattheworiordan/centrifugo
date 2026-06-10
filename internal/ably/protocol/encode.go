package protocol

// Derived from github.com/ably/server internal/protocol (Apache-2.0).

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Format is the wire encoding of a ProtocolMessage frame.
type Format int

const (
	FormatJSON Format = iota
	FormatMsgpack
)

// FormatFromQuery returns the format selected by the `format` query
// parameter on a WebSocket upgrade request. Empty defaults to JSON.
func FormatFromQuery(v string) (Format, error) {
	switch v {
	case "", "json":
		return FormatJSON, nil
	case "msgpack":
		return FormatMsgpack, nil
	default:
		return 0, fmt.Errorf("unsupported format %q", v)
	}
}

// String returns the format name for diagnostics.
func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatMsgpack:
		return "msgpack"
	default:
		return "unknown"
	}
}

// Marshal encodes a ProtocolMessage in the given format.
func Marshal(m *ProtocolMessage, f Format) ([]byte, error) {
	return MarshalAny(m, f)
}

// MarshalAny encodes an arbitrary wire value in the given format. It exists
// for the adapter's bespoke wire shapes (ACK/NACK and channel-scoped ERROR
// frames, the /time response array) so every encoder shares one msgpack
// configuration.
func MarshalAny(v any, f Format) ([]byte, error) {
	switch f {
	case FormatJSON:
		return json.Marshal(v)
	case FormatMsgpack:
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		enc.UseCompactInts(true)
		if err := enc.Encode(v); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported format %d", f)
	}
}

// Unmarshal decodes data into m using the given format.
func Unmarshal(data []byte, f Format, m *ProtocolMessage) error {
	return UnmarshalAny(data, f, m)
}

// UnmarshalAny decodes an arbitrary wire value in the given format — the
// decode counterpart of MarshalAny, used by the REST surface where request
// bodies are bare Message documents rather than ProtocolMessage frames.
func UnmarshalAny(data []byte, f Format, v any) error {
	switch f {
	case FormatJSON:
		return json.Unmarshal(data, v)
	case FormatMsgpack:
		return msgpack.Unmarshal(data, v)
	default:
		return fmt.Errorf("unsupported format %d", f)
	}
}

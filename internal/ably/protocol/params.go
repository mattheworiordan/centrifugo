package protocol

// ParamsMap is the channel params attribute (RTL4k). The values are
// strings on the canonical wire, but SDKs send bare scalars too —
// ably-js resume_rewind_1 sends params {rewind: 1}, a msgpack integer /
// JSON number — and a strict map[string]string decode would reject the
// WHOLE frame ("bad inbound frame"), silently eating the ATTACH. Decoding
// coerces scalar values to their canonical string form; encoding is a
// plain string map.

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

// ParamsMap behaves as map[string]string with scalar-tolerant decoding.
type ParamsMap map[string]string

func coerceParamValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case int:
		return strconv.Itoa(t)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

// UnmarshalJSON implements json.Unmarshaler.
func (p *ParamsMap) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == nil {
		*p = nil
		return nil
	}
	out := make(ParamsMap, len(raw))
	for k, v := range raw {
		out[k] = coerceParamValue(v)
	}
	*p = out
	return nil
}

var _ msgpack.CustomDecoder = (*ParamsMap)(nil)

// DecodeMsgpack implements msgpack.CustomDecoder.
func (p *ParamsMap) DecodeMsgpack(dec *msgpack.Decoder) error {
	raw, err := dec.DecodeMap()
	if err != nil {
		return err
	}
	if raw == nil {
		*p = nil
		return nil
	}
	out := make(ParamsMap, len(raw))
	for k, v := range raw {
		out[k] = coerceParamValue(v)
	}
	*p = out
	return nil
}

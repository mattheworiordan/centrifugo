package protocol

// Derived from github.com/ably/server internal/protocol (Apache-2.0).

// Message is a published message payload — one Message within a
// ChannelMessage atomic publish.
//
// ID and Serial are distinct identifiers:
//   - ID is client-supplied and optional; it carries idempotency intent
//     so the server can reject duplicate publishes within the retention
//     window.
//   - Serial is server-assigned on publish in the form
//     `<channelSerial>:<idx>`, where channelSerial is the containing
//     ChannelMessage's serial and idx is this Message's position
//     within that batch.
type Message struct {
	ID           string `json:"id,omitempty"           msgpack:"id,omitempty"`
	Serial       string `json:"serial,omitempty"       msgpack:"serial,omitempty"`
	ClientID     string `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	// ConnectionKey is only ever populated by a REST publisher publishing
	// on behalf of an existing realtime connection (TM2h). The server
	// resolves it to a ConnectionID and never stores or delivers it.
	ConnectionKey string `json:"connectionKey,omitempty" msgpack:"connectionKey,omitempty"`
	Name          string `json:"name,omitempty"          msgpack:"name,omitempty"`
	Data          any    `json:"data,omitempty"          msgpack:"data,omitempty"`
	Encoding      string `json:"encoding,omitempty"      msgpack:"encoding,omitempty"`
	// Extras passes through verbatim (TM2i): headers, push metadata and —
	// in M8 — the AIT ai transport/codec blocks.
	Extras    any   `json:"extras,omitempty"    msgpack:"extras,omitempty"`
	Timestamp int64 `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
}

// ChannelMessage is one atomic publish on a channel: a server-assigned
// channelSerial (the discrete attach/resume point in the channel's
// stream) plus the one or more Messages published in that batch.
//
// One publish (REST request or inbound MESSAGE frame) maps to exactly
// one ChannelMessage; subscribers receive ChannelMessages as the
// atomic delivery unit (one outbound MESSAGE frame per ChannelMessage).
// Storage persists ChannelMessages keyed by ChannelSerial.
type ChannelMessage struct {
	ChannelSerial string     `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	Messages      []*Message `json:"messages,omitempty"      msgpack:"messages,omitempty"`
}

// AuthDetails carries credentials on an AUTH frame (TR4p/AD1): the
// client presents a fresh token mid-connection (RTC8) and the server
// acknowledges with an updated CONNECTED.
type AuthDetails struct {
	AccessToken string `json:"accessToken,omitempty" msgpack:"accessToken,omitempty"`
}

// ProtocolMessage is one frame on the realtime WebSocket connection.
//
// MsgSerial keeps omitempty so frames where the serial is meaningless
// (HEARTBEAT, CONNECTED, ...) match the wire shape of the real Ably
// service. ACK/NACK frames — where the serial must be present even when
// 0 (ably-js correlates pending publishes by the literal field) — do NOT
// encode through this struct: the adapter session owns a bespoke ackFrame
// wire shape for them, as it does for channel-scoped ERRORs (explicit
// channel attribute).
type ProtocolMessage struct {
	Action            Action             `json:"action"                      msgpack:"action"`
	ID                string             `json:"id,omitempty"                msgpack:"id,omitempty"`
	ConnectionID      string             `json:"connectionId,omitempty"      msgpack:"connectionId,omitempty"`
	Channel           string             `json:"channel,omitempty"           msgpack:"channel,omitempty"`
	ChannelSerial     string             `json:"channelSerial,omitempty"     msgpack:"channelSerial,omitempty"`
	MsgSerial         int64              `json:"msgSerial,omitempty"         msgpack:"msgSerial,omitempty"`
	Timestamp         int64              `json:"timestamp,omitempty"         msgpack:"timestamp,omitempty"`
	Count             int                `json:"count,omitempty"             msgpack:"count,omitempty"`
	Flags             int64              `json:"flags,omitempty"             msgpack:"flags,omitempty"`
	Messages          []*Message         `json:"messages,omitempty"          msgpack:"messages,omitempty"`
	Error             *ErrorInfo         `json:"error,omitempty"             msgpack:"error,omitempty"`
	Params            ParamsMap          `json:"params,omitempty"            msgpack:"params,omitempty"`
	Auth              *AuthDetails       `json:"auth,omitempty"              msgpack:"auth,omitempty"`
	Presence          []*PresenceMessage `json:"presence,omitempty"          msgpack:"presence,omitempty"`          // TR4l
	ConnectionDetails *ConnectionDetails `json:"connectionDetails,omitempty" msgpack:"connectionDetails,omitempty"` // TR4o
}

// ConnectionDetails carries connection-scoped constraints and metadata
// on a CONNECTED ProtocolMessage (TR4o, CD1). Durations are wire-encoded
// as integer milliseconds.
type ConnectionDetails struct {
	ClientID           string `json:"clientId,omitempty"           msgpack:"clientId,omitempty"`           // CD2a
	ConnectionKey      string `json:"connectionKey,omitempty"      msgpack:"connectionKey,omitempty"`      // CD2b
	MaxMessageSize     int64  `json:"maxMessageSize,omitempty"     msgpack:"maxMessageSize,omitempty"`     // CD2c
	MaxFrameSize       int64  `json:"maxFrameSize,omitempty"       msgpack:"maxFrameSize,omitempty"`       // CD2d
	MaxInboundRate     int64  `json:"maxInboundRate,omitempty"     msgpack:"maxInboundRate,omitempty"`     // CD2e
	ConnectionStateTTL int64  `json:"connectionStateTtl,omitempty" msgpack:"connectionStateTtl,omitempty"` // CD2f
	MaxIdleInterval    int64  `json:"maxIdleInterval,omitempty"    msgpack:"maxIdleInterval,omitempty"`    // CD2h
}

// PresenceMessage is one presence event on a channel (TP1): a member
// entering, updating its data, or leaving, plus the synthetic
// present/absent states used during SYNC.
type PresenceMessage struct {
	ID           string         `json:"id,omitempty"           msgpack:"id,omitempty"`
	Action       PresenceAction `json:"action"                 msgpack:"action"`
	ClientID     string         `json:"clientId,omitempty"     msgpack:"clientId,omitempty"`
	ConnectionID string         `json:"connectionId,omitempty" msgpack:"connectionId,omitempty"`
	Data         any            `json:"data,omitempty"         msgpack:"data,omitempty"`
	Encoding     string         `json:"encoding,omitempty"     msgpack:"encoding,omitempty"`
	// Extras passes through verbatim (TP3i-territory: headers etc.).
	Extras    any   `json:"extras,omitempty"    msgpack:"extras,omitempty"`
	Timestamp int64 `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
}

// PresenceAction is the wire enum of presence event kinds (TP2).
type PresenceAction int

const (
	PresenceAbsent  PresenceAction = 0
	PresencePresent PresenceAction = 1
	PresenceEnter   PresenceAction = 2
	PresenceLeave   PresenceAction = 3
	PresenceUpdate  PresenceAction = 4
)

// ErrorInfo describes an error in Ably's standard wire form, attached
// to a ProtocolMessage when the server needs to convey a non-fatal
// problem to the client (e.g. a resume that could not fully replay).
type ErrorInfo struct {
	Message    string `json:"message,omitempty"    msgpack:"message,omitempty"`
	Code       int    `json:"code,omitempty"       msgpack:"code,omitempty"`
	StatusCode int    `json:"statusCode,omitempty" msgpack:"statusCode,omitempty"`
	HRef       string `json:"href,omitempty"       msgpack:"href,omitempty"`
}

// Flags carried on ATTACHED (TR3).
const (
	// FlagHasPresence indicates the channel has members present at attach
	// time: the client should expect a SYNC to follow (RTL4c1-adjacent).
	FlagHasPresence int64 = 1 << 0
	// FlagHasBacklog indicates a rewind attach has retained messages to
	// deliver after ATTACHED (RTL2i; TR3).
	FlagHasBacklog int64 = 1 << 1
	// FlagTransient marks an ATTACH that should not be resumed on
	// failure (TR3).
	FlagTransient int64 = 1 << 4
	// FlagAttachResume marks an ATTACH sent to resume a prior attachment
	// (TR3): rewind params are suppressed on such attaches (pinned by
	// ably-js resume_rewind_1).
	FlagAttachResume int64 = 1 << 5
	// Channel mode flags (TR3, bits 16-19): an ATTACH carrying any mode
	// bits requests a restricted attachment; ATTACHED echoes the granted
	// modes.
	FlagModePresence          int64 = 1 << 16
	FlagModePublish           int64 = 1 << 17
	FlagModeSubscribe         int64 = 1 << 18
	FlagModePresenceSubscribe int64 = 1 << 19
	FlagModeAnnotationPublish int64 = 1 << 21
	// FlagResumed indicates the channel state was resumed from the
	// client's supplied channelSerial: the gap between the client's
	// cursor and the live tail was replayed in full. Cleared when the
	// server could not satisfy the resume in full (e.g. cap exceeded,
	// retention aged-out) — clients should treat the absence of this
	// flag as a discontinuity.
	FlagResumed int64 = 1 << 2
)

package ably

import (
	"fmt"

	"github.com/centrifugal/centrifuge"
	cproto "github.com/centrifugal/protocol"
)

// transport adapts an Ably realtime session to centrifuge's Transport
// interface. Modeled on internal/uniws/transport.go and the library's own
// websocketTransport, minus the actual socket: each []byte the centrifuge
// Client writer hands to Write/WriteMany is exactly one encoded
// protocol.Reply (DefaultProtobufReplyEncoder.Encode is MarshalVT, no
// length prefix), which is decoded back to a struct and handed to the
// session for translation into Ably frames.
type transport struct {
	session *session
}

var _ centrifuge.Transport = (*transport)(nil)

const transportName = "ably"

// Name returns name of transport.
func (t *transport) Name() string {
	return transportName
}

// Protocol declares Protobuf so the centrifuge Client writer encodes
// replies with MarshalVT — the cheapest encode/decode round trip available
// across the mandatory Transport []byte boundary (measured in the M0
// architecture spike; JSON would roughly triple the outbound mapping cost).
func (t *transport) Protocol() centrifuge.ProtocolType {
	return centrifuge.ProtocolTypeProtobuf
}

// ProtocolVersion returns transport protocol version.
func (t *transport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional. The Ably
// session feeds synthesized commands through Client.HandleCommand, so the
// centrifuge client is bidirectional.
func (t *transport) Unidirectional() bool {
	return false
}

// Emulation returns whether transport uses emulation layer.
func (t *transport) Emulation() bool {
	return false
}

// AcceptProtocol is observability-only; the Ably adapter is not one of the
// HTTP transports this label distinguishes.
func (t *transport) AcceptProtocol() string {
	return ""
}

// DisabledPushFlags disables the Disconnect push: disconnect arrives via
// Close(Disconnect) instead, as in the library's own WebSocket transport.
func (t *transport) DisabledPushFlags() uint64 {
	return centrifuge.PushFlagDisconnect
}

// PingPongConfig disables centrifuge app-level pings: Ably connection
// liveness is the session's HEARTBEAT ticker (RTN23a).
func (t *transport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{PingInterval: -1, PongTimeout: -1}
}

// Write decodes one centrifuge Reply and hands it to the session. Called
// from the centrifuge Client writer goroutine.
func (t *transport) Write(message []byte) error {
	var reply cproto.Reply
	if err := reply.UnmarshalVT(message); err != nil {
		return fmt.Errorf("ably transport: decode reply: %w", err)
	}
	t.session.handleReply(&reply)
	return nil
}

// WriteMany decodes a batch of centrifuge Replies and hands them to the
// session one by one.
func (t *transport) WriteMany(messages ...[]byte) error {
	for _, message := range messages {
		if err := t.Write(message); err != nil {
			return err
		}
	}
	return nil
}

// Close is called by centrifuge when the client is closed server-side
// (node shutdown, forced disconnect, ...).
func (t *transport) Close(disconnect centrifuge.Disconnect) error {
	t.session.handleTransportClose(disconnect)
	return nil
}

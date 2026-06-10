package ably

// session is one Ably realtime protocol connection (protocol v6) backed by
// a per-connection centrifuge.Client behind the custom Transport in
// transport.go — the architecture selected by the M0 spike (Path A:
// lightweight client + custom transport; see
// .working/ably-centrifugo-poc/spikes/path-spike/REPORT.md).
//
// Inbound (Ably → centrifuge): the session synthesizes *cproto.Command
// structs and feeds them to Client.HandleCommand — the same public entry
// point the library's own bidirectional WebSocket handler uses. No byte
// encoding happens on this leg.
//
// Outbound (centrifuge → Ably): the centrifuge Client writer encodes each
// protocol.Reply and hands the bytes to the transport, which decodes them
// back to structs and calls handleReply here for translation to Ably
// frames.
//
// Frame flow served:
//
//	WS upgrade   → CONNECTED(4) + connectionDetails (RTN6, TR4o, CD1)
//	HEARTBEAT(0) → HEARTBEAT(0) echo                (RTN13a)
//	ATTACH(10)   → ATTACHED(11)                     (RTL4c)
//	DETACH(12)   → DETACHED(13)                     (RTL5d)
//	MESSAGE(15)  → ACK(1) / NACK(2)                 (RTN7a)
//	publication  → MESSAGE(15) to subscribed conns  (RTL7)
//	CLOSE(7)     → CLOSED(8) + connection close     (RTN12a)
//
// plus a server heartbeat ticker backing the advertised maxIdleInterval
// (RTN23a).
//
// The publish leg does NOT go through the session's centrifuge client
// (decision D3): the session builds the full Ably message envelope, then
// calls node.Publish directly. Delivery rides the per-connection client:
// subscribed clients receive the publication and handleReply translates
// push.Pub back into Ably MESSAGE frames — including to the publisher
// itself via its own subscription (the RTC1a echo-on default). echo=false
// connections suppress their own messages with a subscription tags filter
// (RTL7f, see attach).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"

	"github.com/centrifugal/centrifuge"
	cproto "github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
)

// Connection constraints advertised in connectionDetails on CONNECTED
// (CD2). maxIdleIntervalMS is backed by the session heartbeat ticker: the
// ticker must fire comfortably inside the advertised interval, otherwise
// clients treat the transport as dead and reconnect (RTN23a).
const (
	maxMessageSize     = 65536
	maxFrameSize       = 524288
	maxInboundRate     = 1000
	connectionStateTTL = 120_000 // ms
	maxIdleIntervalMS  = 15_000  // ms
	heartbeatInterval  = 10 * time.Second
	connectTimeout     = 5 * time.Second
)

// Ably error codes used by the adapter, verified against the Ably error
// code registry (github.com/ably/ably-common protocol/errors.json).
const (
	errCodeBadRequest             = 40000 // bad request
	errCodeMaxMessageLength       = 40009 // maximum message length exceeded
	errCodeInvalidChannelName     = 40010 // invalid channel name
	errCodeInvalidClientID        = 40012 // invalid client id
	errCodeInvalidCredentials     = 40101 // invalid credentials
	errCodeOperationNotPermitted  = 40160 // operation not permitted with provided capability
	errCodeInternal               = 50000 // internal error
	errCodeDisconnected           = 80003 // disconnected
	errCodeChannelOperationFailed = 90000 // channel operation failed
)

// writeTimeout bounds every WS write so a non-reading client cannot pin a
// handler goroutine on a full TCP buffer (matches the uniws transport's
// write-timeout discipline).
const writeTimeout = 5 * time.Second

// pubTagOrigin is the publication tag carrying the publisher's Ably
// connectionId. Set on every adapter publish; echo=false connections
// subscribe with a SubscribeRequest.Tf filter excluding it (RTL7f, see
// attach).
const pubTagOrigin = "o"

// sessionParams carries the per-connection parameters extracted from the
// upgrade request querystring (RTN2) and authentication.
type sessionParams struct {
	// userID becomes the centrifuge Credentials UserID: the clientId query
	// param (RTN2d) when present, the authenticated key name otherwise.
	userID string
	// clientID is the clientId query param (RTN2d), echoed back in
	// connectionDetails.clientId (CD2a) when set.
	clientID string
	// echo is the echo query param (RTN2b; on by default per RTC1a). When
	// false, every ATTACH subscribes with a tags filter suppressing this
	// connection's own publications (RTL7f, see attach).
	echo bool
	// protocolVersion is the v query param (RTN2f). Stored, not yet acted
	// on.
	protocolVersion string
	// format is the wire encoding selected by the format query param
	// (RTN2a): every outbound frame is marshaled in it (msgpack frames
	// travel as WS binary messages, JSON as text) and every inbound frame
	// is unmarshaled by it.
	format protocol.Format
}

// opKind identifies the synthesized centrifuge command a pending reply
// belongs to.
type opKind int

const (
	opConnect opKind = iota
	opSubscribe
	opUnsubscribe
)

type pendingOp struct {
	kind    opKind
	channel string
}

type session struct {
	node   *centrifuge.Node
	conn   *websocket.Conn
	params sessionParams

	// writeMu serializes WS writes: the reader loop, the heartbeat ticker,
	// the centrifuge Client writer (via transport → handleReply) and the
	// transport close path all write frames.
	writeMu sync.Mutex

	client  *centrifuge.Client
	closeFn centrifuge.ClientCloseFunc

	nextCmdID atomic.Uint32
	pendingMu sync.Mutex
	pending   map[uint32]pendingOp

	connected chan error // signalled exactly once by the centrifuge connect reply

	closeMu sync.Mutex
	closed  bool
	closeCh chan struct{} // closed on teardown: stops the heartbeat ticker and cancels the client context
}

func newSession(node *centrifuge.Node, conn *websocket.Conn, params sessionParams) *session {
	return &session{
		node:      node,
		conn:      conn,
		params:    params,
		pending:   make(map[uint32]pendingOp),
		connected: make(chan error, 1),
		closeCh:   make(chan struct{}),
	}
}

// run drives the session to completion: centrifuge connect, CONNECTED
// frame, heartbeat ticker, then the read loop. Blocks until the connection
// dies.
func (s *session) run(reqCtx context.Context) {
	defer s.teardown()

	// Refuse new sessions when the node is already shutting down, as the
	// uniws handler does.
	select {
	case <-s.node.NotifyShutdown():
		return
	default:
	}

	if err := s.connect(reqCtx); err != nil {
		log.Error().Err(err).Str("transport", transportName).Msg("ably session connect failed")
		// RTN14g: an ERROR ProtocolMessage with an empty channel attribute
		// fails the connection; the server terminates it afterwards.
		_ = s.writeFrame(&protocol.ProtocolMessage{
			Action: protocol.ActionError,
			Error:  &protocol.ErrorInfo{Code: errCodeInternal, StatusCode: 500, Message: "failed to establish connection"},
		})
		return
	}

	// RTN6: the connection is successful once the initial CONNECTED
	// ProtocolMessage is sent. The Ably WS protocol has no client→server
	// CONNECT frame: the upgrade itself, with auth and options in query
	// params (RTN2), is the connect request.
	connectionID := s.client.ID()
	err := s.writeFrame(&protocol.ProtocolMessage{
		Action:       protocol.ActionConnected,
		ConnectionID: connectionID,
		ConnectionDetails: &protocol.ConnectionDetails{ // TR4o, CD1
			ClientID:           s.params.clientID,     // CD2a
			ConnectionKey:      connectionID + "!key", // CD2b; resume is M3, any opaque string
			MaxMessageSize:     maxMessageSize,        // CD2c
			MaxFrameSize:       maxFrameSize,          // CD2d
			MaxInboundRate:     maxInboundRate,        // CD2e
			ConnectionStateTTL: connectionStateTTL,    // CD2f
			MaxIdleInterval:    maxIdleIntervalMS,     // CD2h
		},
	})
	if err != nil {
		return
	}

	go s.heartbeatLoop()

	for {
		// Decode by the session's negotiated format (RTN2a) regardless of
		// the WS frame type flag — tolerant on read; writes always carry
		// the matching frame type (see writeBytes).
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, s.params.format, &m); err != nil {
			log.Warn().Err(err).Str("transport", transportName).Str("format", s.params.format.String()).Msg("bad inbound frame")
			continue
		}
		if !s.handleFrame(&m) {
			return
		}
	}
}

// connect creates the per-connection centrifuge client and synthesizes its
// Connect command, blocking until the connect reply arrives.
func (s *session) connect(reqCtx context.Context) error {
	// The centrifuge client must not be anonymous: set credentials into
	// the client context before NewClient. Centrifugo's connecting handler
	// returns nil Credentials for connections without a token
	// (internal/client/handler.go OnClientConnecting) and the library then
	// falls back to context credentials (centrifuge client.go connectCmd →
	// GetCredentials), so Ably sessions need no allow_anonymous_* config.
	ctx := centrifuge.SetCredentials(
		newCancelContext(reqCtx, s.closeCh),
		&centrifuge.Credentials{UserID: s.params.userID},
	)

	client, closeFn, err := centrifuge.NewClient(ctx, s.node, &transport{session: s})
	if err != nil {
		return fmt.Errorf("centrifuge.NewClient: %w", err)
	}
	s.client = client
	s.closeFn = closeFn

	// Feed the Connect command exactly as a native bidirectional transport
	// would after reading it off the wire.
	cmd := &cproto.Command{
		Id:      s.addPending(pendingOp{kind: opConnect}),
		Connect: &cproto.ConnectRequest{Name: transportName},
	}
	if !client.HandleCommand(cmd, cmd.SizeVT()) {
		return errors.New("centrifuge rejected connect command")
	}

	select {
	case err := <-s.connected:
		return err
	case <-time.After(connectTimeout):
		return errors.New("timeout waiting for centrifuge connect reply")
	case <-s.closeCh:
		return errors.New("session closed while connecting")
	}
}

// handleFrame processes one inbound ProtocolMessage. Returns false when
// the session must end.
func (s *session) handleFrame(m *protocol.ProtocolMessage) bool {
	switch m.Action {
	case protocol.ActionHeartbeat:
		// RTN13a: a client HEARTBEAT expects a HEARTBEAT in response; echo
		// the id so the SDK can correlate its ping.
		_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionHeartbeat, ID: m.ID})
		return true
	case protocol.ActionAttach:
		// RTL4c: ATTACH is confirmed by ATTACHED once the synthesized
		// centrifuge Subscribe succeeds (reply mapped in handleReply).
		s.attach(m.Channel)
		return true
	case protocol.ActionDetach:
		// RTL5d: DETACH is confirmed by DETACHED via the synthesized
		// centrifuge Unsubscribe (reply mapped in handleReply).
		s.detach(m.Channel)
		return true
	case protocol.ActionMessage:
		// RTL6: an inbound MESSAGE is a publish request; it is confirmed
		// with ACK or failed with NACK (RTN7a).
		s.publish(m)
		return true
	case protocol.ActionClose:
		// RTN12a: confirm the close request with CLOSED, then drop the
		// connection.
		_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionClosed})
		return false
	default:
		// PRESENCE, SYNC and AUTH handling lands in later milestones.
		log.Warn().Int("action", int(m.Action)).Str("transport", transportName).Msg("unhandled inbound action")
		return true
	}
}

// publish serves one inbound MESSAGE frame. Inbound publishes do not go
// through the session's centrifuge client: the session builds the full
// Ably message envelope server-side, then calls node.Publish directly
// (decision D3). No prior ATTACH is required — with the connection
// CONNECTED the messages are published immediately (RTL6c1), which is what
// makes transient publishing natural on this path.
func (s *session) publish(m *protocol.ProtocolMessage) {
	if m.Channel == "" {
		s.writeNack(m.MsgSerial, errCodeBadRequest, 400, "publish failed: channel attribute missing")
		return
	}
	// Same channel-name validation as attach (40010): publishing to an
	// invalid name must NACK, not reach centrifuge (pinned by ably-js
	// channelattach_publish_invalid).
	if strings.HasPrefix(m.Channel, ":") {
		s.writeNack(m.MsgSerial, errCodeInvalidChannelName, 400, "publish failed: invalid channel name")
		return
	}

	connectionID := s.client.ID()
	now := time.Now().UnixMilli()

	// Envelope every Message in full BEFORE any publish, so subscribers
	// never depend on SDK-side inheritance from the enclosing
	// ProtocolMessage, and so a rejected frame publishes nothing.
	for idx, msg := range m.Messages {
		if msg == nil {
			s.writeNack(m.MsgSerial, errCodeBadRequest, 400, "publish failed: null message")
			return
		}
		if msg.ClientID != "" && s.params.clientID != "" && msg.ClientID != s.params.clientID {
			// RTL6g: an identified connection can only publish messages
			// carrying its own clientId; an incompatible explicit clientId
			// is rejected by the service (the server-side reject expected
			// by RTL6g4's test). The connection stays usable. Full
			// capability semantics are M4.
			s.writeNack(m.MsgSerial, errCodeInvalidClientID, 400,
				fmt.Sprintf("publish failed: message clientId %q is incompatible with connection clientId %q", msg.ClientID, s.params.clientID))
			return
		}
		if msg.ID == "" {
			// Server-assigned unique id of the form
			// <connectionId>:<msgSerial>:<index> — the shape TM2a expects
			// SDKs to derive for realtime messages (the ProtocolMessage id
			// is connectionId:msgSerial per TR4n). A client-supplied id is
			// preserved; idempotency mapping is M3.
			msg.ID = fmt.Sprintf("%s:%d:%d", connectionID, m.MsgSerial, idx)
		}
		if msg.ClientID == "" {
			// RTL6g1b: the service assigns the connection's clientId to
			// messages published without one (no-op for unidentified
			// connections).
			msg.ClientID = s.params.clientID
		}
		// TM2c: the message is attributed to the publishing connection.
		msg.ConnectionID = connectionID
		if msg.Timestamp == 0 {
			// Stamp the server receipt time. SDKs never send a timestamp
			// on publish (they back-fill from the enclosing frame per
			// TM2f), so this is what subscribers observe.
			msg.Timestamp = now
		}
		// Normalize binary data to the canonical JSON-safe form (Base64
		// string + "base64" encoding segment, RSL4d1) before enveloping,
		// so the stored publication is format-agnostic. Everything else —
		// including every other encoding-chain segment — passes through
		// verbatim (see payload.go). No-op for JSON sessions: a JSON
		// decode never yields []byte data.
		normalizeMessageData(msg)
	}

	// Marshal every envelope before the first publish: the marshaled bytes
	// are both the publication payload and the measurement basis for the
	// maxMessageSize check, which must reject the frame before anything is
	// published.
	payloads := make([][]byte, 0, len(m.Messages))
	totalSize := 0
	for _, msg := range m.Messages {
		data, err := json.Marshal(msg)
		if err != nil {
			s.writeNack(m.MsgSerial, errCodeInternal, 500, fmt.Sprintf("publish failed: %s", err))
			return
		}
		payloads = append(payloads, data)
		totalSize += len(data)
	}
	// CD2c/TO3l8: maxMessageSize, advertised in connectionDetails, limits
	// the summed size of a frame's messages array (the realtime counterpart
	// of REST's RSL1i, same error code 40009). Measured here as the summed
	// marshaled-envelope byte size AFTER normalization; real Ably sums
	// name + data + clientId + extras only, so the server-added envelope
	// fields (id, connectionId, timestamp) — and for binary payloads the
	// ~33% Base64 inflation of the canonical form — make this adapter
	// marginally stricter, never looser. This
	// is the application-level limit: the frame is NACKed, nothing is
	// published, and the connection stays usable — contrast the protocol-
	// level maxFrameSize read limit set in serveRealtime, which kills the
	// connection outright.
	if totalSize > maxMessageSize {
		s.writeNack(m.MsgSerial, errCodeMaxMessageLength, 400,
			fmt.Sprintf("publish failed: maximum message length exceeded (%d bytes, limit %d)", totalSize, maxMessageSize))
		return
	}

	// Publish sequentially in frame order. Each Message becomes one
	// centrifuge Publication; order preservation is what M1.2 guarantees —
	// atomic batch semantics and server-assigned serials land with
	// channelSerial in M6/M8. Until then a mid-frame publish failure
	// NACKs the whole frame with the already-published prefix NOT rolled
	// back: NACK is the honest verdict (an ACK would falsely confirm the
	// tail), and an SDK retry (RTN19a) may duplicate the prefix until
	// idempotent dedup lands in M3.
	for _, data := range payloads {
		_, err := s.node.Publish(m.Channel, data,
			centrifuge.WithTags(map[string]string{pubTagOrigin: connectionID}))
		if err != nil {
			log.Error().Err(err).Str("channel", m.Channel).Str("transport", transportName).Msg("publish failed")
			s.writeNack(m.MsgSerial, errCodeInternal, 500, "publish failed")
			return
		}
	}
	s.writeAck(m.MsgSerial)
}

// ackFrame is the ACK/NACK wire shape. Unlike ProtocolMessage, msgSerial
// and count are NOT omitempty — in either format: ably-js correlates
// pending publishes via completeMessages({serial, count})
// (src/common/lib/transport/protocol.ts onAck) and an omitted msgSerial —
// the first publish on a connection is serial 0 (RTN7b) — makes that
// lookup undefined and the publish promise never settles (found
// empirically via the publish_no_attach probe; ably-go tolerated
// missing-as-0). TR4j-faithful: the serial is always present on ACK/NACK.
// Zero-value emission of the msgpack tags is pinned by
// TestAckFrameMsgpackWireShape.
type ackFrame struct {
	Action    protocol.Action     `json:"action"          msgpack:"action"`
	MsgSerial int64               `json:"msgSerial"       msgpack:"msgSerial"`
	Count     int                 `json:"count"           msgpack:"count"`
	Error     *protocol.ErrorInfo `json:"error,omitempty" msgpack:"error,omitempty"`
}

// writeAck confirms one inbound MESSAGE frame (RTN7a). msgSerial
// round-trips exactly; count is 1: one inbound frame is one serial
// (RTN7b).
func (s *session) writeAck(msgSerial int64) {
	s.writeWire(&ackFrame{
		Action:    protocol.ActionAck,
		MsgSerial: msgSerial,
		Count:     1,
	})
}

// writeNack fails one inbound MESSAGE frame (RTN7a), consuming its
// msgSerial.
func (s *session) writeNack(msgSerial int64, code int, statusCode int, message string) {
	s.writeWire(&ackFrame{
		Action:    protocol.ActionNack,
		MsgSerial: msgSerial,
		Count:     1,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	})
}

func (s *session) attach(channel string) {
	// Channel-name validation, failing the CHANNEL — never the connection
	// (pinned by ably-js channelattachempty/channelattachinvalid, RTL4d
	// territory): empty names and names beginning with ':' are invalid,
	// code 40010. Validated here so the bad name never reaches centrifuge,
	// whose empty-channel handling disconnects the whole client. The full
	// Ably channel-name grammar is not enforced. detach() deliberately has
	// no such guard: after the 40010 the channel is FAILED client-side and
	// conforming SDKs never send DETACH for it (an empty-name DETACH from a
	// non-conforming client gets centrifuge's bad-request disconnect).
	if channel == "" || strings.HasPrefix(channel, ":") {
		s.writeChannelError(channel, errCodeInvalidChannelName, 400, "invalid channel name")
		return
	}
	sub := &cproto.SubscribeRequest{Channel: channel}
	if !s.params.echo {
		// RTL7f: an echo=false connection (RTN2b; RTC1a — echo defaults to
		// on) must not receive its own publications back. Every adapter
		// publish tags its publications with the publisher's connectionId
		// (pubTagOrigin, see publish), so the subscription carries a tags
		// filter excluding them. The leaf FilterNode (Op "") with Cmp "neq"
		// matches when the tag is absent OR differs (centrifuge
		// internal/filter CompareNotEQ), so untagged publications from
		// native Centrifugo clients still deliver.
		//
		// Preconditions and invariants, both verified in centrifuge:
		//   - The filter is only honored when the channel allows tags
		//     filters (SubscribeOptions.AllowTagsFilter, centrifugo channel
		//     option allow_tags_filter); otherwise centrifuge rejects the
		//     subscribe (client.go subscribeCmd).
		//   - Filtered publications still reach writePublication for offset
		//     tracking (hub.go broadcastPublication), so the subscriber's
		//     stream position advances and a future resume (M3) sees no
		//     false discontinuity.
		sub.Tf = &cproto.FilterNode{Cmp: "neq", Key: pubTagOrigin, Val: s.client.ID()}
	}
	cmd := &cproto.Command{
		Id:        s.addPending(pendingOp{kind: opSubscribe, channel: channel}),
		Subscribe: sub,
	}
	if !s.client.HandleCommand(cmd, cmd.SizeVT()) {
		// HandleCommand can return false after a successful dispatch (its
		// post-dispatch context check); only report failure when the pending
		// entry was NOT already consumed by a reply.
		if _, ok := s.takePending(cmd.Id); ok {
			s.writeAttachError(channel, nil)
		}
	}
}

func (s *session) detach(channel string) {
	cmd := &cproto.Command{
		Id:          s.addPending(pendingOp{kind: opUnsubscribe, channel: channel}),
		Unsubscribe: &cproto.UnsubscribeRequest{Channel: channel},
	}
	if !s.client.HandleCommand(cmd, cmd.SizeVT()) {
		if _, ok := s.takePending(cmd.Id); ok {
			log.Warn().Str("channel", channel).Str("transport", transportName).Msg("unsubscribe rejected")
		}
	}
}

// handleReply maps one decoded centrifuge protocol.Reply to Ably frames.
// Called from the centrifuge Client writer goroutine (through
// transport.Write/WriteMany).
func (s *session) handleReply(reply *cproto.Reply) {
	if reply.Id != 0 {
		op, ok := s.takePending(reply.Id)
		if !ok {
			log.Warn().Uint32("id", reply.Id).Str("transport", transportName).Msg("reply for unknown command id")
			return
		}
		switch op.kind {
		case opConnect:
			var err error
			if reply.Error != nil {
				err = fmt.Errorf("centrifuge connect error %d: %s", reply.Error.Code, reply.Error.Message)
			}
			s.connected <- err
		case opSubscribe:
			if reply.Error != nil {
				s.writeAttachError(op.channel, reply.Error)
				return
			}
			// RTL4c: the confirmation ATTACHED carries the channel.
			_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionAttached, Channel: op.channel})
		case opUnsubscribe:
			// RTL5d: confirm with DETACHED. Centrifuge unsubscribe is
			// idempotent, so detaching a never-attached channel still
			// confirms — matching Ably, where DETACH on a detached channel
			// is not an error.
			if reply.Error != nil {
				log.Warn().Str("channel", op.channel).Uint32("code", reply.Error.Code).Str("transport", transportName).Msg("unsubscribe error")
			}
			_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionDetached, Channel: op.channel})
		}
		return
	}

	if reply.Push != nil {
		if reply.Push.Pub != nil {
			// RTL7: deliver the publication as a MESSAGE frame. The channel
			// lives on Push.Channel: centrifuge sets it on every
			// subscription push unless the client requested channel
			// compression via a SubscribeRequest flag this adapter never
			// sets (Publication.Channel is only populated for wildcard
			// subscriptions).
			s.deliverPublication(reply.Push.Channel, reply.Push.Pub)
			return
		}
		// Join/Leave (presence, M5), Unsubscribe and Refresh pushes are
		// translated in later milestones. Until then they are dropped.
		log.Debug().Str("channel", reply.Push.Channel).Str("transport", transportName).Msg("push dropped (translation lands in a later milestone)")
		return
	}
	// An empty reply is a centrifuge server ping — disabled via
	// PingPongConfig{PingInterval: -1}; Ably liveness is the HEARTBEAT
	// ticker.
}

// deliverPublication translates one centrifuge Publication into an Ably
// MESSAGE frame. The publication data is one fully-enveloped Ably Message
// JSON document, built by the publishing session before node.Publish, so
// delivery is a decode and re-frame — no field synthesis happens here. A
// payload that does not decode is logged and dropped; it must not kill the
// session.
func (s *session) deliverPublication(channel string, pub *cproto.Publication) {
	var msg protocol.Message
	if err := json.Unmarshal(pub.Data, &msg); err != nil {
		log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("dropping publication with undecodable payload")
		return
	}
	if s.params.format == protocol.FormatMsgpack {
		// The stored envelope is the canonical JSON-safe form; a msgpack
		// session receives binary payloads as the msgpack binary type with
		// the transport "base64" segment popped (RSL4c1, see payload.go).
		denormalizeMessageData(&msg)
	}
	// The frame timestamp doubles as the TM2f inheritance source for SDKs;
	// the enveloped message carries its own timestamp anyway.
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		Messages:  []*protocol.Message{&msg},
		Timestamp: time.Now().UnixMilli(),
	})
}

// writeAttachError fails an ATTACH on the client. RTL14: an ERROR
// ProtocolMessage carrying the channel attribute transitions that channel
// to FAILED. A centrifuge permission-denied maps to Ably's
// operation-not-permitted capability error (40160, statusCode 401); any
// other failure maps to the generic channel-operation-failed code (90000)
// until full capability semantics land in M4. replyErr may be nil (the
// command never dispatched).
func (s *session) writeAttachError(channel string, replyErr *cproto.Error) {
	code, statusCode, detail := errCodeChannelOperationFailed, 400, "subscribe rejected"
	if replyErr != nil {
		detail = replyErr.Message
		if replyErr.Code == centrifuge.ErrorPermissionDenied.Code {
			code, statusCode = errCodeOperationNotPermitted, 401
		}
	}
	s.writeChannelError(channel, code, statusCode, fmt.Sprintf("attach failed: %s", detail))
}

// channelErrorFrame is the wire shape of a channel-scoped ERROR. Unlike
// ProtocolMessage, channel is NOT omitempty — in either format: ably-js
// routes an ERROR to a channel only when the channel attribute is present
// (otherwise it fails the whole connection, RTN14g semantics), and the
// empty channel name "" is itself attachable — its attach error must
// carry "channel":"" explicitly (pinned by ably-js channelattachempty).
// Zero-value emission of the msgpack tags is pinned by
// TestChannelErrorFrameMsgpackWireShape.
type channelErrorFrame struct {
	Action  protocol.Action     `json:"action"  msgpack:"action"`
	Channel string              `json:"channel" msgpack:"channel"`
	Error   *protocol.ErrorInfo `json:"error"   msgpack:"error"`
}

// writeChannelError fails one channel on the client with an ERROR frame
// carrying the channel attribute (RTL14: the channel transitions to
// FAILED; the connection stays up).
func (s *session) writeChannelError(channel string, code int, statusCode int, message string) {
	s.writeWire(&channelErrorFrame{
		Action:  protocol.ActionError,
		Channel: channel,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	})
}

// handleTransportClose runs when centrifuge closes the client server-side
// (node shutdown, forced disconnect, ...). The transport declares
// DisabledPushFlags = PushFlagDisconnect, so this is the only disconnect
// path — as in the library's own WebSocket transport. DISCONNECTED tells
// the SDK the transport is gone but the connection may be retried
// (RTN15h3: a non-token error triggers an immediate reconnect attempt).
//
// It also runs as a synchronous echo of teardown's own closeFn() —
// centrifuge's client close drives Transport.Close on the closing
// goroutine — in which case beginClose reports the session already
// closing and there is nothing to report to the client.
func (s *session) handleTransportClose(disconnect centrifuge.Disconnect) {
	if !s.beginClose() {
		return
	}
	if disconnect.Code != centrifuge.DisconnectConnectionClosed.Code {
		_ = s.writeFrame(&protocol.ProtocolMessage{
			Action: protocol.ActionDisconnected,
			Error: &protocol.ErrorInfo{
				Code:       errCodeDisconnected,
				StatusCode: 503,
				Message:    fmt.Sprintf("connection closed: %s", disconnect.Reason),
			},
		})
	}
	// The client is already closing inside centrifuge — calling closeFn
	// here is both unnecessary and the path back into this very function.
	_ = s.conn.Close()
}

// heartbeatLoop emits server-initiated activity so the client never trips
// the maxIdleInterval read deadline it adopted from connectionDetails
// (RTN23a).
func (s *session) heartbeatLoop() {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionHeartbeat}) != nil {
				return
			}
		case <-s.closeCh:
			return
		}
	}
}

// beginClose marks the session closed and fires closeCh, returning false
// when the session was already closing. A plain sync.Once cannot provide
// this idempotency: teardown's closeFn() synchronously drives centrifuge's
// client close into Transport.Close → handleTransportClose → back here,
// and a re-entrant Once.Do deadlocks.
func (s *session) beginClose() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	close(s.closeCh) // stops the heartbeat ticker; cancels the client context
	return true
}

func (s *session) teardown() {
	if !s.beginClose() {
		return
	}
	if s.closeFn != nil {
		_ = s.closeFn()
	}
	_ = s.conn.Close()
}

// writeFrame marshals and writes one ProtocolMessage in the session's
// wire format (RTN2a).
func (s *session) writeFrame(m *protocol.ProtocolMessage) error {
	data, err := protocol.Marshal(m, s.params.format)
	if err != nil {
		return err
	}
	return s.writeBytes(data)
}

// writeWire marshals and writes a bespoke wire frame in the session's
// format — used where the shared ProtocolMessage omitempty tags would
// drop a field that must be present on the wire (ackFrame,
// channelErrorFrame).
func (s *session) writeWire(v any) {
	data, err := protocol.MarshalAny(v, s.params.format)
	if err != nil {
		log.Error().Err(err).Str("transport", transportName).Msg("marshal wire frame")
		return
	}
	_ = s.writeBytes(data)
}

// writeBytes writes one encoded frame with the WS frame type matching the
// session's wire format: msgpack frames are binary messages, JSON frames
// are text messages.
func (s *session) writeBytes(data []byte) error {
	messageType := websocket.TextMessage
	if s.params.format == protocol.FormatMsgpack {
		messageType = websocket.BinaryMessage
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return s.conn.WriteMessage(messageType, data)
}

func (s *session) addPending(op pendingOp) uint32 {
	id := s.nextCmdID.Add(1)
	s.pendingMu.Lock()
	s.pending[id] = op
	s.pendingMu.Unlock()
	return id
}

func (s *session) takePending(id uint32) (pendingOp, bool) {
	s.pendingMu.Lock()
	op, ok := s.pending[id]
	delete(s.pending, id)
	s.pendingMu.Unlock()
	return op, ok
}

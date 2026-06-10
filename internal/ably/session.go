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

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
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
	errCodeBadRequest              = 40000 // bad request
	errCodeInvalidConnectionID     = 40006 // invalid connection id
	errCodeMaxMessageLength        = 40009 // maximum message length exceeded
	errCodeInvalidChannelName      = 40010 // invalid channel name
	errCodeInvalidClientID         = 40012 // invalid client id
	errCodeInvalidCredentials      = 40101 // invalid credentials
	errCodeIncompatibleCredentials = 40102 // incompatible credentials
	errCodeOperationNotPermitted   = 40160 // operation not permitted with provided capability
	errCodeNotFound                = 40400 // not found
	errCodeInternal                = 50000 // internal error
	errCodeDisconnected            = 80003 // disconnected
	errCodeChannelOperationFailed  = 90000 // channel operation failed
	errCodePresenceNoClientID      = 91000 // unable to enter presence channel (no clientId)
	errCodePresenceNotEntered      = 91002 // unable to leave presence channel that is not entered
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
	// clientID is the connection's bound identity — the clientId query
	// param (RTN2d) or the token-bound clientId (RSA7a) — echoed back in
	// connectionDetails.clientId (CD2a) when set.
	clientID string
	// wildcardClientID reports a wildcard-token connection that assumed
	// no identity: connectionDetails.clientId carries the literal "*"
	// (RSA15b) while messages stay unstamped.
	wildcardClientID bool
	// echo is the echo query param (RTN2b; on by default per RTC1a). When
	// false, every ATTACH subscribes with a tags filter suppressing this
	// connection's own publications (RTL7f, see attach).
	echo bool
	// protocolVersion is the v query param (RTN2f). Stored, not yet acted
	// on.
	protocolVersion string
	// capability governs this connection (see authResult.capability),
	// enforced on attach and publish.
	capability auth.Capability
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
	// params/modes carry the ATTACH request context to the ATTACHED reply
	// (RTL4k params echo; RTL4m mode grant).
	params map[string]string
	modes  int64
}

type session struct {
	node     *centrifuge.Node
	mint     *serialMint
	conn     *websocket.Conn
	params   sessionParams
	presence *presenceStore

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

	// modesMu guards attachedModes: granted mode bits per attached channel
	// (0 = unrestricted). Written by the reader loop (attach/detach
	// replies arrive via the centrifuge writer goroutine), read at
	// enforcement points on both goroutines.
	modesMu       sync.Mutex
	attachedModes map[string]int64

	closeMu sync.Mutex
	closed  bool
	// cleanClose marks a client-initiated CLOSE (RTN12a): presence leaves
	// immediately. Abrupt drops keep members for the grace window.
	cleanClose bool
	closeCh    chan struct{} // closed on teardown: stops the heartbeat ticker and cancels the client context
}

func newSession(node *centrifuge.Node, conn *websocket.Conn, params sessionParams, presence *presenceStore, mint *serialMint) *session {
	return &session{
		node:          node,
		mint:          mint,
		conn:          conn,
		params:        params,
		presence:      presence,
		pending:       make(map[uint32]pendingOp),
		attachedModes: make(map[string]int64),
		connected:     make(chan error, 1),
		closeCh:       make(chan struct{}),
	}
}

// run drives the session to completion: centrifuge connect, CONNECTED
// frame, heartbeat ticker, then the read loop. Blocks until the connection
// dies.
func (s *session) run(reqCtx context.Context) {
	// Presence cleanup is a DEDICATED defer, not part of teardown: when a
	// server-side disconnect wins beginClose (handleTransportClose first,
	// e.g. node shutdown), teardown early-returns — but the reader loop
	// always exits, so this defer is the one guaranteed cleanup point.
	defer s.leavePresence()
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
	detailsClientID := s.params.clientID
	if s.params.wildcardClientID {
		// RSA15b: a wildcard-token connection that assumed no identity
		// advertises the literal "*" — the SDK knows it may publish on
		// behalf of any clientId. Messages stay unstamped.
		detailsClientID = "*"
	}
	connectionID := s.client.ID()
	err := s.writeFrame(&protocol.ProtocolMessage{
		Action:       protocol.ActionConnected,
		ConnectionID: connectionID,
		ConnectionDetails: &protocol.ConnectionDetails{ // TR4o, CD1
			ClientID:           detailsClientID,       // CD2a
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
		s.attach(m)
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
	case protocol.ActionPresence:
		// RTP territory: presence operations consume the frame's msgSerial
		// and are confirmed with ACK like publishes.
		s.handlePresence(m)
		return true
	case protocol.ActionClose:
		// RTN12a: confirm the close request with CLOSED, then drop the
		// connection. A clean close leaves presence immediately.
		s.closeMu.Lock()
		s.cleanClose = true
		s.closeMu.Unlock()
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
	if !validChannelName(m.Channel) {
		s.writeNack(m.MsgSerial, errCodeInvalidChannelName, 400, "publish failed: invalid channel name")
		return
	}
	// Publishing requires the publish operation on the channel (40160).
	if !s.params.capability.Allows(auth.OpPublish, m.Channel) {
		s.writeNack(m.MsgSerial, errCodeOperationNotPermitted, 401, "publish failed: capability does not permit publish")
		return
	}
	// RTL4m enforcement: an attachment restricted to modes lacking publish
	// rejects publishes with 40160. An unattached channel (modes==0) is a
	// transient publish, unrestricted by modes.
	if !modeAllows(s.channelModes(m.Channel), protocol.FlagModePublish) {
		s.writeNack(m.MsgSerial, errCodeOperationNotPermitted, 401, "publish failed: channel mode does not permit publish")
		return
	}

	connectionID := s.client.ID()

	// Validation, envelope rules (CD2c/TO3l8 size accounting included) and
	// payload normalization live in the shared publish core (publish.go),
	// reused verbatim by the REST publish surface. The application-level
	// 40009 reject NACKs the frame and keeps the connection alive —
	// contrast the protocol-level maxFrameSize read limit set in
	// serveRealtime, which kills the connection outright.
	payloads, idemKeys, serials, problem := buildEnvelopes(m.Messages, envelopeParams{
		connectionID: connectionID,
		clientID:     s.params.clientID,
		mintSerial:   func() string { return s.mint.Mint(m.Channel) },
		newID: func(idx int) string {
			// Server-assigned unique id of the form
			// <connectionId>:<msgSerial>:<index> — the shape TM2a expects
			// SDKs to derive for realtime messages (the ProtocolMessage id
			// is connectionId:msgSerial per TR4n).
			return fmt.Sprintf("%s:%d:%d", connectionID, m.MsgSerial, idx)
		},
	})
	if problem != nil {
		s.writeNack(m.MsgSerial, problem.code, problem.statusCode, "publish failed: "+problem.message)
		return
	}

	// Publish sequentially in frame order. Each Message becomes one
	// centrifuge Publication; order preservation is what M1.2 guarantees —
	// atomic batch semantics and server-assigned serials land with
	// channelSerial in M6/M8. Until then a mid-frame publish failure
	// NACKs the whole frame with the already-published prefix NOT rolled
	// back: NACK is the honest verdict (an ACK would falsely confirm the
	// tail), and an SDK retry (RTN19a) may duplicate the prefix until
	// idempotent dedup lands in M3.2.
	for i, data := range payloads {
		// A client-supplied message id dedups republishes within the
		// retention window (RSL1k2/RSL1k5): the broker returns the cached
		// stream position and publishes nothing, so the duplicate still
		// ACKs (the SDK retry contract) without a second delivery.
		opts := publishOptions(m.Channel, connectionID, serials[i])
		if idemKeys[i] != "" {
			opts = append(opts, centrifuge.WithIdempotencyKey(idemKeys[i]),
				centrifuge.WithIdempotentResultTTL(idempotentResultTTL))
		}
		_, err := s.node.Publish(m.Channel, data, opts...)
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

// defaultChannelModes is the mode set granted to an attach that requests
// no restriction — real Ably always carries mode bits on ATTACHED, and a
// default attachment gets this full set (pinned by ably-js
// attachWithInvalidChannelParams: a plain attach yields channel.modes ==
// ['presence','publish','subscribe','presence_subscribe','annotation_publish']).
const defaultChannelModes = protocol.FlagModePresence | protocol.FlagModePublish |
	protocol.FlagModeSubscribe | protocol.FlagModePresenceSubscribe |
	protocol.FlagModeAnnotationPublish

// recognizedChannelParams is the set of channel params echoed back on
// ATTACHED — unrecognized params are dropped, not echoed verbatim (pinned
// by ably-js attachWithInvalidChannelParams: params {nonexistent:'foo'}
// yields channel.params == {}).
var recognizedChannelParams = map[string]bool{
	"modes":     true,
	"delta":     true,
	"rewind":    true,
	"occupancy": true,
}

// filterChannelParams returns only the recognized params (nil when none
// survive, so omitempty drops the attribute and SDKs see params {}).
func filterChannelParams(params map[string]string) map[string]string {
	var out map[string]string
	for k, v := range params {
		if recognizedChannelParams[k] {
			if out == nil {
				out = make(map[string]string, len(params))
			}
			out[k] = v
		}
	}
	return out
}

// modesFromAttach derives the requested mode bits from an ATTACH frame:
// either the params "modes" comma-list (RTL4k, channelOptions.params
// form) or the mode flag bits (TR3, channelOptions.modes form). When both
// are present params.modes wins outright — NOT a union (pinned by ably-js
// attachWithChannelParamsModesAndChannelModes: "modes is ignored when
// params.modes is present"). Zero means unrestricted.
func modesFromAttach(m *protocol.ProtocolMessage) int64 {
	if raw, ok := m.Params["modes"]; ok {
		var modes int64
		for _, name := range strings.Split(raw, ",") {
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "presence":
				modes |= protocol.FlagModePresence
			case "publish":
				modes |= protocol.FlagModePublish
			case "subscribe":
				modes |= protocol.FlagModeSubscribe
			case "presence_subscribe":
				modes |= protocol.FlagModePresenceSubscribe
			}
		}
		return modes
	}
	return m.Flags & (protocol.FlagModePresence | protocol.FlagModePublish |
		protocol.FlagModeSubscribe | protocol.FlagModePresenceSubscribe)
}

// channelModes returns the granted mode bits for a channel (0 =
// unrestricted or not attached).
func (s *session) channelModes(channel string) int64 {
	s.modesMu.Lock()
	defer s.modesMu.Unlock()
	return s.attachedModes[channel]
}

// modeAllows reports whether the attachment's modes permit the ability —
// unrestricted attachments (and unattached channels: transient publish)
// permit everything.
func modeAllows(modes int64, ability int64) bool {
	return modes == 0 || modes&ability != 0
}

// writeAttached records the attachment grant and confirms it on the wire:
// ATTACHED echoing the recognized params (RTL4k1) and the granted mode bits
// (RTL4m), HAS_PRESENCE + a single-page SYNC when members exist (RTP18-lite
// — members delivered as PRESENT; no channelSerial cursor means ably-js
// setPresence completes the sync on this frame). Called from the
// opSubscribe reply (fresh attach, centrifuge reply goroutine) and from
// the re-attach update path (frame-reader goroutine).
// latestChannelSerial returns the serial of the channel's most recent
// publication — the attach point ATTACHED advertises (RTL15a
// attachSerial) — or "" for a channel with no retained publications.
func (s *session) latestChannelSerial(channel string) string {
	res, err := s.node.History(channel, centrifuge.WithLimit(1), centrifuge.WithReverse(true))
	if err != nil {
		// A channel without history configured (or a broker hiccup) just
		// attaches without a serial — never fail the attach over it.
		return ""
	}
	if len(res.Publications) == 0 {
		return ""
	}
	return res.Publications[0].Tags[pubTagSerial]
}

func (s *session) writeAttached(channel string, params map[string]string, modes int64) {
	// An unrestricted request is granted the full default mode set —
	// ATTACHED always carries mode bits.
	if modes == 0 {
		modes = defaultChannelModes
	}
	var members []*protocol.PresenceMessage
	if modeAllows(modes, protocol.FlagModePresenceSubscribe) {
		members = s.presence.members(channel)
	}
	var flags int64
	if len(members) > 0 {
		flags |= protocol.FlagHasPresence
	}
	flags |= modes
	s.modesMu.Lock()
	s.attachedModes[channel] = modes
	s.modesMu.Unlock()
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       channel,
		ChannelSerial: s.latestChannelSerial(channel), // RTL15a: the attach point
		Flags:         flags,
		Params:        filterChannelParams(params),
	})
	if len(members) > 0 {
		snapshot := make([]*protocol.PresenceMessage, 0, len(members))
		for _, m := range members {
			present := *m
			present.Action = protocol.PresencePresent
			snapshot = append(snapshot, &present)
		}
		_ = s.writeFrame(&protocol.ProtocolMessage{
			Action:   protocol.ActionSync,
			Channel:  channel,
			Presence: snapshot,
		})
	}
}

func (s *session) attach(m *protocol.ProtocolMessage) {
	channel := m.Channel
	// Channel-name validation (validChannelName, publish.go), failing the
	// CHANNEL — never the connection (pinned by ably-js
	// channelattachempty/channelattachinvalid, RTL4d territory): code
	// 40010. Validated here so the bad name never reaches centrifuge,
	// whose empty-channel handling disconnects the whole client. detach()
	// deliberately has no such guard: after the 40010 the channel is
	// FAILED client-side and conforming SDKs never send DETACH for it (an
	// empty-name DETACH from a non-conforming client gets centrifuge's
	// bad-request disconnect).
	if !validChannelName(channel) {
		s.writeChannelError(channel, errCodeInvalidChannelName, 400, "invalid channel name")
		return
	}
	// Attaching requires the subscribe operation on the channel: a
	// capability miss fails the CHANNEL with 40160 (operation not
	// permitted with provided capability), never the connection.
	if !s.params.capability.Allows(auth.OpSubscribe, channel) {
		s.writeChannelError(channel, errCodeOperationNotPermitted, 401, "capability does not permit subscribe")
		return
	}
	// An ATTACH for an already-attached channel is an options update
	// (RTL4h territory: ably-js setOptions re-attaches to renegotiate
	// params/modes and resolves on the fresh ATTACHED). The centrifuge
	// subscription already exists — and re-subscribing would be rejected
	// as a duplicate — so the new grant is confirmed directly.
	s.modesMu.Lock()
	_, alreadyAttached := s.attachedModes[channel]
	s.modesMu.Unlock()
	if alreadyAttached {
		s.writeAttached(channel, m.Params, modesFromAttach(m))
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
		Id: s.addPending(pendingOp{
			kind:    opSubscribe,
			channel: channel,
			params:  m.Params,
			modes:   modesFromAttach(m),
		}),
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

// handlePresence serves one inbound PRESENCE frame (RTP): each entry is
// an ENTER, UPDATE or LEAVE op; the member set updates and the event fans
// out as a presence-tagged publication. The frame's msgSerial is ACKed
// after every entry lands, mirroring the publish contract.
func (s *session) handlePresence(m *protocol.ProtocolMessage) {
	if !validChannelName(m.Channel) {
		s.writeNack(m.MsgSerial, errCodeInvalidChannelName, 400, "presence failed: invalid channel name")
		return
	}
	// Presence requires the presence operation on the channel (40160).
	if !s.params.capability.Allows(auth.OpPresence, m.Channel) {
		s.writeNack(m.MsgSerial, errCodeOperationNotPermitted, 401, "presence failed: capability does not permit presence")
		return
	}
	// RTL4m enforcement: presence operations need the presence mode.
	if !modeAllows(s.channelModes(m.Channel), protocol.FlagModePresence) {
		s.writeNack(m.MsgSerial, errCodeOperationNotPermitted, 401, "presence failed: channel mode does not permit presence")
		return
	}
	connectionID := s.client.ID()
	now := time.Now().UnixMilli()
	for idx, pm := range m.Presence {
		if pm == nil {
			s.writeNack(m.MsgSerial, errCodeBadRequest, 400, "presence failed: null entry")
			return
		}
		// RTP8: a presence member needs an identity — the entry's clientId
		// or the connection's (91000 otherwise).
		clientID := pm.ClientID
		if clientID == "" {
			clientID = s.params.clientID
		}
		if clientID == "" {
			s.writeNack(m.MsgSerial, errCodePresenceNoClientID, 400, "presence failed: no clientId")
			return
		}
		entry := *pm
		entry.ClientID = clientID
		entry.ConnectionID = connectionID // TP3 attribution
		if entry.ID == "" {
			entry.ID = fmt.Sprintf("%s:%d:%d", connectionID, m.MsgSerial, idx)
		}
		if entry.Timestamp == 0 {
			entry.Timestamp = now
		}
		normalizePresenceData(&entry)

		switch entry.Action {
		case protocol.PresenceEnter, protocol.PresenceUpdate:
			s.presence.set(m.Channel, &entry)
		case protocol.PresenceLeave:
			if !s.presence.remove(m.Channel, connectionID, clientID) {
				s.writeNack(m.MsgSerial, errCodePresenceNotEntered, 400, "presence failed: not entered")
				return
			}
		default:
			s.writeNack(m.MsgSerial, errCodeBadRequest, 400, "presence failed: unsupported action")
			return
		}
		if err := publishPresenceEvent(s.node, s.mint, m.Channel, &entry); err != nil {
			log.Error().Err(err).Str("channel", m.Channel).Str("transport", transportName).Msg("presence publish failed")
			s.writeNack(m.MsgSerial, errCodeInternal, 500, "presence failed")
			return
		}
	}
	s.writeAck(m.MsgSerial)
}

// publishPresenceEvent fans a presence event out as a presence-tagged
// publication: no history options (presence never pollutes message
// history) and no origin tag (presence events reach everyone, including
// the originator, regardless of the echo=false message filter). Package
// level so grace timers outliving their session can use it.
func publishPresenceEvent(node *centrifuge.Node, mint *serialMint, channel string, entry *protocol.PresenceMessage) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	// RTL15b: presence events advance the channel position too — the
	// serial is drawn from the same per-channel sequence as messages.
	cs := mint.Mint(channel)
	if _, err = node.Publish(channel, data,
		centrifuge.WithTags(map[string]string{pubTagKind: pubTagKindPresence, pubTagSerial: cs})); err != nil {
		return err
	}
	// Presence history lives on the client-unreachable shadow channel with
	// the live channel's retention tier — the live publication above stays
	// history-free so message history is never polluted.
	historyOpts := publishOptions(channel, "", cs)
	_, err = node.Publish(presenceHistoryChannel(channel), data, historyOpts...)
	return err
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
			s.writeAttached(op.channel, op.params, op.modes)
		case opUnsubscribe:
			// RTL5d: confirm with DETACHED. Centrifuge unsubscribe is
			// idempotent, so detaching a never-attached channel still
			// confirms — matching Ably, where DETACH on a detached channel
			// is not an error.
			if reply.Error != nil {
				log.Warn().Str("channel", op.channel).Uint32("code", reply.Error.Code).Str("transport", transportName).Msg("unsubscribe error")
			}
			s.modesMu.Lock()
			delete(s.attachedModes, op.channel)
			s.modesMu.Unlock()
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
	if pub.Tags[pubTagKind] == pubTagKindPresence {
		// RTL4m: an attachment restricted to modes lacking
		// presence_subscribe does not receive presence events.
		if !modeAllows(s.channelModes(channel), protocol.FlagModePresenceSubscribe) {
			return
		}
		var pm protocol.PresenceMessage
		if err := json.Unmarshal(pub.Data, &pm); err != nil {
			log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("dropping presence publication with undecodable payload")
			return
		}
		_ = s.writeFrame(&protocol.ProtocolMessage{
			Action:        protocol.ActionPresence,
			Channel:       channel,
			ChannelSerial: pub.Tags[pubTagSerial], // RTL15b: presence advances the channel position
			Presence:      []*protocol.PresenceMessage{&pm},
			Timestamp:     time.Now().UnixMilli(),
		})
		return
	}
	// RTL4m: an attachment restricted to modes lacking subscribe does not
	// receive messages.
	if !modeAllows(s.channelModes(channel), protocol.FlagModeSubscribe) {
		return
	}
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
		Action:        protocol.ActionMessage,
		Channel:       channel,
		ChannelSerial: pub.Tags[pubTagSerial], // RTL15b: SDKs track this as channel.properties.channelSerial
		Messages:      []*protocol.Message{&msg},
		Timestamp:     time.Now().UnixMilli(),
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

// leavePresence handles the connection's members at teardown: a clean
// CLOSE leaves immediately; an abrupt drop keeps members present for the
// grace window (advertised ~15s) so a reconnecting client never flickers,
// with synthesized LEAVEs only for identities that did not re-enter.
func (s *session) leavePresence() {
	if s.client == nil {
		return
	}
	s.closeMu.Lock()
	clean := s.cleanClose
	s.closeMu.Unlock()
	node := s.node
	mint := s.mint
	fanout := func(channel string, member *protocol.PresenceMessage) {
		if err := publishPresenceEvent(node, mint, channel, member); err != nil {
			log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("leave fan-out failed")
		}
	}
	if !clean {
		s.presence.scheduleExpiry(s.client.ID(), fanout)
		return
	}
	now := time.Now().UnixMilli()
	for channel, members := range s.presence.removeConnection(s.client.ID()) {
		for _, m := range members {
			leave := *m
			leave.Action = protocol.PresenceLeave
			leave.Timestamp = now
			fanout(channel, &leave)
		}
	}
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

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"

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
	// tokenExpires is the connection token's exp (ms since epoch, 0 =
	// never): when it passes without an AUTH renewal, the session sends
	// DISCONNECTED 40142 and drops — the SDK renews and reconnects.
	tokenExpires int64
	// reauth verifies a mid-connection AUTH token (RTC8), returning the
	// refreshed identity. Built by the handler over the same verifier as
	// connect-time auth.
	reauth func(token string) (authResult, *authProblem)
	// recoverError, when set, is carried on the INITIAL CONNECTED frame
	// (TR4o error attribute): the recover claim was rejected — e.g. a
	// malformed connectionKey (80018) — and the connection proceeded
	// fresh. ably-js surfaces it as stateChange.reason and resets
	// msgSerial (RTN16e territory; pinned by unrecoverableConnection).
	recoverError *protocol.ErrorInfo
	// recoverID is the connectionId recovered from the recover query
	// param (RTN16): the client presents its previous connectionKey and
	// the session adopts that identity — CONNECTED echoes the SAME
	// connectionId with a NEW connectionKey (RTN16d). Empty for fresh
	// connections. PoC posture: the claim is not verified against any
	// connection registry (single-node, no connection-state store) —
	// divergence documented for M9.
	recoverID string
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
	// opUnsubscribeSilent tears a subscription down without a DETACHED
	// frame — the channel was already failed on the client (capability
	// downgrade, RTC8a1).
	opUnsubscribeSilent
)

type pendingOp struct {
	kind    opKind
	channel string
	// params/modes carry the ATTACH request context to the ATTACHED reply
	// (RTL4k params echo; RTL4m mode grant).
	params map[string]string
	modes  int64
	// resuming marks an ATTACH that presented a channelSerial cursor which
	// resolved to a broker position: the synthesized subscribe carries
	// Recover, and the reply's recovered publications are replayed after
	// ATTACHED with the RESUMED flag (RTL4j territory).
	resuming bool
	// rewinding marks a fresh ATTACH with a rewind param: the same
	// recovery machinery replays the backlog, but ATTACHED carries
	// HAS_BACKLOG (when non-empty) instead of RESUMED (RTL2i).
	rewinding bool
	// claimResume marks an ATTACH_RESUME attach without a cursor: the
	// client claims prior attachment; ATTACHED grants RESUMED on the
	// claim (RTN16-lite, unverified — see attach).
	claimResume bool
	// rewindSpec, when non-empty, requests a MATERIALIZED rewind on a
	// mutableMessages channel: the backlog is the latest materialized
	// state, never the raw op stream (rewind=1 after create+append must
	// deliver ONE concatenated message — the ably-js append pin).
	rewindSpec string
}

type session struct {
	node         *centrifuge.Node
	mint         *serialMint
	materialized *materializedStore
	conn         frameConn
	params       sessionParams
	presence     *presenceStore

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

	// authMu guards the token-expiry timers: armed at connect, re-armed by
	// a successful AUTH (its handler runs on the frame-reader goroutine,
	// the timers fire on their own goroutines).
	authMu       sync.Mutex
	expiryTimer  *time.Timer
	preAuthTimer *time.Timer

	closeMu sync.Mutex
	closed  bool
	// cleanClose marks a client-initiated CLOSE (RTN12a): presence leaves
	// immediately. Abrupt drops keep members for the grace window.
	cleanClose bool
	closeCh    chan struct{} // closed on teardown: stops the heartbeat ticker and cancels the client context
}

func newSession(node *centrifuge.Node, conn frameConn, params sessionParams, presence *presenceStore, mint *serialMint, materialized *materializedStore) *session {
	return &session{
		node:          node,
		mint:          mint,
		materialized:  materialized,
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
	if err := s.writeConnected(); err != nil {
		return
	}
	// RTN15-territory: a token-authenticated session disconnects with
	// 40142 when the token's exp passes without an AUTH renewal.
	s.armTokenExpiry()

	go s.heartbeatLoop()

	for {
		// The transport owns wire decoding (wsConn is format-tolerant on
		// read per RTN2a; a comet front feeds posted frames one at a
		// time). A read error ends the session.
		m, err := s.conn.readFrame()
		if err != nil {
			return
		}
		if !s.handleFrame(m) {
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
		// RTN13: HEARTBEAT echoes back (the SDK ping). The 1ms floor keeps
		// loopback round-trips out of the same Date.now() millisecond —
		// ably-js asserts responseTime > 0 (connectionPingWithCallback),
		// which any real network satisfies trivially.
		time.Sleep(time.Millisecond)
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
	case protocol.ActionAuth:
		// RTC8: mid-connection token renewal.
		s.handleAuth(m)
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
	// RTL32: a message presenting a serial and a mutation action is an op
	// on an existing message, not a create.
	if len(m.Messages) == 1 && m.Messages[0] != nil &&
		m.Messages[0].Serial != "" && m.Messages[0].Action != protocol.MessageActionCreate {
		s.mutateMessage(m)
		return
	}

	connectionID := s.connectionID()

	// Validation, envelope rules (CD2c/TO3l8 size accounting included) and
	// payload normalization live in the shared publish core (publish.go),
	// reused verbatim by the REST publish surface. The application-level
	// 40009 reject NACKs the frame and keeps the connection alive —
	// contrast the protocol-level maxFrameSize read limit set in
	// serveRealtime, which kills the connection outright.
	// T1.2: the channel publish lock makes mint+append atomic — serial
	// order equals broker offset order (serials.go). The deferred unlock
	// also spans the ACK write: a stalled client can hold the lock for up
	// to writeTimeout, stalling that channel's other publishers — bounded
	// and inversion-free (writeMu is a strict leaf), PoC-acceptable.
	unlock := s.mint.lockChannel(m.Channel)
	defer unlock()
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
		_, err := s.node.Publish(brokerChannel(m.Channel), data, opts...)
		if err != nil {
			log.Error().Err(err).Str("channel", m.Channel).Str("transport", transportName).Msg("publish failed")
			s.writeNack(m.MsgSerial, errCodeInternal, 500, "publish failed")
			return
		}
	}
	if mutableChannel(m.Channel) {
		// The create registers materialized state (mutation ops and
		// materialized reads key off it), and the ACK carries the
		// assigned message serials (TR4s).
		msgSerials := make([]string, len(m.Messages))
		for i, msg := range m.Messages {
			msgSerials[i] = msg.Serial
			s.materialized.create(m.Channel, msg)
		}
		s.writeAckRes(m.MsgSerial, msgSerials)
		return
	}
	s.writeAck(m.MsgSerial)
}

// errCodeMutableRequired (93002, registry-verified): mutation ops are
// only legal on channels with the mutableMessages rule.
const errCodeMutableRequired = 93002

// mutateMessage serves a realtime mutation frame (RTL32): update (1),
// delete (2) or append (5) by serial. The op applies to the materialized
// state, fans out as an op message carrying the original serial and an
// operation version, and ACKs with res serials[0] = versionSerial.
func (s *session) mutateMessage(m *protocol.ProtocolMessage) {
	msg := m.Messages[0]
	if !mutableChannel(m.Channel) {
		s.writeNack(m.MsgSerial, errCodeMutableRequired, 400, "mutation failed: this operation can only be performed on a channel with mutable messages enabled")
		return
	}
	// T1.2: mint+mutate+append atomic per channel (serials.go).
	unlock := s.mint.lockChannel(m.Channel)
	defer unlock()
	// The mutator supplies the MessageOperation in version; the server
	// assigns the versionSerial and timestamp (TM2s).
	version := msg.Version
	if version == nil {
		version = &protocol.MessageVersion{}
	}
	version.Serial = s.mint.Mint(m.Channel)
	version.Timestamp = time.Now().UnixMilli()

	// Binary deltas normalize like creates (canonical JSON-safe envelope).
	normalizeMessageData(msg)

	op, prob := s.materialized.mutate(m.Channel, msg.Serial, msg.Action, msg.Data, msg.Encoding, msg.Extras, version)
	if prob != nil {
		s.writeNack(m.MsgSerial, prob.code, prob.statusCode, "mutation failed: "+prob.message)
		return
	}
	op.ID = fmt.Sprintf("%s:%d:0", s.connectionID(), m.MsgSerial)
	data, err := json.Marshal(op)
	if err != nil {
		s.writeNack(m.MsgSerial, errCodeInternal, 500, "mutation failed")
		return
	}
	// The op rides the live channel like any publication — tagged with
	// its versionSerial (it advances the channel position) and the
	// publisher origin (echo=false suppression applies).
	if _, err := s.node.Publish(brokerChannel(m.Channel), data, publishOptions(m.Channel, s.connectionID(), version.Serial)...); err != nil {
		log.Error().Err(err).Str("channel", m.Channel).Str("transport", transportName).Msg("mutation publish failed")
		s.writeNack(m.MsgSerial, errCodeInternal, 500, "mutation failed")
		return
	}
	// TR4s: the mutation ACK returns the versionSerial.
	s.writeAckRes(m.MsgSerial, []string{version.Serial})
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
	// Res carries one PublishResult per acknowledged frame (TR4s) on
	// mutableMessages channels: serials 1:1 with the frame's messages —
	// ably-js completes each pending publish with res[i]
	// (messagequeue.ts) and AIT hard-fails without serials[0].
	Res []publishResult `json:"res,omitempty" msgpack:"res,omitempty"`
}

// publishResult is the TR4s/RSL1n PublishResult document.
type publishResult struct {
	Serials []string `json:"serials" msgpack:"serials"`
}

// writeConnected sends the CONNECTED frame (RTN6 at connect; RTC8a-ack
// on a successful AUTH — same connectionId, same key: an update, not a
// new connection).
func (s *session) writeConnected() error {
	detailsClientID := s.params.clientID
	if s.params.wildcardClientID {
		// RSA15b: a wildcard-token connection that assumed no identity
		// advertises the literal "*" — the SDK knows it may publish on
		// behalf of any clientId. Messages stay unstamped.
		detailsClientID = "*"
	}
	connectionID := s.connectionID()
	// The recover-rejection error rides only the FIRST CONNECTED (an
	// AUTH-ack CONNECTED must not re-assert it). Frame-reader-goroutine
	// state, like the rest of params.
	connectErr := s.params.recoverError
	s.params.recoverError = nil
	return s.writeFrame(&protocol.ProtocolMessage{
		Action:       protocol.ActionConnected,
		ConnectionID: connectionID,
		Error:        connectErr,
		ConnectionDetails: &protocol.ConnectionDetails{ // TR4o, CD1
			ClientID: detailsClientID, // CD2a
			// CD2b: "<connectionId>!<token>". The token is the per-session
			// centrifuge id, so a recovered connection keeps its
			// connectionId but gets a FRESH key (RTN16d asserts the key
			// changes across recovery). REST TM2h attribution strips at
			// the first '!'.
			ConnectionKey:      connectionID + "!" + s.client.ID(),
			MaxMessageSize:     maxMessageSize,     // CD2c
			MaxFrameSize:       maxFrameSize,       // CD2d
			MaxInboundRate:     maxInboundRate,     // CD2e
			ConnectionStateTTL: connectionStateTTL, // CD2f
			MaxIdleInterval:    maxIdleIntervalMS,  // CD2h
		},
	})
}

// armTokenExpiry (re)arms the disconnect-at-token-expiry timer from
// params.tokenExpires. RTN15-territory: when the token expires without
// renewal the server sends DISCONNECTED 40142 and drops the transport —
// the SDK renews via its authCallback/authUrl and reconnects. A
// successful AUTH re-arms with the new exp (or disarms it for an exp-less
// token).
// serverAuthLeadTime is how far before token expiry the server sends a
// client-bound AUTH asking for renewal (RTN22) — matching the Ably
// service's ~30s lead. Tokens shorter-lived than the lead get no warning,
// only the 40142 disconnect at expiry (the renew-on-disconnect path).
const serverAuthLeadTime = 30 * time.Second

func (s *session) armTokenExpiry() {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if s.preAuthTimer != nil {
		s.preAuthTimer.Stop()
		s.preAuthTimer = nil
	}
	if s.params.tokenExpires == 0 {
		return
	}
	wait := time.Until(time.UnixMilli(s.params.tokenExpires))
	if wait < 0 {
		wait = 0
	}
	if wait > serverAuthLeadTime {
		// RTN22: a server-initiated AUTH tells the client to renew now;
		// the client answers with an AUTH carrying the fresh token
		// (handled by handleAuth → CONNECTED update, which re-arms both
		// timers from the new exp).
		s.preAuthTimer = time.AfterFunc(wait-serverAuthLeadTime, func() {
			_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionAuth})
		})
	}
	s.expiryTimer = time.AfterFunc(wait, func() {
		s.disconnectWithError(40142, 401, "token expired")
	})
}

// handleAuth serves a mid-connection AUTH frame (RTC8): the presented
// token is verified by the same path as connect-time auth; success
// adopts the refreshed capability/expiry and acknowledges with CONNECTED
// (an update — same connectionId and key); failure fails the CONNECTION
// with an ERROR frame (no channel attribute), the RTC8a3 posture.
// Capability and identity writes are frame-reader-goroutine-local, the
// same goroutine every enforcement read runs on.
func (s *session) handleAuth(m *protocol.ProtocolMessage) {
	if s.params.reauth == nil || m.Auth == nil || m.Auth.AccessToken == "" {
		s.writeConnectionError(errCodeBadRequest, 400, "auth failed: no token presented")
		return
	}
	res, prob := s.params.reauth(m.Auth.AccessToken)
	if prob != nil {
		s.writeConnectionError(prob.code, prob.statusCode, "auth failed: "+prob.message)
		return
	}
	// RTC8a1-lite: an identified connection cannot assume a different
	// identity mid-flight; a wildcard token keeps the bound identity.
	if res.clientID != "" && s.params.clientID != "" && res.clientID != s.params.clientID {
		s.writeConnectionError(errCodeIncompatibleCredentials, 401, "auth failed: token clientId incompatible with connection clientId")
		return
	}
	s.params.capability = res.capability
	s.params.tokenExpires = res.expires
	if s.params.clientID == "" && res.clientID != "" {
		// An unidentified connection may adopt the renewed token's
		// identity (RTC8a2-adjacent).
		s.params.clientID = res.clientID
		s.params.wildcardClientID = false
	}
	if s.params.clientID == "" {
		// RSA7b4 advertisement tracks the CURRENT token: wildcard only
		// while the renewed token grants it and no identity was assumed.
		s.params.wildcardClientID = res.wildcardClientID
	}
	s.armTokenExpiry()
	_ = s.writeConnected()

	// RTC8a1 downgrade: attachments the renewed capability no longer
	// permits fail with 40160 (the channel transitions to FAILED
	// client-side) and their subscriptions are torn down silently — the
	// client will not re-attach a FAILED channel, and a fresh ATTACH
	// re-runs the normal capability gate. attachedModes is
	// frame-reader-goroutine-local apart from the mutex-guarded reads.
	s.modesMu.Lock()
	var revokedChannels []string
	for channel := range s.attachedModes {
		if !s.params.capability.Allows(auth.OpSubscribe, channel) {
			revokedChannels = append(revokedChannels, channel)
			delete(s.attachedModes, channel)
		}
	}
	s.modesMu.Unlock()
	for _, channel := range revokedChannels {
		s.writeChannelError(channel, errCodeOperationNotPermitted, 401, "channel capability revoked by reauth")
		cmd := &cproto.Command{
			Id:          s.addPending(pendingOp{kind: opUnsubscribeSilent, channel: channel}),
			Unsubscribe: &cproto.UnsubscribeRequest{Channel: brokerChannel(channel)},
		}
		if !s.client.HandleCommand(cmd, cmd.SizeVT()) {
			_, _ = s.takePending(cmd.Id)
		}
	}
}

// disconnectWithError drops the session with a DISCONNECTED frame
// carrying the error — the SDK's renew-and-reconnect signal family
// (40142 token expired, 40141 token revoked). Mirrors the token-expiry
// timer's pattern; safe from any goroutine (writeFrame is
// writeMu-serialized, beginClose is idempotent).
func (s *session) disconnectWithError(code int, statusCode int, message string) {
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action: protocol.ActionDisconnected,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	})
	s.teardown()
}

// writeConnectionError fails the whole connection with an ERROR frame
// carrying NO channel attribute (RTN14g territory: ably-js routes a
// channel-less ERROR to the connection).
func (s *session) writeConnectionError(code int, statusCode int, message string) {
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	})
	s.beginClose()
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

// writeAckRes confirms one inbound MESSAGE frame on a mutableMessages
// channel, carrying the assigned serials (TR4s): one PublishResult for
// the one acknowledged frame, serials 1:1 with its messages.
func (s *session) writeAckRes(msgSerial int64, serials []string) {
	s.writeWire(&ackFrame{
		Action:    protocol.ActionAck,
		MsgSerial: msgSerial,
		Count:     1,
		Res:       []publishResult{{Serials: serials}},
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

// connectionID is the connection identity advertised to the client and
// stamped on everything it publishes: the recovered id when the
// connection presented a recover key (RTN16d), the per-session
// centrifuge client id otherwise. The echo=false subscription filter
// uses the same value as the publication origin tag, so echo
// suppression survives recovery.
func (s *session) connectionID() string {
	if s.params.recoverID != "" {
		return s.params.recoverID
	}
	return s.client.ID()
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

// resolveCursor maps a client-presented channelSerial cursor to the
// broker position of the publication that carried it — by LOOKUP over
// the publication tags, never lexicographic comparison (mint and append
// are not atomic; see serials.go). A cursor that no longer resolves
// (expired from the retention window, or a serial only seen on a
// history-free live presence event) loses continuity: the attach
// proceeds fresh, without RESUMED — exactly how Ably signals an
// unbridgeable gap.
func (s *session) resolveCursor(channel, cursor string) (centrifuge.StreamPosition, bool) {
	res, err := s.node.History(brokerChannel(channel),
		centrifuge.WithLimit(persistedHistorySize), centrifuge.WithReverse(true))
	if err != nil {
		return centrifuge.StreamPosition{}, false
	}
	for _, pub := range res.Publications {
		if pub.Tags[pubTagSerial] == cursor {
			return centrifuge.StreamPosition{Offset: pub.Offset, Epoch: res.StreamPosition.Epoch}, true
		}
	}
	return centrifuge.StreamPosition{}, false
}

// rewindPosition resolves a rewind request to the broker position to
// recover from: "N" replays the last N retained publications, "Ns"
// (or any Go-parsable duration) replays publications whose envelope
// timestamp falls inside the trailing window. Returns false when there
// is nothing to replay (empty channel, zero/invalid spec) — the attach
// proceeds fresh without HAS_BACKLOG (rewind_has_backlog_0).
func (s *session) rewindPosition(channel, spec string) (centrifuge.StreamPosition, bool) {
	var pubs []*centrifuge.Publication
	var top centrifuge.StreamPosition
	if n, err := strconv.Atoi(spec); err == nil {
		if n <= 0 {
			return centrifuge.StreamPosition{}, false
		}
		if n > persistedHistorySize {
			n = persistedHistorySize
		}
		res, err := s.node.History(brokerChannel(channel), centrifuge.WithLimit(n), centrifuge.WithReverse(true))
		if err != nil || len(res.Publications) == 0 {
			return centrifuge.StreamPosition{}, false
		}
		pubs, top = res.Publications, res.StreamPosition
	} else if d, err := time.ParseDuration(spec); err == nil && d > 0 {
		res, err := s.node.History(brokerChannel(channel),
			centrifuge.WithLimit(persistedHistorySize), centrifuge.WithReverse(true))
		if err != nil || len(res.Publications) == 0 {
			return centrifuge.StreamPosition{}, false
		}
		cutoff := time.Now().Add(-d).UnixMilli()
		// Publications are newest-first; keep the contiguous head whose
		// envelope timestamps fall inside the window.
		var inWindow []*centrifuge.Publication
		for _, pub := range res.Publications {
			var msg protocol.Message
			if err := json.Unmarshal(pub.Data, &msg); err != nil || msg.Timestamp < cutoff {
				break
			}
			inWindow = append(inWindow, pub)
		}
		if len(inWindow) == 0 {
			return centrifuge.StreamPosition{}, false
		}
		pubs, top = inWindow, res.StreamPosition
	} else {
		return centrifuge.StreamPosition{}, false
	}
	// Reverse page: the OLDEST publication to replay is the last entry;
	// recovery replays everything after the position, so recover from
	// just before it.
	oldest := pubs[len(pubs)-1]
	return centrifuge.StreamPosition{Offset: oldest.Offset - 1, Epoch: top.Epoch}, true
}

// latestChannelSerial returns the serial of the channel's most recent
// publication — the attach point ATTACHED advertises (RTL15a
// attachSerial) — or "" for a channel with no retained publications.
func (s *session) latestChannelSerial(channel string) string {
	res, err := s.node.History(brokerChannel(channel), centrifuge.WithLimit(1), centrifuge.WithReverse(true))
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

// writeAttached records the attachment grant and confirms it on the wire:
// ATTACHED echoing the recognized params (RTL4k1) and the granted mode bits
// (RTL4m), HAS_PRESENCE + a single-page SYNC when members exist (RTP18-lite
// — members delivered as PRESENT; no channelSerial cursor means ably-js
// setPresence completes the sync on this frame). Called from the
// opSubscribe reply (fresh attach, centrifuge reply goroutine) and from
// the re-attach update path (frame-reader goroutine). On a resumed
// attach, presence events that occurred inside the gap are NOT replayed
// (live presence publications are history-free): current presence state
// arrives via HAS_PRESENCE + SYNC here instead — matching Ably, which
// re-syncs presence on resume rather than replaying it.
func (s *session) writeAttached(channel string, params map[string]string, modes int64, extraFlags int64) {
	// An unrestricted request is granted the full default mode set —
	// ATTACHED always carries mode bits.
	if modes == 0 {
		modes = defaultChannelModes
	}
	var members []*protocol.PresenceMessage
	if modeAllows(modes, protocol.FlagModePresenceSubscribe) {
		members = s.presence.members(channel)
	}
	flags := extraFlags
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
		// RTP4: large presence sets page at 100 members per SYNC. Pages
		// in a sequence carry channelSerial "<sequence>:<cursor>"; the
		// final page's empty cursor ("presence:") ends the sync. A
		// single-page sync omits channelSerial entirely (RTP18-lite, as
		// before) — both forms complete ably-js's setPresence. The paging
		// matters behaviorally: SDK presence.get() callers observe
		// liveness during a paged sync (presence events interleaved
		// between pages apply mid-sync — pinned by ably-js
		// presence_sync_interruptus).
		const syncPageSize = 100
		if len(snapshot) <= syncPageSize {
			_ = s.writeFrame(&protocol.ProtocolMessage{
				Action:   protocol.ActionSync,
				Channel:  channel,
				Presence: snapshot,
			})
		} else {
			for start := 0; start < len(snapshot); start += syncPageSize {
				end := min(start+syncPageSize, len(snapshot))
				cursor := ""
				if end < len(snapshot) {
					cursor = strconv.Itoa(end)
				}
				_ = s.writeFrame(&protocol.ProtocolMessage{
					Action:        protocol.ActionSync,
					Channel:       channel,
					ChannelSerial: "presence:" + cursor,
					Presence:      snapshot[start:end],
				})
			}
		}
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
		// Options update on a live attachment: continuity was never broken,
		// which RTL12 semantics signal with RESUMED. A rewind param
		// arriving via setOptions on a MUTABLE channel still serves the
		// materialized backlog (ably-js applies setOptions({rewind})
		// through this re-attach — the append pin's second phase).
		extra := protocol.FlagResumed
		var backlog []protocol.Message
		if mutableChannel(channel) && m.Params["rewind"] != "" &&
			m.ChannelSerial == "" && m.Flags&protocol.FlagAttachResume == 0 {
			backlog = s.materializedRewind(channel, m.Params["rewind"])
			if len(backlog) > 0 {
				extra |= protocol.FlagHasBacklog
			}
		}
		s.writeAttached(channel, m.Params, modesFromAttach(m), extra)
		for i := range backlog {
			s.deliverMaterialized(channel, &backlog[i])
		}
		return
	}
	sub := &cproto.SubscribeRequest{Channel: brokerChannel(channel)}
	// RTL4j territory: an ATTACH presenting a channelSerial cursor asks to
	// resume from that position. When the cursor resolves to a retained
	// publication, the synthesized subscribe requests centrifuge recovery
	// from its offset — the broker replays the gap atomically with the
	// subscription (no read-then-subscribe race). An unresolvable cursor
	// attaches fresh (no RESUMED flag = discontinuity, the Ably signal).
	resuming := false
	rewinding := false
	rewindSpec := ""
	// RTN16-lite: an ATTACH carrying ATTACH_RESUME — or any cursor-less
	// ATTACH on a RECOVERED connection (ably-js re-attaches recovered
	// channels bare when they had no channelSerial; real Ably derives
	// RESUMED from the recovered connection's channel state) — claims a
	// prior attachment. Without a cursor there is no gap to bridge or
	// verify, so the PoC grants continuity (RESUMED) on the claim alone
	// (divergence noted for M9: a genuinely-new channel attached after
	// recovery is also granted RESUMED). With a cursor, the resolved
	// recovery below decides RESUMED honestly.
	claimResume := m.ChannelSerial == "" &&
		(m.Flags&protocol.FlagAttachResume != 0 || s.params.recoverID != "")
	switch {
	case m.ChannelSerial != "":
		if pos, ok := s.resolveCursor(channel, m.ChannelSerial); ok {
			// Recover asks for replay from the position NOW; Recoverable is
			// the separate declarative property centrifugo's permission
			// chain checks (chOpts.AllowRecovery gates e.Recoverable —
			// internal/client/handler.go) before the library honors
			// Recover. Both are required.
			sub.Recover = true
			sub.Recoverable = true
			sub.Offset = pos.Offset
			sub.Epoch = pos.Epoch
			resuming = true
		}
	case m.Params["rewind"] != "" && m.Flags&protocol.FlagAttachResume == 0 && s.params.recoverID == "":
		// RTL2i: rewind replays a backlog of retained messages on a FRESH
		// attach only — an ATTACH_RESUME attach (or one presenting a
		// cursor, above) suppresses it (pinned by resume_rewind_1).
		if mutableChannel(channel) {
			// Mutable channels rewind MATERIALIZED state (handled in the
			// subscribe reply), not the raw op stream.
			rewindSpec = m.Params["rewind"]
		} else if pos, ok := s.rewindPosition(channel, m.Params["rewind"]); ok {
			// Ordinary channels ride broker recovery, atomic with the
			// subscription.
			sub.Recover = true
			sub.Recoverable = true
			sub.Offset = pos.Offset
			sub.Epoch = pos.Epoch
			rewinding = true
		}
	}
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
		sub.Tf = &cproto.FilterNode{Cmp: "neq", Key: pubTagOrigin, Val: s.connectionID()}
	}
	cmd := &cproto.Command{
		Id: s.addPending(pendingOp{
			kind:        opSubscribe,
			channel:     channel,
			params:      m.Params,
			modes:       modesFromAttach(m),
			resuming:    resuming,
			rewinding:   rewinding,
			claimResume: claimResume,
			rewindSpec:  rewindSpec,
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
		Unsubscribe: &cproto.UnsubscribeRequest{Channel: brokerChannel(channel)},
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
	connectionID := s.connectionID()
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
	// T1.2: mint+append atomic per channel (serials.go).
	unlock := mint.lockChannel(channel)
	defer unlock()
	// RTL15b: presence events advance the channel position too — the
	// serial is drawn from the same per-channel sequence as messages.
	cs := mint.Mint(channel)
	if _, err = node.Publish(brokerChannel(channel), data,
		centrifuge.WithTags(map[string]string{pubTagKind: pubTagKindPresence, pubTagSerial: cs})); err != nil {
		return err
	}
	// Presence history lives on the client-unreachable shadow channel with
	// the live channel's retention tier — the live publication above stays
	// history-free so message history is never polluted.
	historyOpts := publishOptions(channel, "", cs)
	_, err = node.Publish(brokerChannel(presenceHistoryChannel(channel)), data, historyOpts...)
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
			// RTL4j: a resumed attach replays the gap after ATTACHED. The
			// RESUMED flag is only set when the broker confirmed recovery
			// (RTL12: its absence signals a discontinuity). A rewind attach
			// rides the same recovery but signals HAS_BACKLOG instead
			// (RTL2i; never RESUMED — rewind is a fresh attach).
			var extraFlags int64
			var replay []*cproto.Publication
			var materialized []protocol.Message
			if op.rewindSpec != "" {
				materialized = s.materializedRewind(op.channel, op.rewindSpec)
				if len(materialized) > 0 {
					extraFlags |= protocol.FlagHasBacklog
				}
			}
			if op.claimResume {
				extraFlags |= protocol.FlagResumed
			}
			if reply.Subscribe != nil && reply.Subscribe.Recovered {
				switch {
				case op.resuming:
					extraFlags |= protocol.FlagResumed
					replay = reply.Subscribe.Publications
				case op.rewinding:
					replay = reply.Subscribe.Publications
					if len(replay) > 0 {
						extraFlags |= protocol.FlagHasBacklog
					}
				}
			}
			s.writeAttached(op.channel, op.params, op.modes, extraFlags)
			for _, pub := range replay {
				s.deliverPublication(op.channel, pub)
			}
			for i := range materialized {
				s.deliverMaterialized(op.channel, &materialized[i])
			}
		case opUnsubscribeSilent:
			// Teardown after a capability downgrade: the ERROR already
			// failed the channel; nothing more goes on the wire.
			if reply.Error != nil {
				log.Warn().Str("channel", op.channel).Uint32("code", reply.Error.Code).Str("transport", transportName).Msg("silent unsubscribe error")
			}
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
			// Push.Channel is the BROKER name; everything downstream
			// (frames, store lookups, mode filters) speaks Ably names.
			s.deliverPublication(ablyChannel(reply.Push.Channel), reply.Push.Pub)
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

// materializedRewind resolves a mutable-channel rewind spec against the
// materialized store: "N" = the last N messages, a duration = the
// trailing window by create-anchored timestamp (AIT attaches with
// rewind='2m').
func (s *session) materializedRewind(channel, spec string) []protocol.Message {
	if n, err := strconv.Atoi(spec); err == nil {
		return s.materialized.latest(channel, n)
	}
	if d, err := time.ParseDuration(spec); err == nil && d > 0 {
		return s.materialized.latestWindow(channel, time.Now().Add(-d).UnixMilli())
	}
	return nil
}

// deliverMaterialized writes one materialized message as a MESSAGE frame
// — the mutable-channel rewind backlog. The frame channelSerial is the
// message's latest position: the last operation's versionSerial, or the
// create position for never-mutated messages — both are publication tag
// values a future resume cursor can resolve.
func (s *session) deliverMaterialized(channel string, msg *protocol.Message) {
	cursor := ""
	if msg.Version != nil && msg.Version.Serial != "" {
		cursor = msg.Version.Serial
	} else if cs, _, err := serial.ParseMessageSerial(msg.Serial); err == nil {
		cursor = cs
	}
	out := *msg
	if s.params.format == protocol.FormatMsgpack {
		denormalizeMessageData(&out)
	}
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action:        protocol.ActionMessage,
		Channel:       channel,
		ChannelSerial: cursor,
		Messages:      []*protocol.Message{&out},
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
	_ = s.conn.close()
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
func (s *session) stopTokenExpiry() {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if s.preAuthTimer != nil {
		s.preAuthTimer.Stop()
		s.preAuthTimer = nil
	}
}

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
	s.stopTokenExpiry()
	if s.closeFn != nil {
		_ = s.closeFn()
	}
	_ = s.conn.close()
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
		s.presence.scheduleExpiry(s.connectionID(), fanout)
		return
	}
	now := time.Now().UnixMilli()
	for channel, members := range s.presence.removeConnection(s.connectionID()) {
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

// writeBytes hands one encoded frame to the transport under writeMu —
// the single ordering point for every outbound frame. The transport
// maps the session's wire format to its own framing (WS: text vs
// binary message type).
func (s *session) writeBytes(data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.writeEncoded(data, s.params.format == protocol.FormatMsgpack)
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

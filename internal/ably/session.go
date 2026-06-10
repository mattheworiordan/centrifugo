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
// Frame flow served this milestone (publish/MESSAGE lands in M1.2):
//
//	WS upgrade   → CONNECTED(4) + connectionDetails (RTN6, TR4o, CD1)
//	HEARTBEAT(0) → HEARTBEAT(0) echo                (RTN13a)
//	ATTACH(10)   → ATTACHED(11)                     (RTL4c)
//	DETACH(12)   → DETACHED(13)                     (RTL5d)
//	CLOSE(7)     → CLOSED(8) + connection close     (RTN12a)
//
// plus a server heartbeat ticker backing the advertised maxIdleInterval
// (RTN23a).

import (
	"context"
	"errors"
	"fmt"
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

// sessionParams carries the per-connection parameters extracted from the
// upgrade request querystring (RTN2) and authentication.
type sessionParams struct {
	// userID becomes the centrifuge Credentials UserID: the clientId query
	// param (RTN2d) when present, the authenticated key name otherwise.
	userID string
	// clientID is the clientId query param (RTN2d), echoed back in
	// connectionDetails.clientId (CD2a) when set.
	clientID string
	// echo is the echo query param (RTN2b). Stored now; honored when
	// MESSAGE fan-out lands (M1.2/M1.3).
	echo bool
	// protocolVersion is the v query param (RTN2f). Stored, not yet acted
	// on.
	protocolVersion string
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
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		var m protocol.ProtocolMessage
		// M1.1 serves the json wire format only; msgpack framing is M2.
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
			log.Warn().Err(err).Str("transport", transportName).Msg("bad inbound frame")
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
	case protocol.ActionClose:
		// RTN12a: confirm the close request with CLOSED, then drop the
		// connection.
		_ = s.writeFrame(&protocol.ProtocolMessage{Action: protocol.ActionClosed})
		return false
	default:
		// MESSAGE (publish/ACK) handling lands in M1.2; PRESENCE, SYNC and
		// AUTH in later milestones.
		log.Warn().Int("action", int(m.Action)).Str("transport", transportName).Msg("unhandled inbound action")
		return true
	}
}

func (s *session) attach(channel string) {
	cmd := &cproto.Command{
		Id:        s.addPending(pendingOp{kind: opSubscribe, channel: channel}),
		Subscribe: &cproto.SubscribeRequest{Channel: channel},
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
		// Publication delivery (push.Pub → MESSAGE) lands in M1.2;
		// Join/Leave (presence, M5), Unsubscribe and Refresh pushes are
		// translated in later milestones. Until then pushes are dropped.
		log.Debug().Str("channel", reply.Push.Channel).Str("transport", transportName).Msg("push dropped (translation lands in a later milestone)")
		return
	}
	// An empty reply is a centrifuge server ping — disabled via
	// PingPongConfig{PingInterval: -1}; Ably liveness is the HEARTBEAT
	// ticker.
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
	_ = s.writeFrame(&protocol.ProtocolMessage{
		Action:  protocol.ActionError,
		Channel: channel,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    fmt.Sprintf("attach failed: %s", detail),
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

// writeFrame marshals and writes one ProtocolMessage as a WS text frame.
func (s *session) writeFrame(m *protocol.ProtocolMessage) error {
	data, err := protocol.Marshal(m, protocol.FormatJSON)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return s.conn.WriteMessage(websocket.TextMessage, data)
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

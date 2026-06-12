package ably

// The comet/HTTP-fallback transport front (RTN1 fallback family):
// ably-js's NodeCometTransport (streaming) and XHRPollingTransport
// (long-poll) speak a pair of HTTP request lifecycles against /comet/:
//
//	GET  /comet/connect?<auth+RTN2 params>  → JSON array, first element
//	     CONNECTED (the connect request IS the first recv)
//	POST /comet/<connectionKey>/send        → enqueue frames, 204 (C3)
//	GET  /comet/<connectionKey>/recv        → long-poll: next frame batch
//	GET  /comet/<connectionKey>/close|disconnect (C3)
//
// Implemented from the SDK contract (research/09-comet-contract.md) the
// way the production service models it: poll-only — a streaming-mode
// client treats a complete `[...]\n` body + connection end as one chunk
// and immediately re-issues recv, so long-poll responses satisfy both
// client modes. The session machinery is untouched: a comet session is
// the ordinary session driven through a cometConn instead of a wsConn.
//
// Comet is JSON-only by SDK design (comettransport.ts forceJsonProtocol:
// even useBinaryProtocol clients speak JSON over comet and send no
// format param), so a comet session's format is always FormatJSON.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

const (
	// cometReapAfter is the abandonment window: a comet session with no
	// recv parked and no request activity for this long is torn down
	// (the long-poll equivalent of a dead socket; presence grace then
	// runs exactly as for an abrupt WS drop). Production uses 10s; the
	// PoC is lenient — 2× the advertised maxIdleInterval — to survive
	// browser background-tab throttling.
	cometReapAfter = 2 * maxIdleIntervalMS * time.Millisecond
	// cometConnectFlush bounds how long /comet/connect waits for the
	// session's first frames; the SDK abandons the attempt at its 10s
	// realtimeRequestTimeout, so answer before that.
	cometConnectFlush = 8 * time.Second
	// cometCloseInjectTimeout bounds how long /close|/disconnect spends
	// injecting its terminal frame before answering 2xx (B5/M6). The
	// handler never waits for teardown to complete — the SDK reads no body
	// — so a client spamming /close on a wedged session cannot tie up
	// goroutines.
	cometCloseInjectTimeout = 1 * time.Second
)

// cometClosedError ends the session read loop when the comet transport is
// closed (reaper, close/disconnect, or teardown).
type cometClosedError struct{}

func (cometClosedError) Error() string { return "comet transport closed" }

// cometConn implements frameConn over HTTP request lifecycles. Outbound
// frames buffer until a recv collects them (single-flight: at most one
// recv parks; a newcomer supersedes the parked one, which completes
// with an empty batch — the production contract). Inbound frames are
// fed by /send (C3) and read by the session's run loop, preserving the
// single-dispatcher invariant that keeps reauth state goroutine-local.
type cometConn struct {
	mu        sync.Mutex
	buf       [][]byte      // pre-encoded outbound frames awaiting a recv
	waiter    chan [][]byte // the parked recv's completion channel (buffered 1)
	closed    bool
	reaper    *time.Timer
	reapAfter time.Duration // abandonment window (per-conn so tests can shorten it)

	inbound chan *protocol.ProtocolMessage
	closeCh chan struct{}
}

func newCometConn() *cometConn { return newCometConnReap(cometReapAfter) }

// newCometConnReap builds a cometConn with an explicit abandonment window —
// the default for production, a short one for reaper tests.
func newCometConnReap(reapAfter time.Duration) *cometConn {
	c := &cometConn{
		inbound:   make(chan *protocol.ProtocolMessage, 16),
		closeCh:   make(chan struct{}),
		reapAfter: reapAfter,
	}
	// Armed from birth: a client that connects and never polls is
	// abandoned. parkRecv suspends it (the production recvTimeout
	// discipline: the clock never runs while a poll is parked).
	c.reaper = time.AfterFunc(reapAfter, func() { _ = c.close() })
	return c
}

func (c *cometConn) readFrame() (*protocol.ProtocolMessage, error) {
	select {
	case m := <-c.inbound:
		return m, nil
	case <-c.closeCh:
		return nil, cometClosedError{}
	}
}

// feed hands one client frame to the session's read loop (used by
// /send and the close/disconnect injection in C3).
func (c *cometConn) feed(ctx context.Context, m *protocol.ProtocolMessage) error {
	select {
	case c.inbound <- m:
		return nil
	case <-c.closeCh:
		return cometClosedError{}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// writeEncoded buffers one outbound frame and completes a parked recv.
// Called under the session's writeMu (frameConn contract), so buffer
// order is frame order.
func (c *cometConn) writeEncoded(encoded []byte, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return cometClosedError{}
	}
	c.buf = append(c.buf, encoded)
	c.flushLocked()
	return nil
}

// flushLocked hands the whole buffer to the parked recv, if any.
func (c *cometConn) flushLocked() {
	if c.waiter == nil || len(c.buf) == 0 {
		return
	}
	c.waiter <- c.buf // buffered(1): never blocks under mu
	c.waiter = nil
	c.buf = nil
}

// parkRecv is the long-poll rendezvous: it returns immediately with any
// buffered frames, otherwise parks until a frame arrives (the session
// heartbeat ticker guarantees ≤10s, inside the advertised 15s
// maxIdleInterval), the transport closes (remaining frames — including
// the terminal DISCONNECTED/CLOSED written before close — are
// delivered), a newer recv supersedes this one (empty batch), or the
// client abandons the request.
func (c *cometConn) parkRecv(ctx context.Context) [][]byte {
	c.mu.Lock()
	// Entering a poll suspends the abandonment clock (production
	// semantics: a parked poll may outlive recvTimeout indefinitely).
	c.reaper.Stop()
	defer func() {
		c.mu.Lock()
		// B6: re-arm the reaper ONLY when no poll is parked. When this recv
		// was superseded by a successor (c.waiter now holds the successor's
		// channel), re-arming would run the abandonment clock while a poll is
		// actively parked — contradicting "the clock never runs while
		// parked" and risking a spurious reap of a continuously-polling
		// client. The successor's own parkRecv already stopped the clock on
		// entry and will re-arm when IT returns to an idle state.
		if !c.closed && c.waiter == nil {
			c.reaper.Reset(c.reapAfter)
		}
		c.mu.Unlock()
	}()

	if len(c.buf) > 0 {
		frames := c.buf
		c.buf = nil
		c.mu.Unlock()
		return frames
	}
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if c.waiter != nil {
		// Single-flight: the older parked recv completes empty.
		c.waiter <- nil
	}
	ch := make(chan [][]byte, 1)
	c.waiter = ch
	c.mu.Unlock()

	select {
	case frames := <-ch:
		return frames
	case <-ctx.Done():
		c.mu.Lock()
		if c.waiter == ch {
			c.waiter = nil
		}
		c.mu.Unlock()
		// A frame may have raced the cancellation; deliver rather than
		// drop it (the response writer may still succeed — if not, the
		// frames are lost exactly as a dying WS write would be).
		select {
		case frames := <-ch:
			return frames
		default:
			return nil
		}
	}
}

// close is idempotent: it ends the session read loop, completes a
// parked recv with whatever the buffer holds (terminal frames are
// written BEFORE close on every session path, so the final poll carries
// DISCONNECTED/CLOSED), and stops the reaper.
func (c *cometConn) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.reaper.Stop()
	close(c.closeCh)
	if c.waiter != nil {
		c.waiter <- c.buf
		c.waiter = nil
		c.buf = nil
	}
	return nil
}

// --- HTTP front ---

// serveComet routes the /comet/* family: /comet/connect establishes a
// session; the per-key paths (recv/send/close/disconnect) address it
// between requests via the registry's connectionKey index.
func (h *Handler) serveComet(rw http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/comet/")
	if rest == "connect" {
		h.serveCometConnect(rw, r)
		return
	}
	key, op, ok := strings.Cut(rest, "/")
	if !ok || key == "" {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "Could not find path: "+r.URL.Path)
		return
	}
	switch op {
	case "recv":
		h.serveCometRecv(rw, r, key)
	case "send":
		h.serveCometSend(rw, r, key)
	case "close":
		h.serveCometClose(rw, r, key, protocol.ActionClose)
	case "disconnect":
		h.serveCometClose(rw, r, key, protocol.ActionDisconnect)
	default:
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "Could not find path: "+r.URL.Path)
	}
}

// serveCometSend implements POST /comet/<key>/send: the client→server
// half of the transport. The body is a JSON ARRAY of ProtocolMessages
// (the SDK batches; one in-flight send at a time). Frames are fed IN
// ORDER to the session's inbound channel — the run loop stays the sole
// frame dispatcher, exactly as for WS — and the response is always 204:
// ACKs/NACKs and every other server frame ride the recv channel (the
// SDK feeds a nonempty send response through the same path, but 204 is
// the simpler contract the production service settled on).
func (h *Handler) serveCometSend(rw http.ResponseWriter, r *http.Request, key string) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	// The WS read limit's comet analogue (CD2d): cap the send body. Read
	// before the session lookup so a forwarded send carries the same capped
	// body (the foreign node enforces the limit; the owning node parses).
	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, maxFrameSize))
	if err != nil {
		h.writeError(rw, r, http.StatusRequestEntityTooLarge, 40000, "send body too large")
		return
	}
	// P6.4: a key owned by another LIVE node is forwarded, not 410'd.
	if h.registry.lookupKey(key) == nil {
		if nodeID, ok := h.cometForwardTarget(key); ok {
			code, _ := h.forwardComet(r.Context(), cometSurveySendOp, nodeID, cometForwardReq{
				Key: key, KeyName: identity.keyName, ClientID: identity.clientID, Body: body,
			}, h.forwardOpTimeout)
			switch code {
			case cometForwardOK:
				rw.WriteHeader(http.StatusNoContent)
			case cometForwardBadRequest:
				h.writeError(rw, r, http.StatusBadRequest, 40000, "invalid send body: expected a JSON array of protocol messages")
			default:
				h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
			}
			return
		}
	}
	_, cc, ok := h.cometSession(rw, r, key, identity)
	if !ok {
		return
	}
	var frames []*protocol.ProtocolMessage
	if err := json.Unmarshal(body, &frames); err != nil {
		h.writeError(rw, r, http.StatusBadRequest, 40000, "invalid send body: expected a JSON array of protocol messages")
		return
	}
	for _, m := range frames {
		if m == nil {
			continue
		}
		if err := cc.feed(r.Context(), m); err != nil {
			// The session died mid-batch: the client's next request gets
			// the terminal frame from recv or a 410 — same as production.
			h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
			return
		}
	}
	rw.WriteHeader(http.StatusNoContent)
}

// serveCometClose implements GET /comet/<key>/close (clean close: the
// session replies CLOSED and leaves presence immediately) and
// /disconnect (transport disposal: presence grace runs, the client may
// resume). Either action is injected as an inbound frame so the run
// loop processes it with full ordering guarantees, then the response
// waits briefly for teardown — the client treats any 2xx as done and
// never reads a body.
func (h *Handler) serveCometClose(rw http.ResponseWriter, r *http.Request, key string, action protocol.Action) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	sess := h.registry.lookupKey(key)
	if sess == nil {
		// P6.4: a key owned by another LIVE node is forwarded best-effort —
		// the response is 204 either way (closing a dead transport is
		// success, not error: the SDK calls disconnect on dispose paths
		// where the server may have reaped first, and it never reads the
		// body of a 2xx).
		if nodeID, ok := h.cometForwardTarget(key); ok {
			_, _ = h.forwardComet(r.Context(), cometSurveyCloseOp, nodeID, cometForwardReq{
				Key: key, KeyName: identity.keyName, ClientID: identity.clientID, Action: int(action),
			}, h.forwardOpTimeout)
		}
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	if rec, ok := h.registry.recordFor(sess); !ok || !ownsSession(identity, rec) {
		// A1: a caller that does not own the session cannot tear it down.
		// Reply 204 (the dead-transport success contract) WITHOUT injecting
		// the close — indistinguishable from an already-reaped key, and the
		// victim's session is left untouched.
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	cc, ok := sess.conn.(*cometConn)
	if !ok {
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	// B5/M6: inject the close/disconnect frame best-effort and answer 2xx
	// immediately. The SDK treats any 2xx as done and never reads the body,
	// so there is no reason to pin the handler goroutine waiting for
	// teardown — a client spamming /close on a wedged session would
	// otherwise tie up a goroutine for up to writeTimeout each. The bounded
	// inject keeps even the feed from blocking if the inbound queue is full
	// on a stuck session; teardown proceeds asynchronously as the run loop
	// processes the injected frame.
	ctx, cancel := context.WithTimeout(r.Context(), cometCloseInjectTimeout)
	defer cancel()
	_ = cc.feed(ctx, &protocol.ProtocolMessage{Action: action})
	rw.WriteHeader(http.StatusNoContent)
}

// cometSession resolves a per-key comet request to its live session,
// bound to the authenticated caller's identity (A1). Unknown or dead
// keys — and keys the caller does not own — are 410 GONE (the production
// contract: the SDK treats it as nonfatal transport death and reconnects
// fresh). A foreign key is reported IDENTICALLY to an unknown one: a
// distinct status would hand any credentialed caller a liveness oracle
// for connectionKeys it doesn't own.
func (h *Handler) cometSession(rw http.ResponseWriter, r *http.Request, key string, identity authResult) (*session, *cometConn, bool) {
	sess := h.registry.lookupKey(key)
	if sess == nil {
		h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
		return nil, nil, false
	}
	rec, ok := h.registry.recordFor(sess)
	if !ok || !ownsSession(identity, rec) {
		// Not the owner: refuse exactly as for a dead key (no leak).
		h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
		return nil, nil, false
	}
	cc, ok := sess.conn.(*cometConn)
	if !ok {
		// A WS session's key addressed over comet paths — not a thing
		// SDKs do; refuse rather than cross transports.
		h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
		return nil, nil, false
	}
	return sess, cc, true
}

// ownsSession reports whether the authenticated caller may drive the comet
// session described by rec. The connectionKey is an unguessable per-session
// token, but a same-app caller who learns one (logs, proxies, referrer)
// must still be unable to read, inject into, or tear down a session it
// does not own (A1 — the CRITICAL audit finding). The bind:
//   - the signing/authenticating key MUST match (a different key, even in
//     the same app, is refused); and
//   - the clientId must be compatible: equal, OR the session assumed no
//     identity (wildcard or unidentified — clientID ""), OR the caller
//     presents the bare key / a wildcard token with no bound clientId,
//     which already carries the key's full authority and so grants no
//     privilege a fresh connection wouldn't.
func ownsSession(identity authResult, rec sessionRecord) bool {
	if identity.keyName == "" || identity.keyName != rec.keyName {
		return false
	}
	return rec.clientID == "" || identity.clientID == "" || identity.clientID == rec.clientID
}

// writeCometBatch writes one comet response: a JSON array of pre-encoded
// frames with a trailing newline. The trailing \n is load-bearing for
// node's streaming-mode parser (it splits on newlines and DISCARDS an
// unterminated final line); web's full-body JSON.parse tolerates it.
// Empty batches are 204 (a 200 with an empty body is fatal to node:
// JSON.parse(”) throws → malformed-body disconnect).
func writeCometBatch(rw http.ResponseWriter, frames [][]byte) {
	if len(frames) == 0 {
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	rw.Header().Set("Content-Type", contentTypeJSON)
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write([]byte("["))
	_, _ = rw.Write(bytes.Join(frames, []byte(",")))
	_, _ = rw.Write([]byte("]\n"))
}

// writeCometBatchBody writes a pre-rendered batch body (a forwarded recv
// reply, already in writeCometBatch's wire shape) — 204 when empty, like
// the local path's empty poll.
func writeCometBatchBody(rw http.ResponseWriter, body []byte) {
	if len(body) == 0 {
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	rw.Header().Set("Content-Type", contentTypeJSON)
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(body)
}

// serveCometConnect implements GET /comet/connect: authenticate, build
// the session exactly as the WS path does, then serve the connect
// request AS the first recv — its response carries CONNECTED (plus
// anything else the session emits in the same flush).
func (h *Handler) serveCometConnect(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// Comet is JSON-only (see file header). An explicit msgpack request
	// is a client bug — fail loud rather than serve frames it can't read.
	if f := q.Get("format"); f != "" && f != "json" {
		h.writeError(rw, r, http.StatusBadRequest, 40000, "the comet transport is JSON-only")
		return
	}

	identity, authErr := h.authenticate(r)
	if authErr != nil {
		// Pre-session failures are HTTP error envelopes: the SDK turns a
		// coded envelope from /comet/connect into a fatal connection
		// error (RTN14a — pinned by realtime/failure invalid_cred).
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}

	params, rejection := h.buildSessionParams(q, identity, protocol.FormatJSON)
	if rejection != nil {
		h.writeError(rw, r, rejection.statusCode, rejection.code, rejection.message)
		return
	}

	cc := newCometConn()
	// B2/M2: this only SETS the callback — the key is not registered here.
	// registerKey runs inside writeConnected on the RUN goroutine, before
	// the CONNECTED frame the client learns the key from is flushed (see
	// writeConnected). So there is no "registered before the run loop starts"
	// window: by the time any per-key request can carry this key, the run
	// loop is already serving and the key is indexed.
	params.onConnected = func(key string, s *session) { h.registry.registerKey(key, s) }
	sess := newSession(h.node, cc, params, h.presence, h.mint, h.materialized)
	h.registry.register(sess, sessionRecord{
		keyName:  identity.keyName,
		clientID: params.clientID,
		viaToken: identity.viaToken,
		issuedAt: identity.issuedAt,
	})

	// The session outlives this request: run on a detached context
	// (the centrifuge client context is bound to the session's closeCh,
	// not the request, but Values should not dangle either).
	go func() {
		defer h.registry.deregister(sess)
		defer func() {
			if sess.connKey != "" {
				h.registry.deregisterKey(sess.connKey, sess)
			}
		}()
		sess.run(context.WithoutCancel(r.Context()))
	}()

	// The connect request is the first recv: park until the session's
	// first flush (CONNECTED in the success path, ERROR otherwise —
	// either way a 200 array the SDK feeds through onData).
	ctx, cancel := context.WithTimeout(r.Context(), cometConnectFlush)
	defer cancel()
	frames := cc.parkRecv(ctx)
	if len(frames) == 0 {
		// The session produced nothing in time — tear it down and tell
		// the client to retry; nothing has been committed client-side.
		_ = cc.close()
		h.writeError(rw, r, http.StatusServiceUnavailable, 50003, "connection could not be established")
		return
	}
	writeCometBatch(rw, frames)
}

// serveCometRecv implements GET /comet/<key>/recv: the long poll.
func (h *Handler) serveCometRecv(rw http.ResponseWriter, r *http.Request, key string) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	// P6.4: a key owned by another LIVE node is forwarded — the owning node
	// parks the poll (bounded by forwardRecvPark; the ≤10s heartbeat always
	// completes it) and replies with the ready-to-write batch body.
	if h.registry.lookupKey(key) == nil {
		if nodeID, ok := h.cometForwardTarget(key); ok {
			code, body := h.forwardComet(r.Context(), cometSurveyRecvOp, nodeID, cometForwardReq{
				Key: key, KeyName: identity.keyName, ClientID: identity.clientID,
				WaitMS: forwardRecvPark.Milliseconds(),
			}, h.forwardRecvTimeout)
			if code != cometForwardOK {
				h.writeError(rw, r, http.StatusGone, 80016, "Unable to find connection "+key)
				return
			}
			writeCometBatchBody(rw, body)
			return
		}
	}
	_, cc, ok := h.cometSession(rw, r, key, identity)
	if !ok {
		return
	}
	frames := cc.parkRecv(r.Context())
	writeCometBatch(rw, frames)
}

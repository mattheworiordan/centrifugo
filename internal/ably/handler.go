// Package ably implements the Ably client wire protocol (protocol v6) on top
// of centrifuge.Node primitives, so unmodified Ably SDKs can connect to
// Centrifugo.
//
// Spec ID comments (RTNxx, RTLxx, RSCxx, RSLxx, TMxx, ...) reference the Ably
// features specification: https://sdk.ably.com/builds/ably/specification/main/features/
package ably

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// Content types negotiated on REST endpoints. Ably SDKs send
// Accept: application/x-msgpack when using the binary protocol (RSC8c).
const (
	contentTypeJSON    = "application/json"
	contentTypeMsgPack = "application/x-msgpack"
)

// Handler serves the Ably protocol endpoints: REST routes and the realtime
// WebSocket endpoint, all at the web root — Ably SDKs construct root-relative
// paths (/time, /channels/..., realtime WebSocket at /) which cannot be
// prefixed.
type Handler struct {
	mint         *serialMint
	materialized *materializedStore
	revocations  *revocationStore
	registry     *sessionRegistry
	stats        *statsStore
	serverID     string
	node         *centrifuge.Node
	config       configtypes.Ably
	keys         *auth.KeyStore
	upgrade      *websocket.Upgrader
	nonces       *nonceCache
	presence     *presenceStore
	// multiNode is true on the Redis engine (a shared broker → more than one
	// adapter node is possible), gating the cross-node fan-out features
	// (P6.1 presence refresh, P6.2a revocation sync). Derived from whether a
	// cross-node presence manager was wired (mux.go sets both together).
	multiNode bool
}

// NewHandler creates new Handler. The adapter is unusable without API keys
// to authenticate against (RSA11 Basic auth), so an enabled adapter with no
// keys_file — or one that fails to load — is a startup error.
func NewHandler(n *centrifuge.Node, c configtypes.Ably, presenceMgr centrifuge.PresenceManager, checkOrigin func(r *http.Request) bool) (*Handler, error) {
	if c.KeysFile == "" {
		return nil, errors.New("ably adapter is enabled but ably.keys_file is not set")
	}
	keys, err := auth.LoadKeyStore(c.KeysFile)
	if err != nil {
		return nil, err
	}
	upgrade := &websocket.Upgrader{}
	if checkOrigin != nil {
		upgrade.CheckOrigin = checkOrigin
	}
	// D3: seed a cold channel's serial generator from the broker high-water
	// (the latest retained publication's channelSerial tag), so after a
	// restart — or an A4b generator eviction — the next serial is strictly
	// greater than any prior one and continuity never regresses. On the
	// memory engine history is empty after a restart (fresh world), so the
	// seed is a no-op there; on the Redis engine it recovers the sequence.
	seed := func(channel string) (int64, int, bool) {
		res, err := n.History(brokerChannel(channel), centrifuge.WithLimit(1), centrifuge.WithReverse(true))
		if err != nil || len(res.Publications) == 0 {
			return 0, 0, false
		}
		cs := res.Publications[0].Tags[pubTagSerial]
		if cs == "" {
			return 0, 0, false
		}
		ts, counter, err := serial.ParseChannelSerial(cs)
		if err != nil {
			return 0, 0, false
		}
		return ts, counter, true
	}
	h := &Handler{
		mint:         newSerialMint(seed),
		materialized: newMaterializedStore(),
		revocations:  newRevocationStore(),
		registry:     newSessionRegistry(),
		stats:        newStatsStore(),
		serverID:     ablyServerID(),
		node:         n,
		config:       c,
		keys:         keys,
		upgrade:      upgrade,
		nonces:       newNonceCache(),
		presence:     newPresenceStoreWithManager(presenceMgr),
		multiNode:    presenceMgr != nil,
	}
	if err := h.seedPresenceFixtures(c.KeysFile); err != nil {
		return nil, err
	}
	// P6.1: keep this node's members alive in the cross-node presence manager
	// (Redis) and stop refreshing on node shutdown so a dead node's members
	// expire. No-op on the memory engine (presenceMgr == nil).
	h.presence.startRefresh(n.NotifyShutdown())
	// P6.2a: pull revocations issued on OTHER nodes from the broker feed and
	// apply them locally (connect-time store + live-disconnect). No-op
	// single-node (the revoking node enforces synchronously).
	h.startRevocationSync(n.NotifyShutdown())
	return h, nil
}

// seedPresenceFixtures loads the app fixture's channels block (the
// sandbox seeds presence members at app creation — e.g.
// persisted:presence_fixtures with six members, one cipher-encoded) so
// the pinned rest/presence fixture tests see the same world. Synthetic
// members carry fixture connectionIds; encodings pass through verbatim.
func (h *Handler) seedPresenceFixtures(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // operator-provided config path, read once at startup
	if err != nil {
		return err
	}
	var fixture struct {
		Channels []struct {
			Name     string `json:"name"`
			Presence []struct {
				ClientID string `json:"clientId"`
				Data     any    `json:"data"`
				Encoding string `json:"encoding"`
			} `json:"presence"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return fmt.Errorf("ably presence fixtures: %w", err)
	}
	now := time.Now().UnixMilli()
	for _, ch := range fixture.Channels {
		for i, m := range ch.Presence {
			member := &protocol.PresenceMessage{
				ID:           fmt.Sprintf("fixture:%d", i),
				Action:       protocol.PresencePresent,
				ClientID:     m.ClientID,
				ConnectionID: fmt.Sprintf("fixture:%d", i),
				Data:         m.Data,
				Encoding:     m.Encoding,
				Timestamp:    now,
			}
			h.presence.set(ch.Name, member)
			// The sandbox records the seeded members' ENTERs in presence
			// history too (pinned by rest/presence "Presence history
			// simple": six items expected). node.Run() precedes handler
			// construction (internal/app/run.go), so publishing here is
			// safe; only the client-unreachable shadow channel is written.
			enter := *member
			enter.Action = protocol.PresenceEnter
			data, err := json.Marshal(&enter)
			if err != nil {
				return fmt.Errorf("ably presence fixtures: %w", err)
			}
			unlock := h.mint.lockChannel(ch.Name)
			cs := h.mint.Mint(ch.Name)
			_, err = h.node.Publish(brokerChannel(presenceHistoryChannel(ch.Name)), data, publishOptions(ch.Name, "", cs)...)
			unlock()
			if err != nil {
				return fmt.Errorf("ably presence fixtures: %w", err)
			}
		}
	}
	return nil
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// Realtime connections arrive as WebSocket upgrades at the web root (RTN1).
	if r.URL.Path == "/" && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		h.serveRealtime(rw, r)
		return
	}
	// Every REST response carries the same identity and CORS surface the
	// real service does (verified against realtime.ably.io /time):
	// Allow-Origin is a constant `*` — overriding the centrifugo CORS
	// middleware's origin echo, which emits an EMPTY header to clients
	// that send no Origin — with the credentials flag dropped (`*` plus
	// credentials is an invalid CORS combination; Ably uses header auth,
	// not cookies). Serverid/Cluster on every response make the serving
	// stack identifiable on success, not just on errors.
	hdr := rw.Header()
	hdr.Set("Access-Control-Allow-Origin", "*")
	hdr.Del("Access-Control-Allow-Credentials")
	hdr.Set("Access-Control-Expose-Headers", corsExposedHeaders)
	hdr.Set("Vary", "Origin")
	hdr.Set("X-Ably-Serverid", h.serverID)
	hdr.Set("X-Ably-Cluster", ablyCluster)
	// CORS preflights MUST succeed without credentials (browsers strip
	// them from OPTIONS by spec) and MUST get a 2xx, or the browser never
	// sends the real request — a 401 here silently broke every
	// cross-origin REST call from web SDKs (history hydration in the
	// browser demo) while same-origin and non-browser clients worked.
	// Allow-Origin comes from the common block above; Allow-Headers is
	// echoed by the wrapping CORS middleware.
	if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
		rw.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		rw.Header().Set("Access-Control-Max-Age", "86400")
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	switch {
	case r.URL.Path == "/time" && r.Method == http.MethodGet:
		h.serveTime(rw, r)
	case strings.HasPrefix(r.URL.Path, "/channels/"):
		// RSL1/RSL2/channel details — see rest.go.
		h.serveChannels(rw, r)
	case r.URL.Path == "/messages" && r.Method == http.MethodPost:
		// BO2 batch publish — see rest.go.
		h.serveBatchPublish(rw, r)
	case strings.HasPrefix(r.URL.Path, "/keys/") && strings.HasSuffix(r.URL.Path, "/requestToken"):
		// RSA8 token request exchange — see resttoken.go.
		h.serveRequestToken(rw, r)
	case strings.HasPrefix(r.URL.Path, "/keys/") && strings.HasSuffix(r.URL.Path, "/revokeTokens") && r.Method == http.MethodPost:
		// RSA17 token revocation — see revocation.go.
		keyName := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/keys/"), "/revokeTokens")
		h.serveRevokeTokens(rw, r, keyName)
	case r.URL.Path == "/presence" && r.Method == http.MethodGet:
		// BAR1 batch presence — see rest.go.
		h.serveBatchPresence(rw, r)
	case r.URL.Path == "/stats" && r.Method == http.MethodGet:
		// RSC6 app statistics — see stats.go.
		h.serveStats(rw, r)
	case r.URL.Path == "/stats" && r.Method == http.MethodPost:
		// Sandbox-style stats fixture injection — see stats.go.
		h.serveStatsFixtures(rw, r)
	default:
		// Catch-all REST error per the Ably error contract; route surface
		// grows milestone by milestone. PATH RESOLUTION PRECEDES AUTH,
		// matching the real service (verified against realtime.ably.io:
		// `curl /foo` with no credentials is 404/40400, not 401): an
		// unknown path is "Could not find path" regardless of creds.
		// /comet/* is the exception because it's a REAL endpoint family —
		// auth applies there first, so an invalid key gets the coded
		// 40101 that RTN14a depends on when ably-js's comet fallback
		// probes /comet/connect after a failed WebSocket attempt.
		if strings.HasPrefix(r.URL.Path, "/comet/") {
			// The comet/HTTP-fallback transport family — see comet.go.
			h.serveComet(rw, r)
			return
		}
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound,
			"Could not find path: "+r.URL.Path)
	}
}

// serveRealtime runs one Ably realtime session (see session.go for the
// frame flow): querystring parsing (RTN2), key authentication, then the
// per-connection centrifuge client.
func (h *Handler) serveRealtime(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// RTN2a: the format param selects the wire encoding — msgpack or json
	// (absent defaults to json: SDKs always send the param when they want
	// the binary protocol). Unknown formats are rejected before the
	// upgrade with a clear error.
	format, err := protocol.FormatFromQuery(q.Get("format"))
	if err != nil {
		h.writeError(rw, r, http.StatusBadRequest, 40000, "unsupported format: the adapter serves json and msgpack")
		return
	}

	// RTN2e/RSA11 + token auth: resolve the caller's identity before any
	// session state exists. The verdict is delivered in-band after the
	// upgrade: Ably SDKs expect realtime auth failures as an ERROR
	// ProtocolMessage, not a failed handshake.
	identity, authErr := h.authenticate(r)

	conn, _, err := h.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		log.Error().Err(err).Str("transport", "ably").Msg("websocket upgrade error")
		return
	}

	// Protocol-level inbound size guard: cap WS reads at the maxFrameSize
	// the connection advertises in connectionDetails (CD2d), so an abusive
	// frame cannot exhaust memory. Exceeding this READ limit kills the
	// connection (the websocket library sends a close message and the
	// session read loop errors out) — unlike the application-level
	// maxMessageSize check (CD2c) in the session publish path, which NACKs
	// the frame and keeps the connection alive. The gap between the two
	// limits is headroom for protocol overhead around legitimate payloads.
	conn.SetReadLimit(maxFrameSize)

	if authErr != nil {
		// RTN14a: invalid credentials fail the connection. The ERROR frame
		// has an empty channel attribute, so the client transitions to
		// FAILED and the server terminates the connection (RTN14g).
		writeConnectionError(conn, format, authErr.code, authErr.statusCode, authErr.message)
		_ = conn.Close()
		return
	}

	params, rejection := h.buildSessionParams(q, identity, format)
	if rejection != nil {
		writeConnectionError(conn, format, rejection.code, rejection.statusCode, rejection.message)
		_ = conn.Close()
		return
	}
	sess := newSession(h.node, &wsConn{conn: conn, format: format}, params, h.presence, h.mint, h.materialized)
	// Revocation enforcement (RSA17): identity captured at connect; a
	// matching revocation disconnects the session with 40141.
	h.registry.register(sess, sessionRecord{
		keyName:  identity.keyName,
		clientID: params.clientID,
		viaToken: identity.viaToken,
		issuedAt: identity.issuedAt,
	})
	defer h.registry.deregister(sess)
	sess.run(r.Context())
}

// connectRejection is a connect-time refusal the transport front
// delivers its own way: the WS path as an in-band ERROR frame after the
// upgrade, the comet path as an HTTP error envelope.
type connectRejection struct {
	code       int
	statusCode int
	message    string
}

// buildSessionParams derives the session parameters from the RTN2
// connect query and the authenticated identity — shared by the WS and
// comet fronts.
func (h *Handler) buildSessionParams(q url.Values, identity authResult, format protocol.Format) (sessionParams, *connectRejection) {
	// RTN2d: an explicit clientId param is assumed for the connection; a
	// token-bound identity (RSA7a) or the authenticating key name
	// identifies it otherwise.
	clientID := q.Get("clientId")
	if clientID == "*" {
		// RSA7c: the literal '*' clientId value is reserved (it denotes the
		// wildcard identity) and cannot be assumed by a connection.
		return sessionParams{}, &connectRejection{errCodeInvalidClientID, http.StatusBadRequest, "invalid clientId: the wildcard value '*' is reserved"}
	}
	switch {
	case identity.clientID != "":
		// RSA15a: a clientId param must match the token-bound identity.
		if clientID != "" && clientID != identity.clientID {
			return sessionParams{}, &connectRejection{errCodeIncompatibleCredentials, http.StatusUnauthorized, "clientId is incompatible with the token's clientId"}
		}
		clientID = identity.clientID
	case identity.wildcardClientID:
		// RSA7b4/RSA15b: a wildcard token carries no identity; the caller
		// may assume any clientId (including none).
	}
	// C3: the connection clientId becomes a presence/attribution map key and
	// the centrifuge UserID — reject invalid UTF-8 at the boundary.
	if !validClientID(clientID) {
		return sessionParams{}, &connectRejection{errCodeInvalidClientID, http.StatusBadRequest, "clientId is not valid UTF-8"}
	}
	userID := identity.keyName
	if clientID != "" {
		userID = clientID
	}

	// RTN16-lite: a recovering client presents its previous connectionKey
	// ("<connectionId>!<token>") as the recover query param; the session
	// adopts the embedded connectionId so CONNECTED preserves it (RTN16d).
	// Malformed values are rejected with 80018 on the first CONNECTED and
	// the connection proceeds fresh (RTN16e posture).
	recoverID := ""
	var recoverError *protocol.ErrorInfo
	if rec := q.Get("recover"); rec != "" {
		if i := strings.IndexByte(rec, '!'); i > 0 {
			recoverID = rec[:i]
		}
		// This adapter's connectionIds are centrifuge client UUIDs: a
		// claim that cannot be one is rejected outright — the connection
		// proceeds FRESH and the initial CONNECTED carries 80018 (invalid
		// connection id, registry-verified) so the SDK abandons recovery
		// state (resets msgSerial — pinned by ably-js
		// unrecoverableConnection). Well-formed ids are adopted
		// unverified (RTN16-lite, documented divergence).
		if !uuidShaped(recoverID) {
			recoverID = ""
			recoverError = &protocol.ErrorInfo{
				Code:       80018,
				StatusCode: 400,
				Message:    "unable to recover connection: invalid connection key",
			}
		}
	}
	if recoverID != "" {
		// The connection recovered inside the grace window: it never died.
		// Disarm the pending presence expiry so its members neither vanish
		// nor LEAVE (the timer would otherwise delete the recovered
		// session's re-entered members — they share the connectionId key).
		h.presence.cancelExpiry(recoverID)
	}
	return sessionParams{
		userID:           userID,
		clientID:         clientID,
		wildcardClientID: identity.wildcardClientID && clientID == "",
		capability:       identity.capability,
		echo:             q.Get("echo") != "false", // RTN2b: echo is on unless explicitly disabled
		protocolVersion:  q.Get("v"),               // RTN2f
		format:           format,                   // RTN2a
		recoverID:        recoverID,                // RTN16d
		recoverError:     recoverError,             // RTN16e: 80018 on the first CONNECTED
		tokenExpires:     identity.expires,         // RTN15-territory: 40142 disconnect at exp
		reauth:           h.verifyTokenString,      // RTC8 AUTH frames
	}, nil
}

// uuidShaped reports whether s looks like a centrifuge client UUID —
// the only shape this adapter ever mints as a connectionId, so any
// recover claim that isn't one is definitionally invalid (80018).
func uuidShaped(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

// writeConnectionError fails a connection in-band before any session
// exists: an ERROR ProtocolMessage with an empty channel attribute
// (RTN14a/RTN14g — the client transitions to FAILED and the server
// terminates the connection afterwards). The frame is encoded in the
// format the client requested (RTN2a) — an SDK on the binary protocol
// does not decode JSON error frames.
func writeConnectionError(conn *websocket.Conn, format protocol.Format, code int, statusCode int, message string) {
	frame := &protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	}
	data, err := protocol.Marshal(frame, format)
	if err != nil {
		return
	}
	messageType := websocket.TextMessage
	if format == protocol.FormatMsgpack {
		messageType = websocket.BinaryMessage
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = conn.WriteMessage(messageType, data)
}

// serveTime implements GET /time (RSC16): the service time as a JSON/MsgPack
// array containing a single milliseconds-since-epoch integer.
func (h *Handler) serveTime(rw http.ResponseWriter, r *http.Request) {
	now := time.Now().UnixMilli()
	if responseFormat(r) == protocol.FormatMsgpack {
		body, err := protocol.MarshalAny([]int64{now}, protocol.FormatMsgpack)
		if err != nil {
			h.writeError(rw, r, http.StatusInternalServerError, 50000, "failed to encode time")
			return
		}
		rw.Header().Set("Content-Type", contentTypeMsgPack)
		_, _ = rw.Write(body)
		return
	}
	h.writeDocument(rw, r, http.StatusOK, []int64{now})
}

// responseFormat picks the REST response encoding: the format query param
// takes precedence, then the Accept header (RSC8c).
func responseFormat(r *http.Request) protocol.Format {
	switch r.URL.Query().Get("format") {
	case "msgpack":
		return protocol.FormatMsgpack
	case "json":
		return protocol.FormatJSON
	}
	if strings.Contains(r.Header.Get("Accept"), contentTypeMsgPack) {
		return protocol.FormatMsgpack
	}
	return protocol.FormatJSON
}

// writeError writes an Ably REST error response: an error envelope body plus
// X-Ably-Errorcode/X-Ably-Errormessage headers (HP6/HP7). JSON-only for now —
// SDKs accept JSON error bodies regardless of the requested format.
// ablyServerID is this node's identity in error envelopes and the
// X-Ably-Serverid header — the analogue of the real service's
// "frontend.<id>.<region>..." values, named so the serving stack is
// unmistakable (e.g. "centrifugo-adapter.5683e6e9b29318" on fly).
func ablyServerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return "centrifugo-adapter." + host
}

// ablyCluster mirrors the real service's X-Ably-Cluster header
// (prod:realtime:main there) with a value that makes the serving stack
// unmistakable when comparing responses side by side.
const ablyCluster = "poc:centrifugo-adapter"

// browserErrorPage is served when a browser (Accept: text/html) lands on
// an API error — the same courtesy page the real service shows at
// realtime.ably.io, with the PoC's provenance stated plainly.
const browserErrorPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>Ably-protocol endpoint</title>
<style>
  body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center;
         background: #f4f4f5; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #18181b; }
  .card { background: #fff; border-radius: 12px; padding: 48px; max-width: 560px; margin: 24px;
          box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  .mark { width: 56px; height: 56px; border-radius: 12px; background: #18181b; color: #ff2d2d;
          display: flex; align-items: center; justify-content: center; font-size: 28px; margin-bottom: 28px; }
  h1 { font-size: 28px; line-height: 1.25; margin: 0 0 20px; font-weight: 700; }
  p { font-size: 15px; line-height: 1.6; margin: 0 0 14px; color: #3f3f46; }
  a { color: #2563eb; }
  .prov { font-size: 13px; color: #71717a; border-top: 1px solid #e4e4e7; padding-top: 14px; margin-top: 22px; }
</style>
</head>
<body>
<div class="card">
  <div class="mark">&#9650;</div>
  <h1>Let's get you to the right place</h1>
  <p>This is an API endpoint designed for requests, not web browsing.</p>
  <p>Read more about Ably API request formats <a href="https://ably.com/docs/api/rest-api">in the Docs</a>.</p>
  <p class="prov">Served by the <strong>Ably-on-Centrifugo PoC adapter</strong> &mdash; an
  Ably-protocol-compatible server built on Centrifugo, not the Ably service.</p>
</div>
</body>
</html>`

func (h *Handler) writeError(rw http.ResponseWriter, r *http.Request, statusCode int, code int, message string) {
	// The real service appends a help pointer to every error message and
	// carries href/nonfatal/serverId in the envelope (verified against
	// realtime.ably.io) — same shape here, with serverId/cluster values
	// that say plainly which stack answered.
	href := "https://help.ably.io/error/" + strconv.Itoa(code)
	full := strings.TrimSuffix(message, ".") + ". (See " + href + " for help.)"
	// Identity/CORS headers are applied for every response in ServeHTTP;
	// only the error-specific ones are added here.
	rw.Header().Set("X-Ably-Errorcode", strconv.Itoa(code))
	rw.Header().Set("X-Ably-Errormessage", sanitizeHeaderValue(full))
	rw.Header().Set("X-Robots-Tag", "noindex")
	if r != nil && strings.Contains(r.Header.Get("Accept"), "text/html") {
		// A human in a browser: the courtesy page, with the error status
		// preserved for anything inspecting it.
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.WriteHeader(statusCode)
		_, _ = rw.Write([]byte(browserErrorPage))
		return
	}
	rw.Header().Set("Content-Type", contentTypeJSON)
	rw.WriteHeader(statusCode)
	// json.MarshalIndent, not hand-built strings: always-valid JSON for
	// request-derived bytes (the path lands in "Could not find path: …"),
	// and tab-indented like the real service's error bodies.
	envelope := struct {
		Error struct {
			Message    string `json:"message"`
			Code       int    `json:"code"`
			StatusCode int    `json:"statusCode"`
			Nonfatal   bool   `json:"nonfatal"`
			Href       string `json:"href"`
			ServerID   string `json:"serverId"`
		} `json:"error"`
	}{}
	envelope.Error.Message = full
	envelope.Error.Code = code
	envelope.Error.StatusCode = statusCode
	envelope.Error.Href = href
	envelope.Error.ServerID = h.serverID
	body, err := json.MarshalIndent(envelope, "", "\t")
	if err != nil { // unreachable for this struct; keep the response well-formed regardless
		body = []byte(`{"error":{"message":"internal error","code":50000,"statusCode":500}}`)
	}
	_, _ = rw.Write(body)
}

// sanitizeHeaderValue blanks control bytes out of a header value:
// net/http would reject or mangle them, and request-derived text (URL
// paths) can carry them.
func sanitizeHeaderValue(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}

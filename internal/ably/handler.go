// Package ably implements the Ably client wire protocol (protocol v6) on top
// of centrifuge.Node primitives, so unmodified Ably SDKs can connect to
// Centrifugo.
//
// Spec ID comments (RTNxx, RTLxx, RSCxx, RSLxx, TMxx, ...) reference the Ably
// features specification: https://sdk.ably.com/builds/ably/specification/main/features/
package ably

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
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
	node    *centrifuge.Node
	config  configtypes.Ably
	keys    *auth.KeyStore
	upgrade *websocket.Upgrader
}

// NewHandler creates new Handler. The adapter is unusable without API keys
// to authenticate against (RSA11 Basic auth), so an enabled adapter with no
// keys_file — or one that fails to load — is a startup error.
func NewHandler(n *centrifuge.Node, c configtypes.Ably, checkOrigin func(r *http.Request) bool) (*Handler, error) {
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
	return &Handler{
		node:    n,
		config:  c,
		keys:    keys,
		upgrade: upgrade,
	}, nil
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// Realtime connections arrive as WebSocket upgrades at the web root (RTN1).
	if r.URL.Path == "/" && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		h.serveRealtime(rw, r)
		return
	}
	switch {
	case r.URL.Path == "/time" && r.Method == http.MethodGet:
		h.serveTime(rw, r)
	case strings.HasPrefix(r.URL.Path, "/channels/"):
		// RSL1/RSL2/channel details — see rest.go.
		h.serveChannels(rw, r)
	default:
		// Catch-all REST error per the Ably error contract; route surface
		// grows milestone by milestone.
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
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

	// RTN2d: an explicit clientId param is assumed for the connection; a
	// token-bound identity (RSA7a) or the authenticating key name
	// identifies it otherwise.
	clientID := q.Get("clientId")
	if clientID == "*" {
		// RSA7c: the literal '*' clientId value is reserved (it denotes the
		// wildcard identity) and cannot be assumed by a connection.
		writeConnectionError(conn, format, errCodeInvalidClientID, http.StatusBadRequest, "invalid clientId: the wildcard value '*' is reserved")
		_ = conn.Close()
		return
	}
	switch {
	case identity.clientID != "":
		// RSA15a: a clientId param must match the token-bound identity.
		if clientID != "" && clientID != identity.clientID {
			writeConnectionError(conn, format, errCodeIncompatibleCredentials, http.StatusUnauthorized,
				"clientId is incompatible with the token's clientId")
			_ = conn.Close()
			return
		}
		clientID = identity.clientID
	case identity.wildcardClientID:
		// RSA7b4/RSA15b: a wildcard token carries no identity; the caller
		// may assume any clientId (including none).
	}
	userID := identity.keyName
	if clientID != "" {
		userID = clientID
	}

	sess := newSession(h.node, conn, sessionParams{
		userID:           userID,
		clientID:         clientID,
		wildcardClientID: identity.wildcardClientID && clientID == "",
		capability:       identity.capability,
		echo:             q.Get("echo") != "false", // RTN2b: echo is on unless explicitly disabled
		protocolVersion:  q.Get("v"),               // RTN2f
		format:           format,                   // RTN2a
	})
	sess.run(r.Context())
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
	rw.Header().Set("Content-Type", contentTypeJSON)
	_, _ = rw.Write([]byte("[" + strconv.FormatInt(now, 10) + "]"))
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
func (h *Handler) writeError(rw http.ResponseWriter, _ *http.Request, statusCode int, code int, message string) {
	rw.Header().Set("Content-Type", contentTypeJSON)
	rw.Header().Set("X-Ably-Errorcode", strconv.Itoa(code))
	rw.Header().Set("X-Ably-Errormessage", message)
	rw.WriteHeader(statusCode)
	body := `{"error":{"message":` + strconv.Quote(message) +
		`,"code":` + strconv.Itoa(code) +
		`,"statusCode":` + strconv.Itoa(statusCode) + `}}`
	_, _ = rw.Write([]byte(body))
}

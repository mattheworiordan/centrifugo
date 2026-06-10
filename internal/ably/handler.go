// Package ably implements the Ably client wire protocol (protocol v6) on top
// of centrifuge.Node primitives, so unmodified Ably SDKs can connect to
// Centrifugo.
//
// Spec ID comments (RTNxx, RTLxx, RSCxx, RSLxx, TMxx, ...) reference the Ably
// features specification: https://sdk.ably.com/builds/ably/specification/main/features/
package ably

import (
	"encoding/binary"
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
	default:
		// Catch-all REST error per the Ably error contract; route surface
		// grows milestone by milestone.
		h.writeError(rw, r, http.StatusNotFound, 40400, "not found")
	}
}

// serveRealtime runs one Ably realtime session (see session.go for the
// frame flow): querystring parsing (RTN2), key authentication, then the
// per-connection centrifuge client.
func (h *Handler) serveRealtime(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// RTN2a: the format param selects the wire encoding. This milestone
	// serves json only — msgpack framing lands in M2 — so anything else is
	// rejected before the upgrade with a clear error.
	format, err := protocol.FormatFromQuery(q.Get("format"))
	if err != nil || format != protocol.FormatJSON {
		h.writeError(rw, r, http.StatusBadRequest, 40000, "unsupported format: the adapter currently serves json only")
		return
	}

	// RTN2e/RSA11: verify the presented API key (Basic auth header or key
	// query param) before any session state exists. The verdict is
	// delivered in-band after the upgrade: Ably SDKs expect realtime auth
	// failures as an ERROR ProtocolMessage, not a failed handshake.
	key, authErr := h.keys.Authenticate(r)

	conn, _, err := h.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		log.Error().Err(err).Str("transport", "ably").Msg("websocket upgrade error")
		return
	}

	if authErr != nil {
		// RTN14a: an invalid API key fails the connection. The ERROR frame
		// has an empty channel attribute, so the client transitions to
		// FAILED and the server terminates the connection (RTN14g).
		message := "invalid credentials"
		if errors.Is(authErr, auth.ErrNoCredentials) {
			message = "no credentials presented"
		}
		writeConnectionError(conn, errCodeInvalidCredentials, http.StatusUnauthorized, message)
		_ = conn.Close()
		return
	}

	// RTN2d: an explicit clientId param is assumed for the connection; the
	// authenticated key name identifies it otherwise.
	clientID := q.Get("clientId")
	if clientID == "*" {
		// RSA7c: the literal '*' clientId value is reserved (it denotes the
		// wildcard identity) and cannot be assumed by a connection.
		writeConnectionError(conn, errCodeInvalidClientID, http.StatusBadRequest, "invalid clientId: the wildcard value '*' is reserved")
		_ = conn.Close()
		return
	}
	userID := key.APIKey.AppID + "." + key.APIKey.KeyID
	if clientID != "" {
		userID = clientID
	}

	sess := newSession(h.node, conn, sessionParams{
		userID:          userID,
		clientID:        clientID,
		echo:            q.Get("echo") != "false", // RTN2b: echo is on unless explicitly disabled
		protocolVersion: q.Get("v"),               // RTN2f
	})
	sess.run(r.Context())
}

// writeConnectionError fails a connection in-band before any session
// exists: an ERROR ProtocolMessage with an empty channel attribute
// (RTN14a/RTN14g — the client transitions to FAILED and the server
// terminates the connection afterwards).
func writeConnectionError(conn *websocket.Conn, code int, statusCode int, message string) {
	frame := &protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Code:       code,
			StatusCode: statusCode,
			Message:    message,
		},
	}
	data, err := protocol.Marshal(frame, protocol.FormatJSON)
	if err != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

// serveTime implements GET /time (RSC16): the service time as a JSON/MsgPack
// array containing a single milliseconds-since-epoch integer.
func (h *Handler) serveTime(rw http.ResponseWriter, r *http.Request) {
	now := time.Now().UnixMilli()
	if responseFormat(r) == formatMsgPack {
		// Minimal hand-rolled encoding of [int64]: fixarray(1) + int64. The
		// full MsgPack codec replaces this in M2.
		body := make([]byte, 0, 10)
		body = append(body, 0x91, 0xd3)
		body = binary.BigEndian.AppendUint64(body, uint64(now))
		rw.Header().Set("Content-Type", contentTypeMsgPack)
		_, _ = rw.Write(body)
		return
	}
	rw.Header().Set("Content-Type", contentTypeJSON)
	_, _ = rw.Write([]byte("[" + strconv.FormatInt(now, 10) + "]"))
}

type restFormat int

const (
	formatJSON restFormat = iota
	formatMsgPack
)

// responseFormat picks the REST response encoding: the format query param
// takes precedence, then the Accept header (RSC8c).
func responseFormat(r *http.Request) restFormat {
	switch r.URL.Query().Get("format") {
	case "msgpack":
		return formatMsgPack
	case "json":
		return formatJSON
	}
	if strings.Contains(r.Header.Get("Accept"), contentTypeMsgPack) {
		return formatMsgPack
	}
	return formatJSON
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

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
	"os"
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
	mint     *serialMint
	node     *centrifuge.Node
	config   configtypes.Ably
	keys     *auth.KeyStore
	upgrade  *websocket.Upgrader
	nonces   *nonceCache
	presence *presenceStore
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
	h := &Handler{
		mint:     newSerialMint(),
		node:     n,
		config:   c,
		keys:     keys,
		upgrade:  upgrade,
		nonces:   newNonceCache(),
		presence: newPresenceStore(),
	}
	if err := h.seedPresenceFixtures(c.KeysFile); err != nil {
		return nil, err
	}
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
			if _, err := h.node.Publish(presenceHistoryChannel(ch.Name), data, publishOptions(ch.Name, "", h.mint.Mint(ch.Name))...); err != nil {
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
	switch {
	case r.URL.Path == "/time" && r.Method == http.MethodGet:
		h.serveTime(rw, r)
	case strings.HasPrefix(r.URL.Path, "/channels/"):
		// RSL1/RSL2/channel details — see rest.go.
		h.serveChannels(rw, r)
	case strings.HasPrefix(r.URL.Path, "/keys/") && strings.HasSuffix(r.URL.Path, "/requestToken"):
		// RSA8 token request exchange — see resttoken.go.
		h.serveRequestToken(rw, r)
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

	// RTN16-lite: a recovering client presents its previous connectionKey
	// ("<connectionId>!<token>") as the recover query param; the session
	// adopts the embedded connectionId so CONNECTED preserves it (RTN16d).
	// Malformed values are ignored — the connection proceeds fresh, which
	// is the correct recovery-failure posture.
	recoverID := ""
	if rec := q.Get("recover"); rec != "" {
		if i := strings.IndexByte(rec, '!'); i > 0 {
			recoverID = rec[:i]
		}
	}
	if recoverID != "" {
		// The connection recovered inside the grace window: it never died.
		// Disarm the pending presence expiry so its members neither vanish
		// nor LEAVE (the timer would otherwise delete the recovered
		// session's re-entered members — they share the connectionId key).
		h.presence.cancelExpiry(recoverID)
	}
	sess := newSession(h.node, conn, sessionParams{
		userID:           userID,
		clientID:         clientID,
		wildcardClientID: identity.wildcardClientID && clientID == "",
		capability:       identity.capability,
		echo:             q.Get("echo") != "false", // RTN2b: echo is on unless explicitly disabled
		protocolVersion:  q.Get("v"),               // RTN2f
		format:           format,                   // RTN2a
		recoverID:        recoverID,                // RTN16d
	}, h.presence, h.mint)
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

// Package ably implements the Ably client wire protocol (protocol v6) on top
// of centrifuge.Node primitives, so unmodified Ably SDKs can connect to
// Centrifugo.
//
// Spec ID comments (RTNxx, RTLxx, RSCxx, RSLxx, TMxx, ...) reference the Ably
// features specification: https://sdk.ably.com/builds/ably/specification/main/features/
package ably

import (
	"encoding/binary"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	upgrade *websocket.Upgrader
}

// NewHandler creates new Handler.
func NewHandler(n *centrifuge.Node, c configtypes.Ably, checkOrigin func(r *http.Request) bool) *Handler {
	upgrade := &websocket.Upgrader{}
	if checkOrigin != nil {
		upgrade.CheckOrigin = checkOrigin
	}
	return &Handler{
		node:    n,
		config:  c,
		upgrade: upgrade,
	}
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

// serveRealtime is a placeholder for the realtime protocol endpoint: it
// accepts the WebSocket upgrade and closes. The protocol state machine lands
// with the M0 Node-driving-path spike.
func (h *Handler) serveRealtime(rw http.ResponseWriter, r *http.Request) {
	conn, _, err := h.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		log.Error().Err(err).Str("transport", "ably").Msg("websocket upgrade error")
		return
	}
	_ = conn.Close()
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

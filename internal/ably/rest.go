package ably

// REST message surface (RSL1/RSL2/RSL8-adjacent): publish, history and
// channel details, served under /channels/{channel}[/messages]. Publish
// reuses the shared core in publish.go so envelope rules stay
// byte-identical with the realtime surface; history reads the canonical
// stored envelopes straight out of centrifuge history.

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

const (
	historyDefaultLimit = 100
	historyMaxLimit     = 1000
)

// connectionKeySuffix is the opaque tail of the connectionKey this adapter
// mints in connectionDetails (CD2b): "<connectionId>!key". A REST publish
// carrying Message.connectionKey (TM2h) is attributed to that connection.
const connectionKeySuffix = "!key"

// channelRoute splits an /channels/... request into the channel name and
// the trailing subresource ("" or "messages"). The channel segment is
// taken from the ESCAPED path: Ably channel names may contain characters
// (e.g. '/') that SDKs percent-encode, which r.URL.Path would have
// already decoded into path separators.
func channelRoute(r *http.Request) (channel string, subresource string, ok bool) {
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/channels/")
	if escaped == r.URL.EscapedPath() || escaped == "" {
		return "", "", false
	}
	if i := strings.IndexByte(escaped, '/'); i >= 0 {
		escaped, subresource = escaped[:i], escaped[i+1:]
	}
	channel, err := url.PathUnescape(escaped)
	if err != nil || channel == "" {
		return "", "", false
	}
	return channel, subresource, true
}

// serveChannels dispatches /channels/{channel}[/messages] (the route match
// is done in ServeHTTP).
func (h *Handler) serveChannels(rw http.ResponseWriter, r *http.Request) {
	channel, sub, ok := channelRoute(r)
	if !ok {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
		return
	}
	// Every /channels route is authenticated (RSA11 Basic / key param).
	if _, err := h.keys.Authenticate(r); err != nil {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "invalid credentials")
		return
	}
	switch {
	case sub == "messages" && r.Method == http.MethodPost:
		h.serveRESTPublish(rw, r, channel)
	case sub == "messages" && r.Method == http.MethodGet:
		h.serveRESTHistory(rw, r, channel)
	case sub == "" && r.Method == http.MethodGet:
		h.serveChannelDetails(rw, r, channel)
	default:
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
	}
}

// requestBodyFormat picks the wire encoding of a REST request body from
// its Content-Type (RSC8a/RSC8b: SDKs send application/x-msgpack on the
// binary protocol, application/json otherwise).
func requestBodyFormat(r *http.Request) protocol.Format {
	if strings.Contains(r.Header.Get("Content-Type"), contentTypeMsgPack) {
		return protocol.FormatMsgpack
	}
	return protocol.FormatJSON
}

// decodeMessageBody decodes a REST publish body: either a single Message
// document or a bare array of them (RSL1a/RSL1c — ably-js sends both
// shapes).
func decodeMessageBody(body []byte, format protocol.Format) ([]*protocol.Message, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	isArray := false
	switch format {
	case protocol.FormatJSON:
		trimmed := strings.TrimLeft(string(body), " \t\r\n")
		isArray = strings.HasPrefix(trimmed, "[")
	case protocol.FormatMsgpack:
		b := body[0]
		isArray = (b >= 0x90 && b <= 0x9f) || b == 0xdc || b == 0xdd
	}
	if isArray {
		var msgs []*protocol.Message
		if err := protocol.UnmarshalAny(body, format, &msgs); err != nil {
			return nil, err
		}
		return msgs, nil
	}
	var msg protocol.Message
	if err := protocol.UnmarshalAny(body, format, &msg); err != nil {
		return nil, err
	}
	return []*protocol.Message{&msg}, nil
}

// serveRESTPublish implements POST /channels/{channel}/messages (RSL1).
func (h *Handler) serveRESTPublish(rw http.ResponseWriter, r *http.Request, channel string) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, maxFrameSize))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			// Same verdict as the in-band size check: 40009 for any
			// oversized publish, whether it trips the envelope sum or the
			// request cap.
			h.writeError(rw, r, http.StatusBadRequest, errCodeMaxMessageLength, "maximum message length exceeded")
			return
		}
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "failed to read request body")
		return
	}
	messages, err := decodeMessageBody(body, requestBodyFormat(r))
	if err != nil || len(messages) == 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid publish request body")
		return
	}

	// TM2h: Message.connectionKey lets a REST publisher attribute the
	// message to an existing realtime connection. This adapter's keys are
	// "<connectionId>!key" (see session CONNECTED), so attribution is a
	// suffix strip; the key is request-scoped and never stored (the shared
	// core clears it).
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		// A REST body must not carry connection attribution directly: the
		// only legitimate route is the connectionKey (TM2h) resolved below,
		// so a client-supplied connectionId is dropped, never trusted.
		msg.ConnectionID = ""
		if msg.ConnectionKey == "" {
			continue
		}
		connID := strings.TrimSuffix(msg.ConnectionKey, connectionKeySuffix)
		if connID == msg.ConnectionKey || connID == "" {
			h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidConnectionID, "invalid connection key")
			return
		}
		// The connection's existence is NOT verified (centrifuge exposes no
		// per-ID client lookup): a well-formed key for a dead connection
		// attributes silently rather than erroring — PoC divergence from
		// TM2h's invalid-key error expectation, tracked for M9.
		msg.ConnectionID = connID
	}

	// RSA7e2: a Basic-auth REST client conveys its identity as an
	// X-Ably-ClientId header, Base64 encoded. An identified publisher's
	// clientId is stamped on clientId-less messages and incompatible
	// explicit clientIds are rejected (RSL1m1/RSL1m4, in the shared core).
	publisherClientID := ""
	if raw := r.Header.Get("X-Ably-ClientId"); raw != "" {
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidClientID, "invalid X-Ably-ClientId header")
			return
		}
		publisherClientID = string(decoded)
	}

	idBase, err := newRESTIDBase()
	if err != nil {
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "internal error")
		return
	}
	payloads, idemKeys, problem := buildEnvelopes(messages, envelopeParams{
		// REST publishes have no connection identity: connectionID stays
		// empty (no TM2c attribution beyond explicit TM2h above, no origin
		// tag — REST messages are never echo-suppressed).
		clientID: publisherClientID,
		newID: func(idx int) string {
			// TM2a: server-assigned ids for REST messages share the
			// "<base>:<idx>" shape SDK-side idempotent publishing uses
			// (RSL1k1).
			return fmt.Sprintf("%s:%d", idBase, idx)
		},
	})
	if problem != nil {
		h.writeError(rw, r, statusOf(problem), problem.code, problem.message)
		return
	}
	for i, data := range payloads {
		// Client-supplied ids dedup republishes (RSL1k2/RSL1k5): the broker
		// drops the duplicate and the request still succeeds — pinned by
		// ably-js "idempotentRestPublishing set to false".
		opts := publishOptions(channel, "")
		if idemKeys[i] != "" {
			opts = append(opts, centrifuge.WithIdempotencyKey(idemKeys[i]),
				centrifuge.WithIdempotentResultTTL(idempotentResultTTL))
		}
		if _, err := h.node.Publish(channel, data, opts...); err != nil {
			log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("rest publish failed")
			h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "publish failed")
			return
		}
	}
	// RSL1: a successful publish is 201 with an empty document body; SDKs
	// key off the status (the response body only matters once
	// PublishResult.serials lands in M8).
	h.writeDocument(rw, r, http.StatusCreated, map[string]any{})
}

func statusOf(p *publishProblem) int {
	if p.statusCode == 0 {
		return http.StatusBadRequest
	}
	return p.statusCode
}

// serveRESTHistory implements GET /channels/{channel}/messages (RSL2):
// limit/direction params, bare-array bodies, Link-header pagination.
//
// Pagination cursors ride two adapter-private query params (cursorOffset/
// cursorEpoch). SDKs treat Link URLs as opaque (RSL2b territory via the
// paginated-resource contract), so the cursor encoding is free to lean on
// a centrifuge memory-broker property: per-channel publication offsets are
// dense and monotonic, so the page below offset L is exactly the window
// [L-limit, L-1], fetchable forward via WithSince and reversed for
// backwards responses.
func (h *Handler) serveRESTHistory(rw http.ResponseWriter, r *http.Request, channel string) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	q := r.URL.Query()

	limit := historyDefaultLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > historyMaxLimit {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	backwards := true
	switch q.Get("direction") {
	case "", "backwards":
	case "forwards":
		backwards = false
	default:
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid direction")
		return
	}
	// start/end time bounds (RSL2b1) are applied as a post-filter on the
	// fetched window: centrifuge history iterates by stream position, not
	// time, and PoC retention windows are small enough that filtering the
	// page is honest (a message excluded by the filter still consumes page
	// capacity — divergence noted for M9).
	var startMS, endMS int64
	var hasStart, hasEnd bool
	if raw := q.Get("start"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid start")
			return
		}
		startMS, hasStart = v, true
	}
	if raw := q.Get("end"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid end")
			return
		}
		endMS, hasEnd = v, true
	}
	// RSL2b1: start must be equal to or less than end.
	if hasStart && hasEnd && startMS > endMS {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "start must be equal to or less than end")
		return
	}

	var pubs []*centrifuge.Publication
	var epoch string
	var err error
	if rawCursor := q.Get("cursorOffset"); rawCursor != "" {
		cursor, cerr := strconv.ParseUint(rawCursor, 10, 64)
		if cerr != nil {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid cursor")
			return
		}
		cursorEpoch := q.Get("cursorEpoch")
		if backwards {
			// Window [cursor-limit, cursor-1], fetched forward from
			// since=cursor-limit-1 then reversed for delivery order.
			if cursor <= 1 {
				h.writeHistoryPage(rw, r, channel, nil, limit, backwards, false, 0, "")
				return
			}
			low := uint64(1)
			if cursor > uint64(limit) {
				low = cursor - uint64(limit)
			}
			count := int(cursor - low)
			pubs, epoch, err = h.historyPubs(channel, centrifuge.WithLimit(count),
				centrifuge.WithSince(&centrifuge.StreamPosition{Offset: low - 1, Epoch: cursorEpoch}))
			reversePubs(pubs)
		} else {
			pubs, epoch, err = h.historyPubs(channel, centrifuge.WithLimit(limit),
				centrifuge.WithSince(&centrifuge.StreamPosition{Offset: cursor, Epoch: cursorEpoch}))
		}
	} else {
		pubs, epoch, err = h.historyPubs(channel, centrifuge.WithLimit(limit), centrifuge.WithReverse(backwards))
	}
	if err != nil {
		log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("history read failed")
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "history read failed")
		return
	}

	// Page cursors come from the raw publication window, BEFORE the time
	// post-filter: a filtered-out publication still moves the cursor, so
	// pagination always makes progress.
	var lowestOffset, highestOffset uint64
	for _, pub := range pubs {
		if lowestOffset == 0 || pub.Offset < lowestOffset {
			lowestOffset = pub.Offset
		}
		if pub.Offset > highestOffset {
			highestOffset = pub.Offset
		}
	}

	format := responseFormat(r)
	items := make([]*protocol.Message, 0, len(pubs))
	for _, pub := range pubs {
		var msg protocol.Message
		if err := json.Unmarshal(pub.Data, &msg); err != nil {
			// Not a canonical envelope (e.g. a native centrifugo publish on
			// a shared channel): skip rather than corrupt the page.
			log.Warn().Str("channel", channel).Str("transport", transportName).Msg("skipping non-envelope publication in history")
			continue
		}
		if (hasStart && msg.Timestamp < startMS) || (hasEnd && msg.Timestamp > endMS) {
			continue
		}
		if format == protocol.FormatMsgpack {
			// RSL4c1: binary payloads in msgpack response bodies are the
			// msgpack binary type; pop the canonical transport "base64".
			denormalizeMessageData(&msg)
		}
		items = append(items, &msg)
	}

	// Dense per-channel offsets (memory broker) make "older messages may
	// exist" exactly "the lowest offset seen is above 1"; forwards relies
	// on the full-page heuristic (an empty final page reads as last).
	hasNext := false
	nextCursor := uint64(0)
	if backwards {
		hasNext = len(pubs) > 0 && lowestOffset > 1
		nextCursor = lowestOffset
	} else {
		hasNext = len(pubs) == limit
		nextCursor = highestOffset
	}
	h.writeHistoryPage(rw, r, channel, items, limit, backwards, hasNext, nextCursor, epoch)
}

func (h *Handler) historyPubs(channel string, opts ...centrifuge.HistoryOption) ([]*centrifuge.Publication, string, error) {
	res, err := h.node.History(channel, opts...)
	if err != nil {
		return nil, "", err
	}
	return res.Publications, res.StreamPosition.Epoch, nil
}

func reversePubs(pubs []*centrifuge.Publication) {
	for i, j := 0, len(pubs)-1; i < j; i, j = i+1, j-1 {
		pubs[i], pubs[j] = pubs[j], pubs[i]
	}
}

// writeHistoryPage writes one history page: a bare message array (RSL2)
// plus Link pagination headers (rel="first" and, when a further page may
// exist, rel="next") that SDK paginated resources follow as opaque URLs.
func (h *Handler) writeHistoryPage(rw http.ResponseWriter, r *http.Request, channel string, items []*protocol.Message, limit int, backwards bool, hasNext bool, nextCursor uint64, epoch string) {
	if items == nil {
		items = []*protocol.Message{}
	}
	direction := "forwards"
	if backwards {
		direction = "backwards"
	}
	// The link URL must be the relative form `./messages?<query>`: ably-js
	// getRelParams only matches /^\.\/(\w+)\?(.*)$/ (paginatedresource.ts)
	// and re-issues the request on the original path with the parsed query.
	const base = "./messages"
	// start/end bounds must survive into the page links, or page 2+ of a
	// time-bounded query would return out-of-range items.
	bounds := ""
	if raw := r.URL.Query().Get("start"); raw != "" {
		bounds += "&start=" + url.QueryEscape(raw)
	}
	if raw := r.URL.Query().Get("end"); raw != "" {
		bounds += "&end=" + url.QueryEscape(raw)
	}
	first := fmt.Sprintf("%s?limit=%d&direction=%s%s", base, limit, direction, bounds)
	links := []string{fmt.Sprintf("<%s>; rel=\"first\"", first)}
	if hasNext && len(items) > 0 {
		next := fmt.Sprintf("%s?limit=%d&direction=%s%s&cursorOffset=%d&cursorEpoch=%s",
			base, limit, direction, bounds, nextCursor, url.QueryEscape(epoch))
		links = append(links, fmt.Sprintf("<%s>; rel=\"next\"", next))
	}
	for _, l := range links {
		rw.Header().Add("Link", l)
	}
	h.writeDocument(rw, r, http.StatusOK, items)
}

// channelDetails is the GET /channels/{channel} response shape pinned by
// ably-js rest/status status0 (CHD1-adjacent: channelId plus
// status.occupancy.metrics with the six numeric occupancy fields).
type channelDetails struct {
	ChannelID string        `json:"channelId" msgpack:"channelId"`
	Name      string        `json:"name"      msgpack:"name"`
	Status    channelStatus `json:"status"    msgpack:"status"`
}

type channelStatus struct {
	IsActive  bool             `json:"isActive"  msgpack:"isActive"`
	Occupancy channelOccupancy `json:"occupancy" msgpack:"occupancy"`
}

type channelOccupancy struct {
	Metrics channelMetrics `json:"metrics" msgpack:"metrics"`
}

type channelMetrics struct {
	Connections         int `json:"connections"         msgpack:"connections"`
	Publishers          int `json:"publishers"          msgpack:"publishers"`
	Subscribers         int `json:"subscribers"         msgpack:"subscribers"`
	PresenceConnections int `json:"presenceConnections" msgpack:"presenceConnections"`
	PresenceMembers     int `json:"presenceMembers"     msgpack:"presenceMembers"`
	PresenceSubscribers int `json:"presenceSubscribers" msgpack:"presenceSubscribers"`
}

// serveChannelDetails implements GET /channels/{channel}. Occupancy comes
// from the centrifuge presence/subscription primitives where they exist;
// metrics centrifuge does not track per-channel (publishers) report zero —
// the status0 contract requires numbers, not particular values.
func (h *Handler) serveChannelDetails(rw http.ResponseWriter, r *http.Request, channel string) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	subscribers := h.node.Hub().NumSubscribers(channel)
	presence := 0
	if stats, err := h.node.PresenceStats(channel); err == nil {
		presence = stats.NumClients
	}
	h.writeDocument(rw, r, http.StatusOK, &channelDetails{
		ChannelID: channel,
		Name:      channel,
		Status: channelStatus{
			IsActive: subscribers > 0,
			Occupancy: channelOccupancy{
				Metrics: channelMetrics{
					Connections:         subscribers,
					Subscribers:         subscribers,
					PresenceConnections: presence,
					PresenceMembers:     presence,
					PresenceSubscribers: presence,
				},
			},
		},
	})
}

// writeDocument writes a REST response document in the negotiated format
// (RSC8c).
func (h *Handler) writeDocument(rw http.ResponseWriter, r *http.Request, status int, v any) {
	format := responseFormat(r)
	data, err := protocol.MarshalAny(v, format)
	if err != nil {
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "failed to encode response")
		return
	}
	if format == protocol.FormatMsgpack {
		rw.Header().Set("Content-Type", contentTypeMsgPack)
	} else {
		rw.Header().Set("Content-Type", contentTypeJSON)
	}
	rw.WriteHeader(status)
	_, _ = rw.Write(data)
}

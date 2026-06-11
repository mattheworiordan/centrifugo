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
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

const (
	historyDefaultLimit = 100
	historyMaxLimit     = 1000
)

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
	// Every /channels route is authenticated: token (Bearer) or Basic/key
	// param (RSA11), resolved by the shared authenticator.
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	// messages/{serial} and messages/{serial}/versions (RSL11/RSL14/RSL15):
	// the serial segment is percent-encoded by SDKs (it contains ':').
	if serialSub, found := strings.CutPrefix(sub, "messages/"); found && serialSub != "" {
		versions := false
		if v, cut := strings.CutSuffix(serialSub, "/versions"); cut {
			serialSub, versions = v, true
		}
		serial, err := url.PathUnescape(serialSub)
		if err != nil || serial == "" || strings.ContainsRune(serial, '/') {
			h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
			return
		}
		switch {
		case versions && r.Method == http.MethodGet:
			h.serveMessageVersions(rw, r, channel, identity, serial)
		case !versions && r.Method == http.MethodGet:
			h.serveGetMessage(rw, r, channel, identity, serial)
		case !versions && r.Method == http.MethodPatch:
			h.serveMutateMessage(rw, r, channel, identity, serial)
		default:
			h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
		}
		return
	}
	switch {
	case sub == "messages" && r.Method == http.MethodPost:
		h.serveRESTPublish(rw, r, channel, identity)
	case sub == "messages" && r.Method == http.MethodGet:
		h.serveRESTHistory(rw, r, channel, identity, historyKindMessages)
	case sub == "presence" && r.Method == http.MethodGet:
		h.serveRESTPresence(rw, r, channel, identity)
	case sub == "presence/history" && r.Method == http.MethodGet:
		h.serveRESTHistory(rw, r, channel, identity, historyKindPresence)
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
func (h *Handler) serveRESTPublish(rw http.ResponseWriter, r *http.Request, channel string, identity authResult) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	// Publishing requires the publish operation (40160).
	if !identity.capability.Allows(auth.OpPublish, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit publish")
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
	// "<connectionId>!<token>" (see session CONNECTED), so attribution is
	// everything before the first '!'; the key is request-scoped and never
	// stored (the shared core clears it).
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
		// "<connectionId>!<token>" — attribution is everything before the
		// first '!' (the token half is per-session, see session CONNECTED).
		bang := strings.IndexByte(msg.ConnectionKey, '!')
		if bang <= 0 {
			h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidConnectionID, "invalid connection key")
			return
		}
		connID := msg.ConnectionKey[:bang]
		// The connection's existence is NOT verified (centrifuge exposes no
		// per-ID client lookup): a well-formed key for a dead connection
		// attributes silently rather than erroring — PoC divergence from
		// TM2h's invalid-key error expectation, tracked for M9.
		msg.ConnectionID = connID
	}

	// The publisher's identity: the token-bound clientId for token auth
	// (a wildcard token has none and may assume any — RSA7b4); for Basic
	// auth, the RSA7e2 X-Ably-ClientId header, Base64 encoded. An
	// identified publisher's clientId is stamped on clientId-less messages
	// and incompatible explicit clientIds are rejected (RSL1m1/RSL1m4, in
	// the shared core).
	publisherClientID := identity.clientID
	if !identity.viaToken {
		if raw := r.Header.Get("X-Ably-ClientId"); raw != "" {
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidClientID, "invalid X-Ably-ClientId header")
				return
			}
			publisherClientID = string(decoded)
		}
	}

	idBase, err := newRESTIDBase()
	if err != nil {
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "internal error")
		return
	}
	// T1.2: mint+append atomic per channel (serials.go).
	unlock := h.mint.lockChannel(channel)
	defer unlock()
	payloads, idemKeys, serials, problem := buildEnvelopes(messages, envelopeParams{
		// REST publishes have no connection identity: connectionID stays
		// empty (no TM2c attribution beyond explicit TM2h above, no origin
		// tag — REST messages are never echo-suppressed).
		clientID:   publisherClientID,
		mintSerial: func() string { return h.mint.Mint(channel) },
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
		opts := publishOptions(channel, "", serials[i])
		if idemKeys[i] != "" {
			opts = append(opts, centrifuge.WithIdempotencyKey(idemKeys[i]),
				centrifuge.WithIdempotentResultTTL(idempotentResultTTL))
		}
		if _, err := h.node.Publish(brokerChannel(channel), data, opts...); err != nil {
			log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("rest publish failed")
			h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "publish failed")
			return
		}
	}
	// RSL1: a successful publish is 201 with an empty document body; SDKs
	// key off the status; mutableMessages channels return the assigned
	// serials (RSL1n PublishResult) — ably-js restchannel._publish decodes
	// the body and AIT requires serials[0].
	if mutableChannel(channel) {
		// The MESSAGE serials ("<channelSerial>:<idx>"), matching the
		// realtime ACK res — the identity mutation ops key off. Creates
		// register materialized state.
		msgSerials := make([]string, len(messages))
		for i, msg := range messages {
			msgSerials[i] = msg.Serial
			h.materialized.create(channel, msg)
		}
		h.writeDocument(rw, r, http.StatusCreated, map[string]any{"serials": msgSerials})
		return
	}
	h.writeDocument(rw, r, http.StatusCreated, map[string]any{})
}

// serveGetMessage implements GET /channels/{ch}/messages/{serial}
// (RSL11): the latest materialized state of one mutable message.
func (h *Handler) serveGetMessage(rw http.ResponseWriter, r *http.Request, channel string, identity authResult, serial string) {
	if !mutableChannel(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeMutableRequired, "this operation can only be performed on a channel with mutable messages enabled")
		return
	}
	if !identity.capability.Allows(auth.OpHistory, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit history")
		return
	}
	msg, ok := h.materialized.get(channel, serial)
	if !ok {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "message not found")
		return
	}
	if responseFormat(r) == protocol.FormatMsgpack {
		denormalizeMessageData(&msg)
	}
	h.writeDocument(rw, r, http.StatusOK, &msg)
}

// serveMessageVersions implements GET /channels/{ch}/messages/{serial}/
// versions (RSL14): every version of a mutable message, oldest first —
// the create plus each subsequent op. PoC: a single page (version counts
// are small; Link pagination omitted like REST presence, noted for M9).
func (h *Handler) serveMessageVersions(rw http.ResponseWriter, r *http.Request, channel string, identity authResult, serial string) {
	if !mutableChannel(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeMutableRequired, "this operation can only be performed on a channel with mutable messages enabled")
		return
	}
	if !identity.capability.Allows(auth.OpHistory, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit history")
		return
	}
	versions, ok := h.materialized.versions(channel, serial)
	if !ok {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "message not found")
		return
	}
	format := responseFormat(r)
	items := make([]any, 0, len(versions))
	for i := range versions {
		v := versions[i]
		if format == protocol.FormatMsgpack {
			denormalizeMessageData(&v)
		}
		items = append(items, &v)
	}
	h.writeDocument(rw, r, http.StatusOK, items)
}

// serveMutateMessage implements PATCH /channels/{ch}/messages/{serial}
// (RSL15): the body is ONE encoded message carrying the numeric action
// (1=update, 2=delete, 5=append), the new data/extras, and the mutator's
// MessageOperation in version. Responds {versionSerial}.
func (h *Handler) serveMutateMessage(rw http.ResponseWriter, r *http.Request, channel string, identity authResult, serial string) {
	if !mutableChannel(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeMutableRequired, "this operation can only be performed on a channel with mutable messages enabled")
		return
	}
	// Mutating is publishing (RSL15 rides the publish capability).
	if !identity.capability.Allows(auth.OpPublish, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit publish")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxMessageSize+1))
	if err != nil || len(body) > maxMessageSize {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid request body")
		return
	}
	var msg protocol.Message
	if err := protocol.UnmarshalAny(body, requestBodyFormat(r), &msg); err != nil {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid message body")
		return
	}
	switch msg.Action {
	case protocol.MessageActionUpdate, protocol.MessageActionDelete, protocol.MessageActionAppend:
	default:
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "unsupported message action")
		return
	}
	// T1.2: mint+mutate+append atomic per channel (serials.go).
	unlock := h.mint.lockChannel(channel)
	defer unlock()
	version := msg.Version
	if version == nil {
		version = &protocol.MessageVersion{}
	}
	version.Serial = h.mint.Mint(channel)
	version.Timestamp = time.Now().UnixMilli()
	normalizeMessageData(&msg)

	// B7: prepare without committing, publish, commit only on success — a
	// failed publish must not leave a phantom version. The channel lock is
	// held across prepare→publish→commit.
	op, commit, prob := h.materialized.prepareMutation(channel, serial, msg.Action, msg.Data, msg.Encoding, msg.Extras, version)
	if prob != nil {
		h.writeError(rw, r, prob.statusCode, prob.code, prob.message)
		return
	}
	op.ID = version.Serial + ":0"
	data, err := json.Marshal(op)
	if err != nil {
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "mutation failed")
		return
	}
	// REST mutations have no connection identity: no origin tag, like
	// REST publishes.
	if _, err := h.node.Publish(brokerChannel(channel), data, publishOptions(channel, "", version.Serial)...); err != nil {
		log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("rest mutation publish failed")
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "mutation failed")
		return
	}
	commit() // publish succeeded — apply the materialized state now
	h.writeDocument(rw, r, http.StatusOK, map[string]any{"versionSerial": version.Serial})
}

// serveBatchPresence implements GET /presence?channels=a,b (BAR1; pinned
// by ably-js rest/batch batchPresence): one BatchResult whose per-channel
// entries carry the member set or a capability error — partial failures
// are per channel, never the whole request.
func (h *Handler) serveBatchPresence(rw http.ResponseWriter, r *http.Request) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	channelsParam := r.URL.Query().Get("channels")
	if channelsParam == "" {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "missing channels parameter")
		return
	}
	channels := strings.Split(channelsParam, ",")
	format := responseFormat(r)
	results := make([]any, 0, len(channels))
	successCount, failureCount := 0, 0
	for _, channel := range channels {
		if !validChannelName(channel) || !identity.capability.Allows(auth.OpPresence, channel) {
			failureCount++
			results = append(results, map[string]any{
				"channel": channel,
				"error": map[string]any{
					"code": errCodeOperationNotPermitted, "statusCode": 401,
					"message": "capability does not permit presence",
				},
			})
			continue
		}
		members := h.presence.members(channel)
		items := make([]*protocol.PresenceMessage, 0, len(members))
		for _, m := range members {
			present := *m
			present.Action = protocol.PresencePresent
			if format == protocol.FormatMsgpack {
				denormalizePresenceData(&present)
			}
			items = append(items, &present)
		}
		successCount++
		results = append(results, map[string]any{"channel": channel, "presence": items})
	}
	h.writeDocument(rw, r, http.StatusOK, map[string]any{
		"successCount": successCount,
		"failureCount": failureCount,
		"results":      results,
	})
}

// batchPublishSpec is one RSC22 BatchPublishSpec.
type batchPublishSpec struct {
	Channels []string        `json:"channels"`
	Messages json.RawMessage `json:"messages"`
}

// serveBatchSpecs serves the RSC22 rest.batchPublish form: the body is
// an ARRAY of specs; the response is one BatchPublishResult per spec
// with per-channel partial results ({channel, messageId} on success,
// {channel, error} on capability/name failure) plus success/failure
// counts — a bad channel never fails the whole request.
func (h *Handler) serveBatchSpecs(rw http.ResponseWriter, r *http.Request, identity authResult, body []byte) {
	var specs []batchPublishSpec
	if err := json.Unmarshal(body, &specs); err != nil || len(specs) == 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid batch body")
		return
	}
	out := make([]any, 0, len(specs))
	for _, spec := range specs {
		results := make([]any, 0, len(spec.Channels))
		successCount, failureCount := 0, 0
		for _, channel := range spec.Channels {
			entry, problem := h.batchPublishOne(channel, spec.Messages, identity)
			if problem != nil {
				failureCount++
				results = append(results, map[string]any{
					"channel": channel,
					"error": map[string]any{
						"code": problem.code, "statusCode": statusOf(problem),
						"message": problem.message,
					},
				})
				continue
			}
			successCount++
			results = append(results, entry)
		}
		out = append(out, map[string]any{
			"successCount": successCount,
			"failureCount": failureCount,
			"results":      results,
		})
	}
	h.writeDocument(rw, r, http.StatusCreated, out)
}

// batchPublishOne publishes one spec's messages to one channel under the
// channel publish lock, returning the success entry or the per-channel
// problem.
func (h *Handler) batchPublishOne(channel string, rawMessages json.RawMessage, identity authResult) (map[string]any, *publishProblem) {
	if !validChannelName(channel) {
		return nil, &publishProblem{code: errCodeInvalidChannelName, statusCode: 400, message: "invalid channel name"}
	}
	if !identity.capability.Allows(auth.OpPublish, channel) {
		return nil, &publishProblem{code: errCodeOperationNotPermitted, statusCode: 401, message: "capability does not permit publish"}
	}
	unlock := h.mint.lockChannel(channel)
	defer unlock()
	var messages []*protocol.Message
	if err := json.Unmarshal(rawMessages, &messages); err != nil {
		var single protocol.Message
		if err := json.Unmarshal(rawMessages, &single); err != nil {
			return nil, &publishProblem{code: errCodeBadRequest, statusCode: 400, message: "invalid batch messages"}
		}
		messages = []*protocol.Message{&single}
	}
	idBase, err := newRESTIDBase()
	if err != nil {
		return nil, &publishProblem{code: errCodeInternal, statusCode: 500, message: "internal error"}
	}
	payloads, idemKeys, serials, problem := buildEnvelopes(messages, envelopeParams{
		clientID:   identity.clientID,
		mintSerial: func() string { return h.mint.Mint(channel) },
		newID: func(idx int) string {
			return fmt.Sprintf("%s:%d", idBase, idx)
		},
	})
	if problem != nil {
		return nil, problem
	}
	for i, data := range payloads {
		opts := publishOptions(channel, "", serials[i])
		if idemKeys[i] != "" {
			opts = append(opts, centrifuge.WithIdempotencyKey(idemKeys[i]),
				centrifuge.WithIdempotentResultTTL(idempotentResultTTL))
		}
		if _, err := h.node.Publish(brokerChannel(channel), data, opts...); err != nil {
			log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("batch publish failed")
			return nil, &publishProblem{code: errCodeInternal, statusCode: 500, message: "publish failed"}
		}
	}
	if mutableChannel(channel) {
		for _, msg := range messages {
			h.materialized.create(channel, msg)
		}
	}
	return map[string]any{"channel": channel, "messageId": idBase}, nil
}

// serveBatchPublish implements POST /messages (BO2, the batch publish
// API; pinned by ably-js request_batch_api_success via rest.request):
// one body {channels: [...], messages: <message|[]message>} publishes the
// same messages to every channel; the 201 response is a flat array of
// per-channel results. Per-channel partial failure reporting (HP4
// batchResponse) follows the SKIPPED upstream test and is out of scope —
// any invalid channel fails the whole batch.
func (h *Handler) serveBatchPublish(rw http.ResponseWriter, r *http.Request) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxMessageSize+1))
	if err != nil || len(body) > maxMessageSize {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid request body")
		return
	}
	var req struct {
		Channels []string        `json:"channels" msgpack:"channels"`
		Messages json.RawMessage `json:"messages" msgpack:"messages"`
	}
	format := requestBodyFormat(r)
	if format == protocol.FormatMsgpack {
		// Normalize once: decode the msgpack body to the JSON-safe
		// generic form — `any`, not a map, because the RSC22 spec form is
		// an ARRAY — so the shape sniff and per-channel re-decodes below
		// stay format-agnostic.
		var generic any
		if err := protocol.UnmarshalAny(body, format, &generic); err != nil {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid batch body")
			return
		}
		if body, err = json.Marshal(generic); err != nil {
			h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "internal error")
			return
		}
	}
	// Two wire forms share this endpoint: the rest.batchPublish API posts
	// an ARRAY of BatchPublishSpecs (RSC22 — per-channel partial results);
	// the request()-API form posts a single {channels, messages} object
	// (flat per-channel results, whole-batch validation).
	if trimmed := strings.TrimSpace(string(body)); strings.HasPrefix(trimmed, "[") {
		h.serveBatchSpecs(rw, r, identity, body)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Channels) == 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid batch body")
		return
	}
	rawMessages := strings.TrimSpace(string(req.Messages))
	if rawMessages == "" || rawMessages == "null" || rawMessages == "[]" {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid batch body: no messages")
		return
	}
	for _, channel := range req.Channels {
		if !validChannelName(channel) {
			h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
			return
		}
		if !identity.capability.Allows(auth.OpPublish, channel) {
			h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit publish")
			return
		}
	}

	results := make([]any, 0, len(req.Channels))
	for _, channel := range req.Channels {
		// One channel per closure so the publish lock (T1.2: mint+append
		// atomic, never two channel locks at once) releases per iteration.
		result, errResult := func() (map[string]any, *publishProblem) {
			unlock := h.mint.lockChannel(channel)
			defer unlock()
			// FRESH decode per channel: the publish core stamps
			// ids/serials in place; every channel needs its own envelopes.
			var messages []*protocol.Message
			if err := json.Unmarshal(req.Messages, &messages); err != nil {
				var single protocol.Message
				if err := json.Unmarshal(req.Messages, &single); err != nil {
					return nil, &publishProblem{code: errCodeBadRequest, statusCode: 400, message: "invalid batch messages"}
				}
				messages = []*protocol.Message{&single}
			}
			idBase, err := newRESTIDBase()
			if err != nil {
				return nil, &publishProblem{code: errCodeInternal, statusCode: 500, message: "internal error"}
			}
			payloads, idemKeys, serials, problem := buildEnvelopes(messages, envelopeParams{
				clientID:   identity.clientID,
				mintSerial: func() string { return h.mint.Mint(channel) },
				newID: func(idx int) string {
					return fmt.Sprintf("%s:%d", idBase, idx)
				},
			})
			if problem != nil {
				return nil, problem
			}
			for i, data := range payloads {
				opts := publishOptions(channel, "", serials[i])
				if idemKeys[i] != "" {
					opts = append(opts, centrifuge.WithIdempotencyKey(idemKeys[i]),
						centrifuge.WithIdempotentResultTTL(idempotentResultTTL))
				}
				if _, err := h.node.Publish(brokerChannel(channel), data, opts...); err != nil {
					log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("batch publish failed")
					return nil, &publishProblem{code: errCodeInternal, statusCode: 500, message: "publish failed"}
				}
			}
			if mutableChannel(channel) {
				for _, msg := range messages {
					h.materialized.create(channel, msg)
				}
			}
			return map[string]any{"channel": channel, "messageId": idBase}, nil
		}()
		if errResult != nil {
			h.writeError(rw, r, statusOf(errResult), errResult.code, errResult.message)
			return
		}
		results = append(results, result)
	}
	h.writeDocument(rw, r, http.StatusCreated, results)
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
// historyKind selects what a history read returns: message envelopes from
// the live channel, or presence events from the shadow channel
// (RSL3-adjacent GET .../presence/history).
type historyKind int

const (
	historyKindMessages historyKind = iota
	historyKindPresence
)

func (h *Handler) serveRESTHistory(rw http.ResponseWriter, r *http.Request, channel string, identity authResult, kind historyKind) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	// History reads require the history operation (40160).
	if !identity.capability.Allows(auth.OpHistory, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit history")
		return
	}
	readChannel := channel
	if kind == historyKindPresence {
		readChannel = presenceHistoryChannel(channel)
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

	// untilAttach (RSL2b-adjacent): ably-js translates history
	// {untilAttach: true} into from_serial=<attachSerial> — the ATTACHED
	// channelSerial (realtimechannel.ts history). The serial resolves to
	// its publication's offset by tag LOOKUP (see serials.go) and bounds
	// the read at that offset INCLUSIVE: the attach-point publication is
	// the newest pre-attach message. An attach point that has left the
	// retention window means nothing retained is pre-attach — empty page.
	var boundOffset uint64 // 0 = unbounded
	var boundEpoch string
	if fromSerial := q.Get("from_serial"); fromSerial != "" {
		var ok bool
		boundOffset, boundEpoch, ok = h.resolveSerialOffset(readChannel, fromSerial)
		if !ok {
			// Also reachable on page 2+ if the attach-point publication is
			// evicted mid-pagination: the empty page ends the walk early
			// even though older pre-attach messages may survive — marginal
			// under PoC retention windows, matching first-page behavior.
			h.writeHistoryPage(rw, r, kind, nil, limit, backwards, false, 0, "")
			return
		}
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
				h.writeHistoryPage(rw, r, kind, nil, limit, backwards, false, 0, "")
				return
			}
			low := uint64(1)
			if cursor > uint64(limit) {
				low = cursor - uint64(limit)
			}
			count := int(cursor - low)
			pubs, epoch, err = h.historyPubs(readChannel, centrifuge.WithLimit(count),
				centrifuge.WithSince(&centrifuge.StreamPosition{Offset: low - 1, Epoch: cursorEpoch}))
			// Partial eviction makes WithSince resume from the oldest
			// RETAINED publication, which can sit at/after the cursor —
			// without truncation the page re-serves what the previous
			// page already delivered and the cursor never advances (an
			// infinite Link walk on size-evicted streams). Everything
			// at/after the cursor is outside the requested window.
			kept := pubs[:0]
			for _, pub := range pubs {
				if pub.Offset < cursor {
					kept = append(kept, pub)
				}
			}
			pubs = kept
			reversePubs(pubs)
		} else {
			pubs, epoch, err = h.historyPubs(readChannel, centrifuge.WithLimit(limit),
				centrifuge.WithSince(&centrifuge.StreamPosition{Offset: cursor, Epoch: cursorEpoch}))
		}
	} else if backwards && boundOffset > 0 {
		// First page of a bounded backwards read: identical shape to a
		// cursor at boundOffset+1, so the window is [bound-limit+1, bound].
		low := uint64(1)
		if boundOffset >= uint64(limit) {
			low = boundOffset - uint64(limit) + 1
		}
		count := int(boundOffset - low + 1)
		pubs, epoch, err = h.historyPubs(readChannel, centrifuge.WithLimit(count),
			centrifuge.WithSince(&centrifuge.StreamPosition{Offset: low - 1, Epoch: boundEpoch}))
		// Same partial-eviction truncation as the cursor window above:
		// only publications at/below the attach-point bound belong to
		// this page.
		kept := pubs[:0]
		for _, pub := range pubs {
			if pub.Offset <= boundOffset {
				kept = append(kept, pub)
			}
		}
		pubs = kept
		reversePubs(pubs)
	} else {
		pubs, epoch, err = h.historyPubs(readChannel, centrifuge.WithLimit(limit), centrifuge.WithReverse(backwards))
	}
	if err != nil {
		log.Error().Err(err).Str("channel", channel).Str("transport", transportName).Msg("history read failed")
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "history read failed")
		return
	}

	if boundOffset > 0 && !backwards {
		// Forwards reads enforce the bound by truncation on every page
		// (from_serial survives into the page links); reaching the bound
		// ends pagination.
		kept := pubs[:0]
		for _, pub := range pubs {
			if pub.Offset <= boundOffset {
				kept = append(kept, pub)
			}
		}
		pubs = kept
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
	items := make([]any, 0, len(pubs))
	for _, pub := range pubs {
		var ts int64
		var item any
		if kind == historyKindPresence {
			var pm protocol.PresenceMessage
			if err := json.Unmarshal(pub.Data, &pm); err != nil {
				log.Warn().Str("channel", channel).Str("transport", transportName).Msg("skipping non-envelope presence publication in history")
				continue
			}
			ts = pm.Timestamp
			if format == protocol.FormatMsgpack {
				denormalizePresenceData(&pm)
			}
			item = &pm
		} else {
			var msg protocol.Message
			if err := json.Unmarshal(pub.Data, &msg); err != nil {
				// Not a canonical envelope (e.g. a native centrifugo publish
				// on a shared channel): skip rather than corrupt the page.
				log.Warn().Str("channel", channel).Str("transport", transportName).Msg("skipping non-envelope publication in history")
				continue
			}
			ts = msg.Timestamp
			if format == protocol.FormatMsgpack {
				// RSL4c1: binary payloads in msgpack response bodies are the
				// msgpack binary type; pop the canonical transport "base64".
				denormalizeMessageData(&msg)
			}
			item = &msg
		}
		if (hasStart && ts < startMS) || (hasEnd && ts > endMS) {
			continue
		}
		items = append(items, item)
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
		if boundOffset > 0 && highestOffset >= boundOffset {
			hasNext = false
		}
		nextCursor = highestOffset
	}
	h.writeHistoryPage(rw, r, kind, items, limit, backwards, hasNext, nextCursor, epoch)
}

// resolveSerialOffset maps a channelSerial to the broker offset of the
// publication that carried it — tag LOOKUP, never lexicographic
// comparison (serials.go). Used by the from_serial history bound.
func (h *Handler) resolveSerialOffset(channel, serial string) (uint64, string, bool) {
	pubs, epoch, err := h.historyPubs(channel,
		centrifuge.WithLimit(persistedHistorySize), centrifuge.WithReverse(true))
	if err != nil {
		return 0, "", false
	}
	for _, pub := range pubs {
		if pub.Tags[pubTagSerial] == serial {
			return pub.Offset, epoch, true
		}
	}
	return 0, "", false
}

func (h *Handler) historyPubs(channel string, opts ...centrifuge.HistoryOption) ([]*centrifuge.Publication, string, error) {
	res, err := h.node.History(brokerChannel(channel), opts...)
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
func (h *Handler) writeHistoryPage(rw http.ResponseWriter, r *http.Request, kind historyKind, items []any, limit int, backwards bool, hasNext bool, nextCursor uint64, epoch string) {
	if items == nil {
		items = []any{}
	}
	direction := "forwards"
	if backwards {
		direction = "backwards"
	}
	// The link URL must be the relative form `./<word>?<query>`: ably-js
	// getRelParams only matches /^\.\/(\w+)\?(.*)$/ (paginatedresource.ts)
	// — only the parsed QUERY is reused (the request path stays the
	// resource's own), so the word is cosmetic but must be one \w+ token:
	// presence history uses "./history".
	base := "./messages"
	if kind == historyKindPresence {
		base = "./history"
	}
	// start/end bounds must survive into the page links, or page 2+ of a
	// time-bounded query would return out-of-range items.
	bounds := ""
	if raw := r.URL.Query().Get("start"); raw != "" {
		bounds += "&start=" + url.QueryEscape(raw)
	}
	if raw := r.URL.Query().Get("end"); raw != "" {
		bounds += "&end=" + url.QueryEscape(raw)
	}
	if raw := r.URL.Query().Get("from_serial"); raw != "" {
		// The untilAttach bound survives into page links: backwards pages
		// continue via cursor (already below the bound); forwards pages
		// re-apply the truncation.
		bounds += "&from_serial=" + url.QueryEscape(raw)
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

// serveRESTPresence implements GET /channels/{channel}/presence: the
// current member set as a bare array of PresenceMessages with action
// present (RSL3-adjacent — ably-js rest presence get). Bounded by limit;
// presence sets are small enough that Link pagination is omitted (noted
// for M9 should a test demand it).
func (h *Handler) serveRESTPresence(rw http.ResponseWriter, r *http.Request, channel string, identity authResult) {
	if !validChannelName(channel) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeInvalidChannelName, "invalid channel name")
		return
	}
	// Reading presence requires the presence operation (40160).
	if !identity.capability.Allows(auth.OpPresence, channel) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "capability does not permit presence")
		return
	}
	limit := historyDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > historyMaxLimit {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	format := responseFormat(r)
	members := h.presence.members(channel)
	items := make([]*protocol.PresenceMessage, 0, len(members))
	for _, m := range members {
		if len(items) >= limit {
			break
		}
		present := *m
		present.Action = protocol.PresencePresent
		if format == protocol.FormatMsgpack {
			denormalizePresenceData(&present)
		}
		items = append(items, &present)
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
	subscribers := h.node.Hub().NumSubscribers(brokerChannel(channel))
	// Presence occupancy comes from the adapter-owned member set (the
	// centrifuge presence manager only sees native centrifugo clients).
	presence := len(h.presence.members(channel))
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

// corsExposedHeaders lets browser SDKs read pagination and error
// metadata cross-origin: without exposing Link, ably-js paginated
// resources silently stop after page one when the API lives on a
// different origin than the app. The list (and its casing) mirrors the
// real service's Access-Control-Expose-Headers.
const corsExposedHeaders = "Link,Transfer-Encoding,Content-Length,X-Ably-ErrorCode,X-Ably-ErrorMessage,X-Ably-ServerId,X-Ably-Cluster,Server"

// writeDocument writes a REST response document in the negotiated format
// (RSC8c). JSON is tab-indented like the real service's REST responses
// (cosmetic for SDKs, but side-by-side curls read identically).
func (h *Handler) writeDocument(rw http.ResponseWriter, r *http.Request, status int, v any) {
	format := responseFormat(r)
	var data []byte
	var err error
	if format == protocol.FormatMsgpack {
		data, err = protocol.MarshalAny(v, format)
		rw.Header().Set("Content-Type", contentTypeMsgPack)
	} else {
		data, err = json.MarshalIndent(v, "", "\t")
		rw.Header().Set("Content-Type", contentTypeJSON)
	}
	if err != nil {
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "failed to encode response")
		return
	}
	rw.WriteHeader(status)
	_, _ = rw.Write(data)
}

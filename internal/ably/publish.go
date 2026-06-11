package ably

// Shared publish core for the two publish surfaces — the realtime MESSAGE
// frame (RTL6, session.go) and REST POST /channels/{channel}/messages
// (RSL1, rest.go). Both build the full Ably message envelope server-side
// and call node.Publish directly (decision D3); only the verdict transport
// differs (NACK frame vs HTTP error). The core lives here so validation,
// envelope rules, size accounting and retention stay byte-identical across
// surfaces.

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"

	"github.com/centrifugal/centrifuge"
)

// Two-tier message retention, applied as publish-time history options
// (Ably's implicit-history model: every channel retains a short window,
// the persisted: namespace retains messages long-term — RSL2 history
// reads come from this storage). Hardcoded for the PoC; the M3 config
// follow-up lifts sizes/TTLs into configtypes.Ably. Unrelated to the
// centrifugo-native without_namespace history settings in
// config.ably-dev.json, which only govern native centrifugo clients.
const (
	// persistedNamespacePrefix marks channels with long-term retention,
	// mirroring the sandbox app fixture's `persisted` namespace.
	persistedNamespacePrefix = "persisted:"
	persistedHistorySize     = 1000
	persistedHistoryTTL      = 24 * time.Hour
	// Everything else gets Ably's implicit ~2 minute history window.
	ephemeralHistorySize = 100
	ephemeralHistoryTTL  = 2 * time.Minute
)

// idempotentResultTTL is the window within which a republish carrying the
// same client-supplied message id is deduplicated (RSL1k5: "for a period
// of time" — Ably documents ~2 minutes). The broker caches the original
// stream position per (channel, id) and drops the duplicate publication.
const idempotentResultTTL = 2 * time.Minute

// publishProblem is a publish verdict the surface translates to its wire
// form: the session NACKs the frame, the REST handler writes an Ably
// error response.
type publishProblem struct {
	code       int
	statusCode int
	message    string
}

// mutableChannel reports whether the channel has mutable-messages
// semantics (the mutableMessages channel rule): publishes return serials
// (TR4s/RSL1n) and per-serial mutation is allowed. PoC convention: the
// "mutable:" namespace (ably-js updates-deletes tests) and the "ai:"
// namespace (AIT's default) — a provisioning shim would set the flag per
// channel rule (M9).
func mutableChannel(name string) bool {
	return strings.HasPrefix(name, "mutable:") || strings.HasPrefix(name, "ai:")
}

// persistentChannel reports whether the channel gets the long-term
// retention tier. Mutable-messages channels are persisted alongside the
// persisted: namespace: AIT session resume and late-join hydration read
// channel history (load-history/load-conversation in the SDK), so a
// second tab opening an ai: conversation after the ephemeral 2-minute
// window must still find the backlog — this is the multi-client sync
// the use-chat demo advertises. (The materialized mutation store never
// evicts; this aligns raw history retention with it.)
func persistentChannel(name string) bool {
	return strings.HasPrefix(name, persistedNamespacePrefix) || mutableChannel(name)
}

// maxChannelNameBytes caps channel-name length (A4). Without a bound, a
// hostile REST publish or attach could use a multi-megabyte name that
// becomes a permanent key in the channel-keyed stores (serialMint,
// presence, materialized) — the trivial OOM vector in HIGH finding #3.
// 2048 bytes is far above any real channel name (the longest in the
// pinned SDK suites is 44) and matches the documented service limit;
// over-length names are rejected like any other invalid name (40010).
const maxChannelNameBytes = 2048

// validChannelName reports whether name is acceptable as an Ably channel
// name. Empty names, names beginning with ':', and names exceeding
// maxChannelNameBytes are invalid (error code 40010; pinned by ably-js
// channelattachempty/channelattachinvalid). The full Ably channel-name
// grammar is not enforced.
func validChannelName(name string) bool {
	return name != "" && !strings.HasPrefix(name, ":") && len(name) <= maxChannelNameBytes
}

// envelopeParams carries the publisher identity the envelope is built
// from.
type envelopeParams struct {
	// connectionID attributes every message to the publishing realtime
	// connection (TM2c). Empty for REST publishes — there is no
	// connection, and any per-message attribution (TM2h connectionKey)
	// was resolved by the caller before the core runs.
	connectionID string
	// clientID is the publisher's identity: the connection clientId for
	// realtime (RTN2d), the X-Ably-ClientId header identity for REST
	// (RSA7e2). Messages without a clientId are stamped with it
	// (RTL6g1b/RSL1m1); an incompatible explicit clientId is rejected
	// (RTL6g/RSL1m4).
	clientID string
	// newID generates the server-assigned message id for the message at
	// the given batch index when the client supplied none (TM2a).
	newID func(idx int) string
	// mintSerial returns a fresh channelSerial per message — every
	// message becomes its own publication, so each gets its own serial
	// (see serials.go). Both current callers set it; nil is tolerated
	// (skips serial stamping, serials returned nil).
	mintSerial func() string
}

// buildEnvelopes validates and envelopes an inbound message batch in
// place, then marshals every envelope to the canonical JSON-safe stored
// form. The marshaled payloads are both the publication payloads and the
// measurement basis for the maxMessageSize check, which must reject the
// batch before anything is published. A non-nil problem means nothing may
// be published.
//
// The returned idempotencyKeys slice is index-parallel to the payloads:
// a non-empty entry is the CLIENT-supplied message id, which carries
// idempotency intent (RSL1k2/RSL1k5) — the publish loops pass it to the
// broker for dedup. Server-generated ids never dedup (entry "").
//
// The returned serials slice is also index-parallel: each message's
// freshly-minted channelSerial, which the publish loops attach as the
// pubTagSerial publication tag. Message.Serial inside the envelope is
// the per-message form <channelSerial>:000. Nil when p.mintSerial is nil.
func buildEnvelopes(messages []*protocol.Message, p envelopeParams) ([][]byte, []string, []string, *publishProblem) {
	now := time.Now().UnixMilli()
	idempotencyKeys := make([]string, len(messages))
	var serials []string
	if p.mintSerial != nil {
		serials = make([]string, len(messages))
	}

	// Envelope every Message in full BEFORE any publish, so subscribers
	// never depend on SDK-side inheritance from an enclosing frame, and
	// so a rejected batch publishes nothing.
	for idx, msg := range messages {
		if msg == nil {
			return nil, nil, nil, &publishProblem{code: errCodeBadRequest, statusCode: 400, message: "null message"}
		}
		if msg.ClientID != "" && p.clientID != "" && msg.ClientID != p.clientID {
			// RTL6g/RSL1m4: an identified publisher can only publish
			// messages carrying its own clientId; an incompatible explicit
			// clientId is rejected by the service. Full capability
			// semantics are M4.
			return nil, nil, nil, &publishProblem{code: errCodeInvalidClientID, statusCode: 400,
				message: fmt.Sprintf("message clientId %q is incompatible with publisher clientId %q", msg.ClientID, p.clientID)}
		}
		clientSuppliedID := msg.ID != ""
		if msg.ID == "" {
			// TM2a: a unique id applied by Ably when the client supplied
			// none. A client-supplied id is preserved and doubles as the
			// idempotency key (RSL1k2).
			msg.ID = p.newID(idx)
		}
		if clientSuppliedID {
			idempotencyKeys[idx] = msg.ID
		}
		if msg.ClientID == "" {
			// RTL6g1b/RSL1m1: the service assigns the publisher's clientId
			// to messages published without one (no-op for unidentified
			// publishers).
			msg.ClientID = p.clientID
		}
		if p.connectionID != "" {
			// TM2c: the message is attributed to the publishing
			// connection.
			msg.ConnectionID = p.connectionID
		}
		// TM2h: connectionKey is request-scoped publisher input, resolved
		// to a ConnectionID by the REST surface before the core runs. It
		// must never reach storage or subscribers.
		msg.ConnectionKey = ""
		// Creates are creates: action and version are server-assigned by
		// the mutation surface — a client-supplied pair on a create would
		// mint a message masquerading as an op.
		msg.Action = protocol.MessageActionCreate
		msg.Version = nil
		if msg.Timestamp == 0 {
			// Stamp the server receipt time. SDKs never send a timestamp
			// on publish (they back-fill from the enclosing frame per
			// TM2f), so this is what subscribers observe.
			msg.Timestamp = now
		}
		if p.mintSerial != nil {
			// RTL15 territory: the message's channel position. Stamped
			// inside the envelope so history reads and the M8 mutation
			// surface see it; idx is always 0 — one message per
			// publication (see serials.go divergence note).
			cs := p.mintSerial()
			serials[idx] = cs
			msg.Serial = serial.MessageSerial(cs, 0)
		}
		// Normalize binary data to the canonical JSON-safe form (Base64
		// string + "base64" encoding segment, RSL4d1) before enveloping,
		// so the stored publication is format-agnostic. Everything else —
		// including every other encoding-chain segment — passes through
		// verbatim (see payload.go). No-op for JSON input: a JSON decode
		// never yields []byte data.
		normalizeMessageData(msg)
	}

	payloads := make([][]byte, 0, len(messages))
	totalSize := 0
	for _, msg := range messages {
		data, err := json.Marshal(msg)
		if err != nil {
			return nil, nil, nil, &publishProblem{code: errCodeInternal, statusCode: 500, message: err.Error()}
		}
		payloads = append(payloads, data)
		totalSize += len(data)
	}
	// CD2c/TO3l8: maxMessageSize limits the summed size of a batch's
	// messages (REST's RSL1i counterpart carries the same error code
	// 40009). Measured as the summed marshaled-envelope byte size AFTER
	// normalization; real Ably sums name + data + clientId + extras only,
	// so the server-added envelope fields — and for binary payloads the
	// ~33% Base64 inflation of the canonical form — make this adapter
	// marginally stricter, never looser.
	if totalSize > maxMessageSize {
		return nil, nil, nil, &publishProblem{code: errCodeMaxMessageLength, statusCode: 400,
			message: fmt.Sprintf("maximum message length exceeded (%d bytes, limit %d)", totalSize, maxMessageSize)}
	}
	return payloads, idempotencyKeys, serials, nil
}

// publishOptions are the node.Publish options for one enveloped message
// on channel: the retention tier the channel name selects, the
// publication's channelSerial tag (RTL15, see serials.go), plus — when
// the publish is attributed to a realtime connection — the origin tag
// echo=false subscriptions filter on (RTL7f, see session attach).
func publishOptions(channel string, originConnectionID string, channelSerial string) []centrifuge.PublishOption {
	opts := make([]centrifuge.PublishOption, 0, 2)
	tags := make(map[string]string, 2)
	if originConnectionID != "" {
		tags[pubTagOrigin] = originConnectionID
	}
	if channelSerial != "" {
		tags[pubTagSerial] = channelSerial
	}
	if len(tags) > 0 {
		opts = append(opts, centrifuge.WithTags(tags))
	}
	if persistentChannel(channel) {
		opts = append(opts, centrifuge.WithHistory(persistedHistorySize, persistedHistoryTTL))
	} else {
		opts = append(opts, centrifuge.WithHistory(ephemeralHistorySize, ephemeralHistoryTTL))
	}
	return opts
}

// newRESTIDBase returns the random base of server-assigned REST message
// ids: messages published without an id get `<base>:<idx>` (TM2a; the
// same shape SDK-side idempotent publishing generates per RSL1k1). 9
// bytes of entropy matches ably-js's MSG_ID_ENTROPY_BYTES.
func newRESTIDBase() (string, error) {
	entropy := make([]byte, 9)
	if _, err := rand.Read(entropy); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(entropy), nil
}

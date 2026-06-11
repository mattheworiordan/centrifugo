package ably

// POST /keys/{keyName}/requestToken (RSA8 token request exchange): the
// client proves key possession with an HMAC-signed TokenRequest (RSA9 —
// the mac IS the authentication; no Basic header accompanies it), and the
// adapter mints an Ably-JWT signed by the same key, which the shared
// verifier consumes on later connections.

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

// defaultTokenTTL is Ably's default token lifetime (TK2a: 60 minutes);
// maxTokenTTL caps requests at 24 hours (the Ably maximum — pinned by
// rest/auth "Should error with excessive ttl"); timestampTolerance bounds
// request-timestamp skew (RSA9d, pinned by "invalid timestamp" → 401).
const (
	defaultTokenTTL    = time.Hour
	maxTokenTTL        = 24 * time.Hour
	timestampTolerance = 15 * time.Minute
)

// flexInt64 tolerates a number arriving as either a JSON/msgpack number
// or a numeric string: authUrl indirection delivers token requests with
// query-string-typed values (echo's /qs_to_body — pinned by ably-js
// auth_useAuthUrl_mixed_authParams_qsParams), and the real service
// accepts them.
type flexInt64 int64

func (f *flexInt64) UnmarshalJSON(data []byte) error {
	raw := strings.Trim(string(data), "\"")
	if raw == "null" || raw == "" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return err
	}
	*f = flexInt64(v)
	return nil
}

var _ msgpack.CustomDecoder = (*flexInt64)(nil)

func (f *flexInt64) DecodeMsgpack(dec *msgpack.Decoder) error {
	v, err := dec.DecodeInterface()
	if err != nil {
		return err
	}
	switch t := v.(type) {
	case nil:
		*f = 0
	case string:
		if t == "" {
			*f = 0
			return nil
		}
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return err
		}
		*f = flexInt64(n)
	case int8:
		*f = flexInt64(t)
	case int16:
		*f = flexInt64(t)
	case int32:
		*f = flexInt64(t)
	case int64:
		*f = flexInt64(t)
	case uint8:
		*f = flexInt64(t)
	case uint16:
		*f = flexInt64(t)
	case uint32:
		*f = flexInt64(t)
	case uint64:
		*f = flexInt64(t)
	case float64:
		*f = flexInt64(int64(t))
	case float32:
		*f = flexInt64(int64(t))
	default:
		return fmt.Errorf("flexInt64: unsupported type %T", v)
	}
	return nil
}

// tokenRequestBody is a signed TokenRequest (TE2-TE6 wire shape). Pointer
// fields distinguish absent from zero: an absent ttl signs as the empty
// string and defaults to one hour.
type tokenRequestBody struct {
	KeyName    string     `json:"keyName"    msgpack:"keyName"`
	TTL        *flexInt64 `json:"ttl"        msgpack:"ttl"`
	Capability string     `json:"capability" msgpack:"capability"`
	ClientID   string     `json:"clientId"   msgpack:"clientId"`
	Timestamp  flexInt64  `json:"timestamp"  msgpack:"timestamp"`
	Nonce      string     `json:"nonce"      msgpack:"nonce"`
	MAC        string     `json:"mac"        msgpack:"mac"`
}

// tokenDetails is the response document (TD wire shape).
type tokenDetails struct {
	Token      string `json:"token"                msgpack:"token"`
	KeyName    string `json:"keyName"              msgpack:"keyName"`
	Issued     int64  `json:"issued"               msgpack:"issued"`
	Expires    int64  `json:"expires"              msgpack:"expires"`
	Capability string `json:"capability,omitempty" msgpack:"capability,omitempty"`
	ClientID   string `json:"clientId,omitempty"   msgpack:"clientId,omitempty"`
}

// serveRequestToken implements POST /keys/{keyName}/requestToken.
func (h *Handler) serveRequestToken(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
		return
	}
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/keys/")
	escaped = strings.TrimSuffix(escaped, "/requestToken")
	keyName, err := url.PathUnescape(escaped)
	if err != nil || keyName == "" || strings.ContainsRune(keyName, '/') {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "not found")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, maxMessageSize))
	if err != nil {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "failed to read request body")
		return
	}
	var req tokenRequestBody
	if err := protocol.UnmarshalAny(body, requestBodyFormat(r), &req); err != nil {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid token request body")
		return
	}
	if req.KeyName != keyName {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "keyName mismatch")
		return
	}
	if req.Nonce == "" || req.MAC == "" || req.Timestamp == 0 {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "incomplete token request")
		return
	}

	// A present ttl must be positive and within the 24h maximum: out of
	// range would sign one thing and issue another.
	if req.TTL != nil && (int64(*req.TTL) <= 0 || int64(*req.TTL) > maxTokenTTL.Milliseconds()) {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid ttl")
		return
	}
	// RSA9d: the request timestamp must be within tolerance of server
	// time; nonce reuse within the window is rejected (replay protection,
	// pinned by rest/auth "duplicate nonce" → 401).
	if skew := time.Since(time.UnixMilli(int64(req.Timestamp))); skew > timestampTolerance || skew < -timestampTolerance {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "token request timestamp out of range")
		return
	}
	// Capability shape validation (400) precedes intersection (401 on an
	// empty result) — pinned by rest/capability "Invalid capabilities" vs
	// the intersection rejection tests.
	if err := auth.ValidateCapabilityShape(req.Capability); err != nil {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, err.Error())
		return
	}
	// The mac covers the LITERAL fields as the SDK signed them: an absent
	// ttl contributes the empty string (ably-js auth.ts getTokenRequest).
	// Timestamp skew was bounded above (RSA9d); the nonce is burned only
	// AFTER the mac proves key possession, so unauthenticated requests
	// cannot deny a nonce to its legitimate owner.
	ttlStr := ""
	if req.TTL != nil {
		ttlStr = strconv.FormatInt(int64(*req.TTL), 10)
	}
	if !h.keys.VerifyTokenRequestMAC(keyName, ttlStr, req.Capability, req.ClientID,
		strconv.FormatInt(int64(req.Timestamp), 10), req.Nonce, req.MAC) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "invalid token request mac")
		return
	}
	if !h.nonces.use(req.Nonce) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "token request nonce already used")
		return
	}

	key, _ := h.keys.Lookup(keyName) // mac verified: the key exists
	effectiveCapability, ok, err := auth.IntersectCapability(key.Capability, req.Capability)
	if err != nil {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, err.Error())
		return
	}
	if !ok {
		// RSA6 territory: the requested capability has no intersection
		// with the key's grants.
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "requested capability has no intersection with the key capability")
		return
	}

	ttl := defaultTokenTTL
	if req.TTL != nil && int64(*req.TTL) > 0 {
		ttl = time.Duration(int64(*req.TTL)) * time.Millisecond
	}
	minted, err := h.keys.MintToken(keyName, req.ClientID, effectiveCapability, ttl)
	if err != nil {
		log.Error().Err(err).Str("transport", transportName).Msg("token minting failed")
		h.writeError(rw, r, http.StatusInternalServerError, errCodeInternal, "token minting failed")
		return
	}
	h.writeDocument(rw, r, http.StatusOK, &tokenDetails{
		Token:      minted.Token,
		KeyName:    keyName,
		Issued:     minted.Issued,
		Expires:    minted.Expires,
		Capability: effectiveCapability, // TD5: the token's EFFECTIVE capability
		ClientID:   req.ClientID,
	})
}

// nonceSweepInterval bounds how often use() performs a full expiry sweep
// (B4/M4): at most once per interval, so a burst of N distinct nonces costs
// O(N) total work rather than O(N²) (the old code swept the whole map on
// every call). Correctness does not depend on the sweep — use() checks each
// nonce's freshness exactly on access — so the sweep is pure memory
// reclamation and can run lazily.
const nonceSweepInterval = 2 * timestampTolerance

// nonceCache rejects nonce reuse within the timestamp tolerance window —
// together with the timestamp check this bounds replay of a captured
// signed TokenRequest.
type nonceCache struct {
	mu        sync.Mutex
	seen      map[string]time.Time
	lastSweep time.Time
	sweeps    int // count of full sweeps performed (test observability)
}

func newNonceCache() *nonceCache {
	return &nonceCache{seen: make(map[string]time.Time)}
}

// use records the nonce, reporting false when it was already used within
// the tolerance window. Freshness is checked per-access (so a stale entry
// not yet swept never causes a false replay-reject); a bounded periodic
// sweep reclaims expired entries without scanning the map on every call.
func (c *nonceCache) use(nonce string) bool {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepLocked(now)
	if t, seen := c.seen[nonce]; seen && now.Sub(t) <= 2*timestampTolerance {
		return false // replay within the window
	}
	c.seen[nonce] = now
	return true
}

// sweepLocked reclaims expired entries at most once per nonceSweepInterval,
// amortizing the O(n) scan to O(1) per use() across a burst (B4/M4).
func (c *nonceCache) sweepLocked(now time.Time) {
	if !c.lastSweep.IsZero() && now.Sub(c.lastSweep) < nonceSweepInterval {
		return
	}
	c.lastSweep = now
	c.sweeps++
	for n, t := range c.seen {
		if now.Sub(t) > 2*timestampTolerance {
			delete(c.seen, n)
		}
	}
}

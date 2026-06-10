package ably

// POST /keys/{keyName}/requestToken (RSA8 token request exchange): the
// client proves key possession with an HMAC-signed TokenRequest (RSA9 —
// the mac IS the authentication; no Basic header accompanies it), and the
// adapter mints an Ably-JWT signed by the same key, which the shared
// verifier consumes on later connections.

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/rs/zerolog/log"
)

// defaultTokenTTL is Ably's default token lifetime (TK2a: 60 minutes).
const defaultTokenTTL = time.Hour

// tokenRequestBody is a signed TokenRequest (TE2-TE6 wire shape). Pointer
// fields distinguish absent from zero: an absent ttl signs as the empty
// string and defaults to one hour.
type tokenRequestBody struct {
	KeyName    string `json:"keyName"    msgpack:"keyName"`
	TTL        *int64 `json:"ttl"        msgpack:"ttl"`
	Capability string `json:"capability" msgpack:"capability"`
	ClientID   string `json:"clientId"   msgpack:"clientId"`
	Timestamp  int64  `json:"timestamp"  msgpack:"timestamp"`
	Nonce      string `json:"nonce"      msgpack:"nonce"`
	MAC        string `json:"mac"        msgpack:"mac"`
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

	// A present ttl must be positive: a zero or negative value would sign
	// one thing and issue another (the 1h default), so it is rejected
	// outright.
	if req.TTL != nil && *req.TTL <= 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid ttl")
		return
	}
	// The mac covers the LITERAL fields as the SDK signed them: an absent
	// ttl contributes the empty string (ably-js auth.ts getTokenRequest).
	// Request timestamp staleness is NOT enforced (RSA9d divergence,
	// tracked for M9) — the mac itself proves key possession.
	ttlStr := ""
	if req.TTL != nil {
		ttlStr = strconv.FormatInt(*req.TTL, 10)
	}
	if !h.keys.VerifyTokenRequestMAC(keyName, ttlStr, req.Capability, req.ClientID,
		strconv.FormatInt(req.Timestamp, 10), req.Nonce, req.MAC) {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeInvalidCredentials, "invalid token request mac")
		return
	}

	ttl := defaultTokenTTL
	if req.TTL != nil && *req.TTL > 0 {
		ttl = time.Duration(*req.TTL) * time.Millisecond
	}
	minted, err := h.keys.MintToken(keyName, req.ClientID, req.Capability, ttl)
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
		Capability: req.Capability,
		ClientID:   req.ClientID,
	})
}

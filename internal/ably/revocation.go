package ably

// Token revocation (RSA17, pinned by ably-js rest/batch revokeTokens):
// POST /keys/{keyName}/revokeTokens revokes tokens matching target
// specifiers ("clientId:<id>" / "revocationKey:<key>"). Two enforcement
// surfaces:
//
//   - connect-time: verifyTokenString consults the registry — a revoked
//     token is refused with 40141 (registry-verified "token revoked");
//   - live connections: sessions matching a specifier are disconnected
//     with DISCONNECTED 40141 at appliesAt (now, or now+30s when
//     allowReauthMargin gives clients a reauth window — RSA17-adjacent;
//     the SDK renews via authCallback on 40141 like any token error).
//
// The registry is in-memory and per-node, like every other PoC store.
// Session identity is captured at CONNECT; an unidentified connection
// that later adopts a clientId via an AUTH frame is not re-registered,
// so revoking that clientId will not live-disconnect it (connect-time
// enforcement still applies on its next connection).

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

// reauthMargin is the enforcement delay granted by allowReauthMargin.
const reauthMargin = 30 * time.Second

type revocationEntry struct {
	keyName      string
	typ          string // "clientId" | "revocationKey"
	value        string
	issuedBefore int64 // ms: tokens issued strictly before this are revoked
	appliesAt    int64 // ms: enforcement moment
}

type revocationStore struct {
	mu      sync.Mutex
	entries []revocationEntry
}

func newRevocationStore() *revocationStore {
	return &revocationStore{}
}

func (s *revocationStore) add(e revocationEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
}

// revoked reports whether a token signed by keyName for clientID, issued
// at issuedAt (0 = unknown, treated as revocable — conservative), is
// revoked as of now.
func (s *revocationStore) revoked(keyName, clientID string, issuedAt, now int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.keyName != keyName || now < e.appliesAt {
			continue
		}
		if e.typ != "clientId" || e.value != clientID {
			// revocationKey targeting needs the x-ably-revocation-key
			// claim, which this PoC's tokens never carry — clientId is
			// the only effective specifier (matching the pinned tests).
			continue
		}
		if issuedAt == 0 || issuedAt < e.issuedBefore {
			return true
		}
	}
	return false
}

// sessionRecord is what the handler retains about a live realtime
// session for revocation enforcement — identity captured at connect.
type sessionRecord struct {
	keyName  string
	clientID string
	viaToken bool
	issuedAt int64
}

// sessionRegistry tracks live sessions so revocations can disconnect
// matching connections, and indexes comet sessions by connectionKey so
// the per-key HTTP routes (/comet/<key>/recv|send|close|disconnect)
// can find their session between requests.
type sessionRegistry struct {
	mu       sync.Mutex
	sessions map[*session]sessionRecord
	byKey    map[string]*session
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions: make(map[*session]sessionRecord),
		byKey:    make(map[string]*session),
	}
}

func (r *sessionRegistry) registerKey(key string, s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKey[key] = s
}

// deregisterKey removes the index entry only when it still maps to s —
// a late deregistration can never evict a successor session.
func (r *sessionRegistry) deregisterKey(key string, s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byKey[key] == s {
		delete(r.byKey, key)
	}
}

func (r *sessionRegistry) lookupKey(key string) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byKey[key]
}

func (r *sessionRegistry) register(s *session, rec sessionRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[s] = rec
}

func (r *sessionRegistry) deregister(s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, s)
}

// matching returns the sessions a revocation entry applies to.
func (r *sessionRegistry) matching(e revocationEntry) []*session {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*session
	for s, rec := range r.sessions {
		if !rec.viaToken || rec.keyName != e.keyName {
			continue
		}
		if e.typ != "clientId" || rec.clientID != e.value {
			continue
		}
		if rec.issuedAt == 0 || rec.issuedAt < e.issuedBefore {
			out = append(out, s)
		}
	}
	return out
}

// --- HTTP surface ---

// revokeRequestBody is the RSA17 revocation request document.
type revokeRequestBody struct {
	Targets           []string   `json:"targets"           msgpack:"targets"`
	IssuedBefore      *flexInt64 `json:"issuedBefore"      msgpack:"issuedBefore"`
	AllowReauthMargin bool       `json:"allowReauthMargin" msgpack:"allowReauthMargin"`
}

// serveRevokeTokens implements POST /keys/{keyName}/revokeTokens (RSA17;
// pinned by ably-js rest/batch revokeTokens): targets are
// "type:value" specifiers; valid types succeed with
// {target, issuedBefore, appliesAt}, invalid ones fail per-target with a
// 400 error entry. allowReauthMargin delays enforcement by 30s.
func (h *Handler) serveRevokeTokens(rw http.ResponseWriter, r *http.Request, keyName string) {
	identity, authErr := h.authenticate(r)
	if authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	// The revoking principal must be the key itself (the SDK refuses
	// token auth client-side with 40162; enforce the same here).
	if identity.viaToken || identity.keyName != keyName {
		h.writeError(rw, r, http.StatusUnauthorized, 40162, "token revocation requires key authentication with the target key")
		return
	}
	key, ok := h.keys.Lookup(keyName)
	if !ok {
		h.writeError(rw, r, http.StatusNotFound, errCodeNotFound, "key not found")
		return
	}
	if !key.RevocableTokens {
		h.writeError(rw, r, http.StatusUnauthorized, errCodeOperationNotPermitted, "token revocation is not enabled for this key")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxMessageSize+1))
	if err != nil || len(body) > maxMessageSize {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid request body")
		return
	}
	var req revokeRequestBody
	if err := protocol.UnmarshalAny(body, requestBodyFormat(r), &req); err != nil || len(req.Targets) == 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid revocation body")
		return
	}

	now := time.Now().UnixMilli()
	issuedBefore := now
	if req.IssuedBefore != nil {
		issuedBefore = int64(*req.IssuedBefore)
	}
	appliesAt := now
	if req.AllowReauthMargin {
		// +1ms: the margin is AT LEAST 30s — and on loopback the whole
		// request can complete inside the same millisecond as the
		// client's pre-request rest.time() reading, whose +30s the
		// pinned ably-js test asserts appliesAt is STRICTLY above (the
		// same local-speed artifact class as the heartbeat ping floor).
		appliesAt = now + reauthMargin.Milliseconds() + 1
	}

	results := make([]any, 0, len(req.Targets))
	successCount, failureCount := 0, 0
	for _, target := range req.Targets {
		typ, value, found := strings.Cut(target, ":")
		if !found || (typ != "clientId" && typ != "revocationKey") || value == "" {
			failureCount++
			results = append(results, map[string]any{
				"target": target,
				"error": map[string]any{
					"code": errCodeBadRequest, "statusCode": 400,
					"message": "invalid revocation target specifier",
				},
			})
			continue
		}
		entry := revocationEntry{
			keyName: keyName, typ: typ, value: value,
			issuedBefore: issuedBefore, appliesAt: appliesAt,
		}
		h.revocations.add(entry)
		h.scheduleRevocationEnforcement(entry)
		successCount++
		results = append(results, map[string]any{
			"target": target, "issuedBefore": issuedBefore, "appliesAt": appliesAt,
		})
	}
	h.writeDocument(rw, r, http.StatusOK, map[string]any{
		"successCount": successCount,
		"failureCount": failureCount,
		"results":      results,
	})
}

// scheduleRevocationEnforcement disconnects matching live sessions with
// DISCONNECTED 40141 at the entry's appliesAt (immediately when due).
func (h *Handler) scheduleRevocationEnforcement(e revocationEntry) {
	enforce := func() {
		for _, s := range h.registry.matching(e) {
			s.disconnectWithError(40141, 401, "token revoked")
		}
	}
	wait := time.Until(time.UnixMilli(e.appliesAt))
	if wait <= 0 {
		// Asynchronous so the HTTP response is not held behind session
		// write timeouts.
		go enforce()
		return
	}
	time.AfterFunc(wait, enforce)
}

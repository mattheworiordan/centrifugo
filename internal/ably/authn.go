package ably

// Unified request/connection authentication for both surfaces: token auth
// (Ably-JWT via the access_token query param on realtime upgrades —
// RTC1-territory, ably-js auth.ts — or an Authorization: Bearer header on
// REST) takes precedence over Basic key auth (RSA11: Basic header or key
// query param). Verify-only: tokens are externally minted against the
// static key store's keys.

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
)

// authResult is the resolved identity of an authenticated caller.
type authResult struct {
	// viaToken reports token authentication (identity comes from the
	// token claims; the RSA7e2 X-Ably-ClientId header applies only to
	// Basic auth).
	viaToken bool
	// clientID is the bound identity; empty = unidentified.
	clientID string
	// wildcardClientID reports a token whose clientId is the literal "*"
	// (RSA7b4): the caller has no identity but may assume any.
	wildcardClientID bool
	// capability governs the caller: the key's capability for Basic auth,
	// the x-ably-capability claim for token auth (falling back to the
	// signing key's capability when the claim is absent — Ably-JWT
	// semantics). Enforced on attach, publish and history.
	capability auth.Capability
	// keyName is the authenticating key (Basic) or signing key (token).
	keyName string
	// expires is the token exp in ms since epoch (0 = no expiry / Basic
	// auth): the realtime session disconnects with 40142 when it passes
	// (RTN15-territory) unless an AUTH renewal extends it first.
	expires int64
	// issuedAt is the token iat in ms (0 when absent) — revocation
	// matching (RSA17).
	issuedAt int64
}

// authProblem is the Ably error verdict of a failed authentication,
// written in-band by the realtime surface and as an HTTP error by REST.
type authProblem struct {
	code       int
	statusCode int
	message    string
}

// verifyTokenString resolves an Ably-JWT into an identity — the token
// half of authenticate, reused verbatim by the AUTH reauth frame (RTC8).
func (h *Handler) verifyTokenString(token string) (authResult, *authProblem) {
	claims, err := h.keys.VerifyToken(token)
	switch {
	case errors.Is(err, auth.ErrTokenExpired):
		// 40142: the client is expected to renew and retry (RSA4b).
		return authResult{}, &authProblem{code: 40142, statusCode: http.StatusUnauthorized, message: "token expired"}
	case err != nil:
		return authResult{}, &authProblem{code: errCodeInvalidCredentials, statusCode: http.StatusUnauthorized, message: "invalid token"}
	}
	capabilityJSON := claims.Capability
	if capabilityJSON == "" {
		// A token without an x-ably-capability claim inherits the
		// signing key's capability.
		if key, ok := h.keys.Lookup(claims.KeyName); ok {
			capabilityJSON = key.Capability
		}
	}
	capability, err := auth.ParseCapability(capabilityJSON)
	if err != nil {
		return authResult{}, &authProblem{code: errCodeInvalidCredentials, statusCode: http.StatusUnauthorized, message: "invalid token capability"}
	}
	res := authResult{
		viaToken:   true,
		capability: capability,
		keyName:    claims.KeyName,
		expires:    claims.Expires,
		issuedAt:   claims.IssuedAt,
	}
	if claims.ClientID == "*" {
		res.wildcardClientID = true // RSA7b4
	} else {
		// C3: a token clientId becomes a presence/attribution map key — reject
		// invalid UTF-8 here so it is caught on every surface that token-auths
		// (realtime connect, REST publish, comet), not only the connect path.
		if !validClientID(claims.ClientID) {
			return authResult{}, &authProblem{code: errCodeInvalidClientID, statusCode: http.StatusUnauthorized, message: "token clientId is not valid UTF-8"}
		}
		res.clientID = claims.ClientID
	}
	// RSA17/40141: a revoked token is refused outright.
	if h.revocations.revoked(claims.KeyName, res.clientID, claims.IssuedAt, time.Now().UnixMilli()) {
		return authResult{}, &authProblem{code: 40141, statusCode: http.StatusUnauthorized, message: "token revoked"}
	}
	return res, nil
}

// authenticate resolves the caller's identity. Token credentials win when
// both schemes are presented (SDKs send exactly one).
func (h *Handler) authenticate(r *http.Request) (authResult, *authProblem) {
	token := r.URL.Query().Get("access_token")
	if token == "" {
		if bearer := r.Header.Get("Authorization"); strings.HasPrefix(bearer, "Bearer ") {
			token = strings.TrimPrefix(bearer, "Bearer ")
			// SDKs Base64-encode the token in the Authorization header
			// (ably-js auth.ts getAuthHeaders); a raw token is also legal.
			// The discriminator is exact: JWTs always contain dots, Base64
			// output never does — so a dotted value is raw, anything else
			// is decoded.
			if !strings.Contains(token, ".") {
				if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
					token = string(decoded)
				}
			}
		}
	}
	if token != "" {
		return h.verifyTokenString(token)
	}

	key, err := h.keys.Authenticate(r)
	if err != nil {
		message := "invalid credentials"
		if errors.Is(err, auth.ErrNoCredentials) {
			message = "no credentials presented"
		}
		return authResult{}, &authProblem{code: errCodeInvalidCredentials, statusCode: http.StatusUnauthorized, message: message}
	}
	capability, err := auth.ParseCapability(key.Capability)
	if err != nil {
		// The store validates capability JSON at load, so this is a
		// programming error rather than a caller problem.
		return authResult{}, &authProblem{code: errCodeInternal, statusCode: http.StatusInternalServerError, message: "invalid key capability"}
	}
	return authResult{
		capability: capability,
		keyName:    key.APIKey.AppID + "." + key.APIKey.KeyID,
	}, nil
}

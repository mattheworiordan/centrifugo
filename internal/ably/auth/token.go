package auth

// Ably-JWT verification (verify-only: externally-minted tokens, RSA8g
// territory — the adapter does not require its own minting for clients
// that bring a JWT). Per the M0 spike (spikes/jwt-short-key.md):
// centrifugo's JWT library accepts arbitrary-length HMAC keys, and
// zero-padding short Ably key secrets to 32 bytes is HMAC-identical for
// secrets under 32 bytes (Go's crypto/hmac pads to the SHA-256 block
// anyway) — the padding is applied for spec parity all the same.

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cristalhq/jwt/v5"
)

// Errors returned by VerifyToken, mapped by callers to Ably error codes
// (40101 invalid credentials, 40142 token expired).
var (
	ErrTokenInvalid = errors.New("invalid token")
	ErrTokenExpired = errors.New("token expired")
)

// TokenClaims is the verified identity an Ably-JWT carries.
type TokenClaims struct {
	// KeyName is the signing key (the kid header, appId.keyId).
	KeyName string
	// ClientID is the x-ably-clientId claim: the token-bound identity.
	// The literal "*" is the wildcard identity (RSA7b4): the bearer may
	// assume any clientId.
	ClientID string
	// Capability is the x-ably-capability claim VERBATIM — a JSON string
	// mapping channel resources to operation arrays, enforced on attach,
	// publish, presence and history.
	Capability string
	// Expires is the token expiry (exp claim) in ms since epoch.
	Expires int64
}

// ablyJWTClaims is the wire shape of Ably-JWT claims.
type ablyJWTClaims struct {
	Exp        int64  `json:"exp"`
	Capability string `json:"x-ably-capability"`
	ClientID   string `json:"x-ably-clientId"`
}

// hmacKey returns the HMAC key for an Ably key secret: secrets shorter
// than 32 bytes are zero-padded to 32 (Ably-JWT signing convention).
func hmacKey(secret string) []byte {
	key := []byte(secret)
	if len(key) < 32 {
		padded := make([]byte, 32)
		copy(padded, key)
		return padded
	}
	return key
}

// VerifyToken parses and verifies an Ably-JWT (HS256, kid header naming
// the signing key as appId.keyId) against the store's keys and returns
// its claims. Expiry is enforced here (RSA4b-adjacent: the server rejects
// expired tokens with 40142 so clients renew).
func (s *KeyStore) VerifyToken(token string) (TokenClaims, error) {
	parsed, err := jwt.ParseNoVerify([]byte(token))
	if err != nil {
		return TokenClaims{}, fmt.Errorf("%w: %s", ErrTokenInvalid, "malformed JWT")
	}
	if alg := parsed.Header().Algorithm; alg != jwt.HS256 {
		return TokenClaims{}, fmt.Errorf("%w: unsupported algorithm %q", ErrTokenInvalid, alg)
	}
	key, ok := s.Lookup(parsed.Header().KeyID)
	if !ok {
		// Same uniform-failure posture as Authenticate: unknown kid is not
		// distinguishable from a bad signature.
		return TokenClaims{}, fmt.Errorf("%w: signature verification failed", ErrTokenInvalid)
	}
	verifier, err := jwt.NewVerifierHS(jwt.HS256, hmacKey(key.APIKey.KeySecret))
	if err != nil {
		return TokenClaims{}, fmt.Errorf("%w: %s", ErrTokenInvalid, "verifier construction failed")
	}
	if err := verifier.Verify(parsed); err != nil {
		return TokenClaims{}, fmt.Errorf("%w: signature verification failed", ErrTokenInvalid)
	}
	var claims ablyJWTClaims
	if err := json.Unmarshal(parsed.Claims(), &claims); err != nil {
		return TokenClaims{}, fmt.Errorf("%w: %s", ErrTokenInvalid, "malformed claims")
	}
	if claims.Exp != 0 && time.Now().Unix() >= claims.Exp {
		return TokenClaims{}, ErrTokenExpired
	}
	return TokenClaims{
		KeyName:    parsed.Header().KeyID,
		ClientID:   claims.ClientID,
		Capability: claims.Capability,
		Expires:    claims.Exp * 1000,
	}, nil
}

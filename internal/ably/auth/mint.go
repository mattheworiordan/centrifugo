package auth

// Ably-JWT minting for the requestToken exchange (RSA8): the adapter
// returns tokens its own verifier consumes — HS256 Ably-JWTs signed by
// the requesting key.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cristalhq/jwt/v5"
)

// MintedToken is a freshly minted token plus its TokenDetails metadata.
type MintedToken struct {
	Token   string
	Issued  int64 // ms since epoch
	Expires int64 // ms since epoch
}

// MintToken signs an Ably-JWT with the named key. clientID and capability
// are embedded as claims when non-empty; ttl bounds the exp claim.
func (s *KeyStore) MintToken(keyName, clientID, capability string, ttl time.Duration) (MintedToken, error) {
	key, ok := s.Lookup(keyName)
	if !ok {
		return MintedToken{}, fmt.Errorf("unknown key %q", keyName)
	}
	signer, err := jwt.NewSignerHS(jwt.HS256, hmacKey(key.APIKey.KeySecret))
	if err != nil {
		return MintedToken{}, err
	}
	issued := time.Now()
	expires := issued.Add(ttl)
	claims := map[string]any{
		"iat": issued.Unix(),
		"exp": expires.Unix(),
	}
	if clientID != "" {
		claims["x-ably-clientId"] = clientID
	}
	if capability != "" {
		claims["x-ably-capability"] = capability
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return MintedToken{}, err
	}
	token, err := jwt.NewBuilder(signer, jwt.WithKeyID(keyName)).Build(json.RawMessage(payload))
	if err != nil {
		return MintedToken{}, err
	}
	return MintedToken{
		Token:   token.String(),
		Issued:  issued.UnixMilli(),
		Expires: expires.UnixMilli(),
	}, nil
}

// VerifyTokenRequestMAC recomputes the HMAC-SHA256 signature of a signed
// TokenRequest and compares it to the presented mac. The canonical sign
// text is exactly what SDKs produce (ably-js auth.ts getTokenRequest):
//
//	keyName \n ttl \n capability \n clientId \n timestamp \n nonce \n
//
// with absent ttl/capability/clientId contributing empty strings.
func (s *KeyStore) VerifyTokenRequestMAC(keyName, ttl, capability, clientID, timestamp, nonce, mac string) bool {
	key, ok := s.Lookup(keyName)
	if !ok {
		return false
	}
	signText := keyName + "\n" + ttl + "\n" + capability + "\n" + clientID + "\n" + timestamp + "\n" + nonce + "\n"
	return hmacEqual(signText, key.APIKey.KeySecret, mac)
}

// hmacEqual reports whether mac is the Base64 HMAC-SHA256 of text under
// key, compared in constant time over the raw digest bytes. The key runs
// through hmacKey for parity with the JWT paths — HMAC-equivalent to the
// raw secret SDKs sign with (zero-padding below 32 bytes changes nothing:
// crypto/hmac pads to the 64-byte block regardless, per the M0 spike).
func hmacEqual(text, key, mac string) bool {
	presented, err := base64.StdEncoding.DecodeString(mac)
	if err != nil {
		return false
	}
	h := hmac.New(sha256.New, hmacKey(key))
	h.Write([]byte(text))
	return hmac.Equal(h.Sum(nil), presented)
}

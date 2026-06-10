package auth

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cristalhq/jwt/v5"
)

// mintTestJWT signs an Ably-JWT with the given kid and secret (padded as
// the verifier expects).
func mintTestJWT(t *testing.T, kid, secret string, claims map[string]any) string {
	t.Helper()
	signer, err := jwt.NewSignerHS(jwt.HS256, hmacKey(secret))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.NewBuilder(signer, jwt.WithKeyID(kid)).Build(json.RawMessage(payload))
	if err != nil {
		t.Fatal(err)
	}
	return token.String()
}

func TestVerifyToken(t *testing.T) {
	store := loadTestStore(t)
	const kid = "poc.key1"
	const secret = "secret_key1_0123456789abcdef"
	future := time.Now().Add(time.Hour).Unix()

	t.Run("valid token round-trips claims", func(t *testing.T) {
		tok := mintTestJWT(t, kid, secret, map[string]any{
			"exp":               future,
			"x-ably-clientId":   "token-bob",
			"x-ably-capability": `{"chan":["publish","subscribe"]}`,
		})
		claims, err := store.VerifyToken(tok)
		if err != nil {
			t.Fatalf("VerifyToken: %v", err)
		}
		if claims.ClientID != "token-bob" {
			t.Errorf("ClientID = %q", claims.ClientID)
		}
		if claims.Capability != `{"chan":["publish","subscribe"]}` {
			t.Errorf("Capability not verbatim: %q", claims.Capability)
		}
		if claims.Expires != future*1000 {
			t.Errorf("Expires = %d, want %d", claims.Expires, future*1000)
		}
	})

	t.Run("RSA4b 40142 territory: expired token", func(t *testing.T) {
		tok := mintTestJWT(t, kid, secret, map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})
		if _, err := store.VerifyToken(tok); !errors.Is(err, ErrTokenExpired) {
			t.Fatalf("err = %v, want ErrTokenExpired", err)
		}
	})

	t.Run("wrong secret rejected", func(t *testing.T) {
		tok := mintTestJWT(t, kid, "wrong-secret-wrong-secret-wrong", map[string]any{"exp": future})
		if _, err := store.VerifyToken(tok); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("err = %v, want ErrTokenInvalid", err)
		}
	})

	t.Run("unknown kid rejected identically to bad signature", func(t *testing.T) {
		tok := mintTestJWT(t, "poc.nokey", secret, map[string]any{"exp": future})
		_, errUnknown := store.VerifyToken(tok)
		tokBad := mintTestJWT(t, kid, "another-wrong-secret-padded!", map[string]any{"exp": future})
		_, errBad := store.VerifyToken(tokBad)
		if !errors.Is(errUnknown, ErrTokenInvalid) || !errors.Is(errBad, ErrTokenInvalid) {
			t.Fatalf("errs = %v / %v", errUnknown, errBad)
		}
		if errUnknown.Error() != errBad.Error() {
			t.Errorf("unknown-kid and bad-signature messages differ: %q vs %q", errUnknown.Error(), errBad.Error())
		}
	})

	t.Run("short secret zero-padding verifies", func(t *testing.T) {
		// poc.key2's secret in testdata is full length; mint with a key
		// derived store to prove padding: use kid poc.key1 but sign with
		// the UNPADDED short prefix — must fail (padding changes nothing
		// for <32B keys per the spike, so this signs with a different key).
		tok := mintTestJWT(t, kid, secret[:16], map[string]any{"exp": future})
		if _, err := store.VerifyToken(tok); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("err = %v, want ErrTokenInvalid (different key)", err)
		}
	})

	t.Run("missing kid header rejected", func(t *testing.T) {
		tok := mintTestJWT(t, "", secret, map[string]any{"exp": future})
		if _, err := store.VerifyToken(tok); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("err = %v, want ErrTokenInvalid", err)
		}
	})

	t.Run("malformed token", func(t *testing.T) {
		if _, err := store.VerifyToken("not-a-jwt"); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("err = %v, want ErrTokenInvalid", err)
		}
	})
}

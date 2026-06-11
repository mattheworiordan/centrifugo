package auth

// Static multi-key store for the Ably protocol adapter PoC. Loads the
// keys of a single static Ably app from a JSON fixture (the harness
// static-app.json, mirroring a sandbox POST /apps response) so Basic
// auth (RSA11) can verify any fixture key and later milestones can read
// per-key capabilities. The single-key Authenticator in auth.go is
// lifted code and stays untouched; the store builds alongside it.

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Key is a single API key held by a KeyStore.
type Key struct {
	// APIKey is the parsed `appId.keyId:keySecret` triple (RSA11 key
	// format).
	APIKey APIKey
	// Capability is the key's capability exactly as it appears in the
	// fixture: a JSON string mapping channel resources to operation
	// arrays. Preserved VERBATIM — consumers deep-compare parsed
	// capabilities and array order matters, so the store never
	// re-encodes it.
	Capability string
	// Raw is the full `appId.keyId:keySecret` key string.
	Raw string
	// RevocableTokens reports whether the key may revoke tokens it
	// signed (RSA17 / the fixture's revocableTokens flag).
	RevocableTokens bool
}

// KeyStore holds the API keys of one static Ably app, indexed by key
// name (`appId.keyId`).
type KeyStore struct {
	appID string
	keys  map[string]Key
	// dummy is the compare target for unknown key names so they take
	// the same constant-time-compare path as wrong secrets. See
	// Authenticate.
	dummy []byte
}

// fixtureApp mirrors the subset of the static app fixture JSON the
// store needs. Unknown fields (namespaces, channels, cipher, limits)
// are ignored.
type fixtureApp struct {
	AppID string       `json:"appId"`
	Keys  []fixtureKey `json:"keys"`
}

type fixtureKey struct {
	KeyName         string `json:"keyName"`
	KeySecret       string `json:"keySecret"`
	KeyStr          string `json:"keyStr"`
	Capability      string `json:"capability"`
	RevocableTokens bool   `json:"revocableTokens"`
}

// LoadKeyStore reads a static Ably app fixture JSON file and returns a
// KeyStore with all of its keys. Each fixture key must carry a keyStr
// (or a keyName+keySecret pair to derive one) in the RSA11 format
// `appId.keyId:keySecret`, belong to the fixture's app, and have a
// unique key name.
func LoadKeyStore(path string) (*KeyStore, error) {
	data, err := os.ReadFile(path) //nolint:gosec // Path comes from operator-provided configuration, read once at startup.
	if err != nil {
		return nil, fmt.Errorf("ably keystore: %w", err)
	}
	var app fixtureApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("ably keystore: parse %s: %w", path, err)
	}
	if app.AppID == "" {
		return nil, fmt.Errorf("ably keystore: %s: missing appId", path)
	}
	if len(app.Keys) == 0 {
		return nil, fmt.Errorf("ably keystore: %s: no keys", path)
	}

	store := &KeyStore{
		appID: app.AppID,
		keys:  make(map[string]Key, len(app.Keys)),
	}
	for i, fk := range app.Keys {
		raw := fk.KeyStr
		if raw == "" {
			if fk.KeyName == "" || fk.KeySecret == "" {
				return nil, fmt.Errorf("ably keystore: %s: key %d has neither keyStr nor keyName+keySecret", path, i)
			}
			raw = fk.KeyName + ":" + fk.KeySecret
		}
		parsed, err := ParseAPIKey(raw)
		if err != nil {
			return nil, fmt.Errorf("ably keystore: %s: key %d: %w", path, i, err)
		}
		name := parsed.AppID + "." + parsed.KeyID
		if parsed.AppID != app.AppID {
			return nil, fmt.Errorf("ably keystore: %s: key %q does not belong to app %q", path, name, app.AppID)
		}
		if fk.KeyName != "" && fk.KeyName != name {
			return nil, fmt.Errorf("ably keystore: %s: keyName %q inconsistent with keyStr name %q", path, fk.KeyName, name)
		}
		if fk.KeySecret != "" && fk.KeySecret != parsed.KeySecret {
			return nil, fmt.Errorf("ably keystore: %s: key %q keySecret inconsistent with keyStr", path, name)
		}
		if _, dup := store.keys[name]; dup {
			return nil, fmt.Errorf("ably keystore: %s: duplicate key %q", path, name)
		}
		if fk.Capability != "" && !json.Valid([]byte(fk.Capability)) {
			return nil, fmt.Errorf("ably keystore: %s: key %q capability is not valid JSON", path, name)
		}
		store.keys[name] = Key{
			APIKey:          parsed,
			Capability:      fk.Capability,
			Raw:             raw,
			RevocableTokens: fk.RevocableTokens,
		}
		if store.dummy == nil {
			// Sized like a real key so the dummy compare in Authenticate
			// is timing-representative. A presented credential could in
			// principle equal these zero bytes; correctness relies on the
			// `!found` guard in Authenticate, not on this value being
			// unmatchable.
			store.dummy = make([]byte, len(raw))
		}
	}
	return store, nil
}

// AppID returns the app ID the stored keys belong to.
func (s *KeyStore) AppID() string {
	return s.appID
}

// Lookup returns the key with the given key name (`appId.keyId`).
func (s *KeyStore) Lookup(keyName string) (Key, bool) {
	k, ok := s.keys[keyName]
	return k, ok
}

// Authenticate extracts the presented API key from r (Basic auth header
// preferred, then `key` query parameter) and verifies it against the
// stored key with the matching name (RSA11 key format). It returns
// ErrNoCredentials when no key is presented and ErrInvalidKey when
// verification fails.
//
// An unknown key name deliberately takes the same code path as a bad
// secret: the presented credential is compared in constant time against
// a dummy value and the same ErrInvalidKey is returned, so a client
// cannot distinguish "no such key" from "wrong secret" via the error or
// a trivial timing oracle. This is server-side hardening, not a spec
// requirement; the caller maps it to Ably's 40101 invalid-credentials
// error.
func (s *KeyStore) Authenticate(r *http.Request) (Key, error) {
	presented, ok := extractKey(r)
	if !ok {
		return Key{}, ErrNoCredentials
	}
	name, _, _ := strings.Cut(presented, ":")
	key, found := s.keys[name]
	expected := s.dummy
	if found {
		expected = []byte(key.Raw)
	}
	// The compare runs unconditionally before the `found` check so both
	// failure modes do the same work.
	if subtle.ConstantTimeCompare([]byte(presented), expected) != 1 || !found {
		return Key{}, ErrInvalidKey
	}
	return key, nil
}

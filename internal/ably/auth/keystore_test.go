package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const testFixturePath = "testdata/static-app.json"

func loadTestStore(t *testing.T) *KeyStore {
	t.Helper()
	store, err := LoadKeyStore(testFixturePath)
	if err != nil {
		t.Fatalf("LoadKeyStore(%q): %v", testFixturePath, err)
	}
	return store
}

func TestLoadKeyStore(t *testing.T) {
	store := loadTestStore(t)

	if got := store.AppID(); got != "poc" {
		t.Errorf("AppID() = %q, want %q", got, "poc")
	}

	hits := []struct {
		keyName string
		wantRaw string
	}{
		{keyName: "poc.key0", wantRaw: "poc.key0:secret_key0_0123456789abcdef"},
		{keyName: "poc.key1", wantRaw: "poc.key1:secret_key1_0123456789abcdef"},
		{keyName: "poc.key2", wantRaw: "poc.key2:secret_key2_0123456789abcdef"},
	}
	for _, tc := range hits {
		t.Run("lookup hit "+tc.keyName, func(t *testing.T) {
			key, ok := store.Lookup(tc.keyName)
			if !ok {
				t.Fatalf("Lookup(%q) = miss, want hit", tc.keyName)
			}
			if key.Raw != tc.wantRaw {
				t.Errorf("Lookup(%q).Raw = %q, want %q", tc.keyName, key.Raw, tc.wantRaw)
			}
			wantName := key.APIKey.AppID + "." + key.APIKey.KeyID
			if wantName != tc.keyName {
				t.Errorf("Lookup(%q) parsed key name = %q", tc.keyName, wantName)
			}
			if key.Capability == "" {
				t.Errorf("Lookup(%q).Capability is empty", tc.keyName)
			}
		})
	}

	misses := []string{"poc.unknown", "other.key0", "key0", ""}
	for _, keyName := range misses {
		t.Run("lookup miss "+keyName, func(t *testing.T) {
			if _, ok := store.Lookup(keyName); ok {
				t.Errorf("Lookup(%q) = hit, want miss", keyName)
			}
		})
	}
}

func TestLoadKeyStoreErrors(t *testing.T) {
	writeFixture := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "static-app.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return path
	}

	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadKeyStore(filepath.Join(t.TempDir(), "nope.json")); err == nil {
			t.Fatal("LoadKeyStore on missing file: want error")
		}
	})

	t.Run("keyStr derived from keyName+keySecret", func(t *testing.T) {
		path := writeFixture(t, `{"appId": "poc", "keys": [{"keyName": "poc.key9", "keySecret": "derivedsecret"}]}`)
		store, err := LoadKeyStore(path)
		if err != nil {
			t.Fatalf("LoadKeyStore: %v", err)
		}
		key, ok := store.Lookup("poc.key9")
		if !ok {
			t.Fatal(`Lookup("poc.key9") = miss, want hit`)
		}
		if want := "poc.key9:derivedsecret"; key.Raw != want {
			t.Errorf("Raw = %q, want derived %q", key.Raw, want)
		}
	})

	cases := []struct {
		name    string
		content string
	}{
		{name: "malformed json", content: `{"appId": "poc",`},
		{name: "missing appId", content: `{"keys": [{"keyStr": "poc.key0:secret"}]}`},
		{name: "no keys", content: `{"appId": "poc", "keys": []}`},
		{name: "key missing keyStr and keyName+keySecret", content: `{"appId": "poc", "keys": [{"capability": "{}"}]}`},
		{name: "invalid keyStr format", content: `{"appId": "poc", "keys": [{"keyStr": "not-a-key"}]}`},
		{name: "key from different app", content: `{"appId": "poc", "keys": [{"keyStr": "other.key0:secret"}]}`},
		{name: "keyName inconsistent with keyStr", content: `{"appId": "poc", "keys": [{"keyName": "poc.key1", "keyStr": "poc.key0:secret"}]}`},
		{name: "keySecret inconsistent with keyStr", content: `{"appId": "poc", "keys": [{"keyName": "poc.key0", "keySecret": "different", "keyStr": "poc.key0:secret"}]}`},
		{name: "duplicate keyName", content: `{"appId": "poc", "keys": [{"keyStr": "poc.key0:secret"}, {"keyStr": "poc.key0:secret2"}]}`},
		{name: "capability not valid JSON", content: `{"appId": "poc", "keys": [{"keyStr": "poc.key0:secret", "capability": "{broken"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFixture(t, tc.content)
			if _, err := LoadKeyStore(path); err == nil {
				t.Errorf("LoadKeyStore: want error for %s", tc.name)
			}
		})
	}
}

func TestKeyStoreAuthenticate(t *testing.T) {
	store := loadTestStore(t)

	makeReq := func(setup func(*http.Request)) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if setup != nil {
			setup(r)
		}
		return r
	}

	tests := []struct {
		name        string
		req         *http.Request
		wantKeyName string
		wantErr     error
	}{
		{
			// RSA11: key string `appId.keyId:keySecret` split across the
			// Basic auth username and password.
			name: "RSA11 basic auth header",
			req: makeReq(func(r *http.Request) {
				r.SetBasicAuth("poc.key1", "secret_key1_0123456789abcdef")
			}),
			wantKeyName: "poc.key1",
		},
		{
			name:        "RSA11 key query parameter",
			req:         httptest.NewRequest(http.MethodGet, "/?key=poc.key2:secret_key2_0123456789abcdef", nil),
			wantKeyName: "poc.key2",
		},
		{
			name: "basic header beats query param when both present",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/?key=poc.key0:wrong", nil)
				r.SetBasicAuth("poc.key0", "secret_key0_0123456789abcdef")
				return r
			}(),
			wantKeyName: "poc.key0",
		},
		{
			name:    "no credentials",
			req:     makeReq(nil),
			wantErr: ErrNoCredentials,
		},
		{
			name:    "wrong secret",
			req:     makeReq(func(r *http.Request) { r.SetBasicAuth("poc.key1", "wrong") }),
			wantErr: ErrInvalidKey,
		},
		{
			// An unknown key name must not be distinguishable from a
			// wrong secret (hardening, see Authenticate).
			name:    "unknown keyName",
			req:     makeReq(func(r *http.Request) { r.SetBasicAuth("poc.nosuchkey", "secret_key1_0123456789abcdef") }),
			wantErr: ErrInvalidKey,
		},
		{
			name:    "unknown key via query parameter",
			req:     httptest.NewRequest(http.MethodGet, "/?key=other.key0:secret", nil),
			wantErr: ErrInvalidKey,
		},
		{
			name:    "malformed key without colon",
			req:     httptest.NewRequest(http.MethodGet, "/?key=garbage", nil),
			wantErr: ErrInvalidKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, err := store.Authenticate(tc.req)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Authenticate err = %v, want %v", err, tc.wantErr)
				}
				if key != (Key{}) {
					t.Errorf("Authenticate returned key %+v on error, want zero Key", key)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authenticate: %v, want success", err)
			}
			gotName := key.APIKey.AppID + "." + key.APIKey.KeyID
			if gotName != tc.wantKeyName {
				t.Errorf("Authenticate key = %q, want %q", gotName, tc.wantKeyName)
			}
			want, ok := store.Lookup(tc.wantKeyName)
			if !ok || key != want {
				t.Errorf("Authenticate key = %+v, want Lookup(%q) = %+v", key, tc.wantKeyName, want)
			}
		})
	}
}

// TestKeyStoreAuthenticateUniformFailure asserts the uniform-failure
// hardening directly: a wrong secret and an unknown key name yield the
// exact same error value, so neither the error type nor its message
// leaks whether the key name exists.
func TestKeyStoreAuthenticateUniformFailure(t *testing.T) {
	store := loadTestStore(t)

	wrongSecret := httptest.NewRequest(http.MethodGet, "/?key=poc.key0:wrong", nil)
	unknownKey := httptest.NewRequest(http.MethodGet, "/?key=poc.nosuchkey:wrong", nil)

	_, errWrongSecret := store.Authenticate(wrongSecret)
	_, errUnknownKey := store.Authenticate(unknownKey)

	if !errors.Is(errWrongSecret, ErrInvalidKey) {
		t.Fatalf("wrong secret err = %v, want ErrInvalidKey", errWrongSecret)
	}
	if errWrongSecret != errUnknownKey { //nolint:errorlint // identity check is the point: errors must be indistinguishable.
		t.Errorf("unknown key err = %v, wrong secret err = %v: must be identical", errUnknownKey, errWrongSecret)
	}
}

// TestKeyStoreCapabilityVerbatim checks the store round-trips capability
// strings byte-for-byte from the fixture. Downstream tests deep-compare
// parsed capabilities and operation array order matters, so the store
// must never re-encode them.
func TestKeyStoreCapabilityVerbatim(t *testing.T) {
	store := loadTestStore(t)

	// Re-read the fixture independently so the comparison is against the
	// file's exact contents, not the store's own decoding.
	data, err := os.ReadFile(testFixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var raw struct {
		Keys []struct {
			KeyName    string `json:"keyName"`
			Capability string `json:"capability"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(raw.Keys) == 0 {
		t.Fatal("fixture has no keys")
	}

	for _, fk := range raw.Keys {
		t.Run(fk.KeyName, func(t *testing.T) {
			key, ok := store.Lookup(fk.KeyName)
			if !ok {
				t.Fatalf("Lookup(%q) = miss", fk.KeyName)
			}
			if key.Capability != fk.Capability {
				t.Errorf("Capability = %q, want fixture verbatim %q", key.Capability, fk.Capability)
			}
		})
	}

	// Operation array order must survive: canpublish:andpresence on key1
	// is ["presence","publish"] in exactly that order.
	key1, ok := store.Lookup("poc.key1")
	if !ok {
		t.Fatal(`Lookup("poc.key1") = miss`)
	}
	var capability map[string][]string
	if err := json.Unmarshal([]byte(key1.Capability), &capability); err != nil {
		t.Fatalf("parse key1 capability: %v", err)
	}
	want := []string{"presence", "publish"}
	if got := capability["canpublish:andpresence"]; !reflect.DeepEqual(got, want) {
		t.Errorf(`capability["canpublish:andpresence"] = %v, want %v (order matters)`, got, want)
	}
}

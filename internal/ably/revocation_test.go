package ably

// Token revocation (RSA17): per-target results, live-session disconnect
// with 40141, connect-time rejection, and the reauth margin.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cristalhq/jwt/v5"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// key4 (poc.key4) carries revocableTokens in the fixture.
const revocableKey = "poc.key4:secret_key4_0123456789abcdef"

// mintSessionJWTWithKey signs an Ably-JWT against an arbitrary fixture
// key (mintSessionJWT is pinned to poc.key1).
func mintSessionJWTWithKey(t *testing.T, keyName, keySecret, clientID string, expires time.Time) string {
	t.Helper()
	padded := make([]byte, 32)
	copy(padded, keySecret)
	signer, err := jwt.NewSignerHS(jwt.HS256, padded)
	require.NoError(t, err)
	claims := map[string]any{
		"exp":             expires.Unix(),
		"iat":             time.Now().Add(-time.Minute).Unix(),
		"x-ably-clientId": clientID,
	}
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	token, err := jwt.NewBuilder(signer, jwt.WithKeyID(keyName)).Build(json.RawMessage(payload))
	require.NoError(t, err)
	return token.String()
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func splitKey(key string) []string { return strings.SplitN(key, ":", 2) }

func revokeBody(t *testing.T, targets []string, extra map[string]any) []byte {
	t.Helper()
	body := map[string]any{"targets": targets}
	for k, v := range extra {
		body[k] = v
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return raw
}

func restRequestWithKey(t *testing.T, ts *realtimeTestServer, key, method, path string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.srv.URL+path, bytesReader(body))
	require.NoError(t, err)
	parts := splitKey(key)
	req.SetBasicAuth(parts[0], parts[1])
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// RSA17: a live token connection matching a revoked clientId is dropped
// with DISCONNECTED 40141, and the revoked token can no longer connect.
func TestTokenRevocationLiveAndConnectTime(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// Mint a token for the victim clientId via key4's requestToken-free
	// path: sign an Ably-JWT directly (iat in the past so issuedBefore
	// matching applies).
	token := mintSessionJWTWithKey(t, "poc.key4", "secret_key4_0123456789abcdef",
		"revoked-rita", time.Now().Add(time.Hour))

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", token)
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	// Revoke the clientId.
	resp := restRequestWithKey(t, ts, revocableKey, http.MethodPost,
		"/keys/poc.key4/revokeTokens",
		revokeBody(t, []string{"clientId:revoked-rita", "badtype:zzz"}, nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		SuccessCount int `json:"successCount"`
		FailureCount int `json:"failureCount"`
		Results      []struct {
			Target       string `json:"target"`
			IssuedBefore int64  `json:"issuedBefore"`
			AppliesAt    int64  `json:"appliesAt"`
			Error        *struct {
				StatusCode int `json:"statusCode"`
			} `json:"error"`
		} `json:"results"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, 1, result.SuccessCount)
	require.Equal(t, 1, result.FailureCount)
	require.Equal(t, "clientId:revoked-rita", result.Results[0].Target)
	require.NotZero(t, result.Results[0].IssuedBefore)
	require.NotZero(t, result.Results[0].AppliesAt)
	require.Nil(t, result.Results[0].Error)
	require.NotNil(t, result.Results[1].Error)
	require.Equal(t, 400, result.Results[1].Error.StatusCode)

	// The live session is disconnected with 40141.
	disconnected := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionDisconnected, disconnected.Action)
	require.Equal(t, 40141, disconnected.Error.Code)

	// The revoked token cannot reconnect: the connection is refused with
	// 40141 in-band.
	conn2 := dialRealtime(t, ts.wsURL, params)
	refused := readFrame(t, conn2)
	require.Equal(t, protocol.ActionError, refused.Action)
	require.Equal(t, 40141, refused.Error.Code)
}

// Keys without revocableTokens are refused; the reauth margin delays
// appliesAt by at least 30s.
func TestTokenRevocationGatesAndMargin(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// key0 has no revocableTokens flag.
	resp := restRequestWithKey(t, ts, testKey, http.MethodPost,
		"/keys/poc.key0/revokeTokens", revokeBody(t, []string{"clientId:x"}, nil))
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Margin: appliesAt strictly above now+30s.
	before := time.Now().UnixMilli()
	resp = restRequestWithKey(t, ts, revocableKey, http.MethodPost,
		"/keys/poc.key4/revokeTokens",
		revokeBody(t, []string{"clientId:y"}, map[string]any{"allowReauthMargin": true, "issuedBefore": before - 1200000}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Results []struct {
			IssuedBefore int64 `json:"issuedBefore"`
			AppliesAt    int64 `json:"appliesAt"`
		} `json:"results"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, before-1200000, result.Results[0].IssuedBefore, "issuedBefore echoed")
	require.Greater(t, result.Results[0].AppliesAt, before+30000)
}

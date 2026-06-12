package ably

// Credentialed-CORS contract, pinned against the REAL service (rest.ably.io,
// verified live both ways):
//
//	GET /time, no Origin            → Access-Control-Allow-Origin: *
//	GET /time, Origin: https://x    → Access-Control-Allow-Origin: https://x
//	                                  Access-Control-Allow-Credentials: true
//
// The echo+credentials pair is what browsers REQUIRE for ably-js's REST
// calls: the SDK's browser XHR sets withCredentials whenever it sends an
// Authorization header (xhrrequest.ts: `if ('authorization' in headers)
// xhr.withCredentials = true`), putting the request in credentials mode
// 'include' — where Chrome rejects a wildcard Allow-Origin outright
// ("must not be the wildcard '*' when the request's credentials mode is
// 'include'"). A constant `*` therefore silently broke every credentialed
// cross-origin REST call from browser SDKs (the live demo's history
// hydration hung on "Loading history..."), while comet requests — which
// auth via the querystring and carry no Authorization header — kept
// working and masked the bug. The Origin-less `*` half is pinned by
// TestSuccessResponseSurface.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCORSCredentialedRequestEchoesOrigin(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	const origin = "https://rt-poc-chat-demo.vercel.app"

	// The exact failing browser request shape: REST history with header auth
	// (→ withCredentials XHR) from a cross-origin page.
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/channels/ai%3Acors/messages?limit=900", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", origin)
	req.SetBasicAuth("poc.key0", "secret_key0_0123456789abcdef")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, origin, resp.Header.Get("Access-Control-Allow-Origin"),
		"an Origin-bearing request must get the origin ECHOED (wildcard is rejected in credentials mode)")
	require.Equal(t, "true", resp.Header.Get("Access-Control-Allow-Credentials"),
		"credentials mode 'include' requires Allow-Credentials: true (real Ably sends it)")
	require.Equal(t, "Origin", resp.Header.Get("Vary"), "per-origin responses must vary on Origin")

	// Error responses carry the same surface — the browser must be able to
	// READ a 401/404 cross-origin to surface it (a blocked error response
	// looks like a network failure to the SDK).
	req, err = http.NewRequest(http.MethodGet, srv.URL+"/channels/ai%3Acors/messages", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", origin) // no auth → 401
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)
	require.Equal(t, origin, resp2.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "true", resp2.Header.Get("Access-Control-Allow-Credentials"))

	// Preflight for the credentialed request: echoed origin + credentials +
	// the requested headers allowed (self-sufficient without the wrapping
	// centrifugo CORS middleware).
	req, err = http.NewRequest(http.MethodOptions, srv.URL+"/channels/ai%3Acors/messages?limit=900", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "authorization")
	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp3.Body.Close() }()
	require.Equal(t, http.StatusNoContent, resp3.StatusCode)
	require.Equal(t, origin, resp3.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "true", resp3.Header.Get("Access-Control-Allow-Credentials"))
	require.Contains(t, resp3.Header.Get("Access-Control-Allow-Headers"), "authorization")
}

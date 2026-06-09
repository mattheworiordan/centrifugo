package ably

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return NewHandler(node, configtypes.Ably{Enabled: true}, func(r *http.Request) bool { return true })
}

func requireTimeWithinSkew(t *testing.T, ms int64) {
	t.Helper()
	now := time.Now().UnixMilli()
	require.InDelta(t, now, ms, float64((time.Minute).Milliseconds()))
}

// RSC16: GET /time returns [ms] as JSON by default.
func TestTimeJSON_RSC16(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/time")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), contentTypeJSON)

	var times []int64
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&times))
	require.Len(t, times, 1)
	requireTimeWithinSkew(t, times[0])
}

// RSC16 + RSC8c: GET /time honors msgpack via format param and Accept header.
func TestTimeMsgPack_RSC16(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	for name, do := range map[string]func() (*http.Response, error){
		"format_param": func() (*http.Response, error) {
			return http.Get(srv.URL + "/time?format=msgpack")
		},
		"accept_header": func() (*http.Response, error) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+"/time", nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Accept", contentTypeMsgPack)
			return http.DefaultClient.Do(req)
		},
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := do()
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Contains(t, resp.Header.Get("Content-Type"), contentTypeMsgPack)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			// fixarray(1) + int64 — see serveTime.
			require.Len(t, body, 10)
			require.Equal(t, byte(0x91), body[0])
			require.Equal(t, byte(0xd3), body[1])
			requireTimeWithinSkew(t, int64(binary.BigEndian.Uint64(body[2:])))
		})
	}
}

// Catch-all REST error contract: 404 with code 40400 and Ably error headers.
func TestNotFoundError(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nonexistent")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "40400", resp.Header.Get("X-Ably-Errorcode"))

	var envelope struct {
		Error struct {
			Message    string `json:"message"`
			Code       int    `json:"code"`
			StatusCode int    `json:"statusCode"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Equal(t, 40400, envelope.Error.Code)
	require.Equal(t, http.StatusNotFound, envelope.Error.StatusCode)
}

// RTN1: realtime connections arrive as WebSocket upgrades at the web root.
// The M0 skeleton accepts the upgrade and closes.
func TestRealtimeUpgradeAccepted(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode, "upgrade at %s", url)
}

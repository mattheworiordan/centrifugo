package ably

// Comet transport front (C2 surface): GET /comet/connect establishes a
// real session and answers with a JSON array whose first element is
// CONNECTED; GET /comet/<key>/recv long-polls with single-flight
// semantics. Wire shapes follow the ably-js contract
// (research/09-comet-contract.md): array bodies with a trailing
// newline, 204 for empty batches, 410 for unknown keys.

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func cometGet(t *testing.T, ts *realtimeTestServer, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.srv.URL+path, nil)
	require.NoError(t, err)
	req.SetBasicAuth("poc.key0", "secret_key0_0123456789abcdef")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func decodeCometBatch(t *testing.T, resp *http.Response) []*protocol.ProtocolMessage {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(string(body), "]\n"),
		"comet bodies end ]\\n — node's streaming parser discards an unterminated final line; got %q", string(body))
	var frames []*protocol.ProtocolMessage
	require.NoError(t, json.Unmarshal(body, &frames))
	return frames
}

// cometConnect establishes a comet session and returns its connectionKey.
func cometConnect(t *testing.T, ts *realtimeTestServer) string {
	t.Helper()
	resp := cometGet(t, ts, "/comet/connect")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	frames := decodeCometBatch(t, resp)
	require.NotEmpty(t, frames)
	require.Equal(t, protocol.ActionConnected, frames[0].Action)
	require.NotNil(t, frames[0].ConnectionDetails)
	require.NotEmpty(t, frames[0].ConnectionDetails.ConnectionKey)
	require.EqualValues(t, maxIdleIntervalMS, frames[0].ConnectionDetails.MaxIdleInterval)
	return frames[0].ConnectionDetails.ConnectionKey
}

func TestCometConnectDeliversConnected(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)
	// The key is URL-path-safe by construction (uuid!uuid) — the SDK
	// concatenates it RAW into per-key paths.
	require.NotContains(t, key, "/")
	require.Contains(t, key, "!")
}

// The long poll: an idle recv parks and is completed by the session's
// heartbeat ticker within the advertised maxIdleInterval.
func TestCometRecvCompletedByHeartbeat(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)

	start := time.Now()
	resp := cometGet(t, ts, "/comet/"+key+"/recv")
	elapsed := time.Since(start)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	frames := decodeCometBatch(t, resp)
	require.NotEmpty(t, frames)
	require.Equal(t, protocol.ActionHeartbeat, frames[0].Action)
	require.Less(t, elapsed, time.Duration(maxIdleIntervalMS)*time.Millisecond,
		"the parked poll must flush inside the advertised maxIdleInterval")
}

// Single-flight: a second recv supersedes the parked first, which
// completes with an empty batch (204) without disturbing queued frames.
func TestCometRecvSingleFlight(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)

	var wg sync.WaitGroup
	wg.Add(1)
	firstStatus := make(chan int, 1)
	go func() {
		defer wg.Done()
		resp := cometGet(t, ts, "/comet/"+key+"/recv")
		firstStatus <- resp.StatusCode
	}()
	// Let the first recv park before superseding it.
	time.Sleep(300 * time.Millisecond)
	resp2 := cometGet(t, ts, "/comet/"+key+"/recv")

	select {
	case st := <-firstStatus:
		require.Equal(t, http.StatusNoContent, st, "superseded recv completes with an empty batch")
	case <-time.After(5 * time.Second):
		t.Fatal("superseded recv never completed")
	}
	// The successor poll is live: the heartbeat completes it.
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	frames := decodeCometBatch(t, resp2)
	require.NotEmpty(t, frames)
	wg.Wait()
}

func TestCometRecvUnknownKeyGone(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	resp := cometGet(t, ts, "/comet/nosuch!key/recv")
	require.Equal(t, http.StatusGone, resp.StatusCode)
	require.Equal(t, "80016", resp.Header.Get("X-Ably-Errorcode"))
}

func TestCometConnectRejectsMsgpack(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	resp := cometGet(t, ts, "/comet/connect?format=msgpack")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

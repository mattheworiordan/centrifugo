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

// cometSend POSTs a frame batch to /comet/<key>/send.
func cometSend(t *testing.T, ts *realtimeTestServer, key string, frames string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.srv.URL+"/comet/"+key+"/send", strings.NewReader(frames))
	require.NoError(t, err)
	req.SetBasicAuth("poc.key0", "secret_key0_0123456789abcdef")
	req.Header.Set("Content-Type", contentTypeJSON)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// recvUntil polls /recv until a frame matching the predicate arrives
// (collecting across heartbeat-completed batches) or the deadline hits.
func recvUntil(t *testing.T, ts *realtimeTestServer, key string, deadline time.Duration, match func(*protocol.ProtocolMessage) bool) *protocol.ProtocolMessage {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		resp := cometGet(t, ts, "/comet/"+key+"/recv")
		if resp.StatusCode == http.StatusNoContent {
			continue
		}
		require.Equal(t, http.StatusOK, resp.StatusCode)
		for _, m := range decodeCometBatch(t, resp) {
			if match(m) {
				return m
			}
		}
	}
	t.Fatal("expected frame never arrived over comet recv")
	return nil
}

// The full lifecycle over comet: attach, publish (ACK + echo), clean
// close delivering CLOSED — the C3 surface end to end.
func TestCometSendAttachPublishClose(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)

	// ATTACH via send; ATTACHED rides recv.
	resp := cometSend(t, ts, key, `[{"action":10,"channel":"comet-life"}]`)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	attached := recvUntil(t, ts, key, 5*time.Second, func(m *protocol.ProtocolMessage) bool {
		return m.Action == protocol.ActionAttached
	})
	require.Equal(t, "comet-life", attached.Channel)

	// Publish via send; the ACK and the echoed MESSAGE ride recv.
	resp = cometSend(t, ts, key, `[{"action":15,"channel":"comet-life","msgSerial":0,"messages":[{"name":"ev","data":"over-comet"}]}]`)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	sawAck, sawMsg := false, false
	recvUntil(t, ts, key, 5*time.Second, func(m *protocol.ProtocolMessage) bool {
		switch m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionMessage:
			require.Equal(t, "comet-life", m.Channel)
			require.Equal(t, "ev", m.Messages[0].Name)
			sawMsg = true
		}
		return sawAck && sawMsg
	})

	// Clean close: a recv parked BEFORE the close deterministically
	// receives the terminal CLOSED (flushLocked completes it when the
	// CLOSE-injected reply is buffered), and the key dies — 410.
	closedCh := make(chan bool, 1)
	go func() {
		resp := cometGet(t, ts, "/comet/"+key+"/recv")
		if resp.StatusCode != http.StatusOK {
			closedCh <- false
			return
		}
		for _, m := range decodeCometBatch(t, resp) {
			if m.Action == protocol.ActionClosed {
				closedCh <- true
				return
			}
		}
		closedCh <- false
	}()
	time.Sleep(300 * time.Millisecond) // let the recv park
	respClose := cometGet(t, ts, "/comet/"+key+"/close")
	require.Equal(t, http.StatusNoContent, respClose.StatusCode)
	select {
	case got := <-closedCh:
		require.True(t, got, "the parked recv must deliver the terminal CLOSED")
	case <-time.After(5 * time.Second):
		t.Fatal("parked recv never completed after close")
	}
	require.Eventually(t, func() bool {
		return cometGet(t, ts, "/comet/"+key+"/recv").StatusCode == http.StatusGone
	}, 5*time.Second, 100*time.Millisecond, "closed session's key must die")
}

// /disconnect ends the session abruptly: presence grace semantics, key
// gone, 204 even when repeated (closing a dead transport is success).
func TestCometDisconnect(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)

	resp := cometGet(t, ts, "/comet/"+key+"/disconnect")
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Eventually(t, func() bool {
		return cometGet(t, ts, "/comet/"+key+"/recv").StatusCode == http.StatusGone
	}, 5*time.Second, 100*time.Millisecond)
	// Repeat disconnect on the dead key: still 204.
	resp = cometGet(t, ts, "/comet/"+key+"/disconnect")
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestCometSendValidation(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts)
	resp := cometSend(t, ts, key, `{"not":"an array"}`)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

// cometGetAs issues a per-key GET with explicit Basic credentials.
func cometGetAs(t *testing.T, ts *realtimeTestServer, path, keyName, secret string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.srv.URL+path, nil)
	require.NoError(t, err)
	req.SetBasicAuth(keyName, secret)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// cometGetToken issues a per-key GET authenticated by an Ably-JWT (the
// Authorization: Bearer form, which authenticate() accepts on every
// surface — connect and the per-key routes alike).
func cometGetToken(t *testing.T, ts *realtimeTestServer, path, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.srv.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// cometConnectToken establishes a token-authenticated comet session and
// returns its connectionKey.
func cometConnectToken(t *testing.T, ts *realtimeTestServer, token string) string {
	t.Helper()
	resp := cometGetToken(t, ts, "/comet/connect", token)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	frames := decodeCometBatch(t, resp)
	require.NotEmpty(t, frames)
	require.Equal(t, protocol.ActionConnected, frames[0].Action)
	require.NotNil(t, frames[0].ConnectionDetails)
	require.NotEmpty(t, frames[0].ConnectionDetails.ConnectionKey)
	return frames[0].ConnectionDetails.ConnectionKey
}

// A1 (CRITICAL): a per-key comet request authenticated by a DIFFERENT app
// key may not read, inject into, or tear down a session it does not own.
// The foreign request is refused identically to an unknown key
// (410/80016) — no liveness oracle — and the victim's session survives.
func TestCometForeignKeyRefused(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	key := cometConnect(t, ts) // owner: poc.key0, no clientId

	const k1, s1 = "poc.key1", "secret_key1_0123456789abcdef" // valid app cred, NOT the owner

	// recv: refused, indistinguishable from an unknown key.
	resp := cometGetAs(t, ts, "/comet/"+key+"/recv", k1, s1)
	require.Equal(t, http.StatusGone, resp.StatusCode)
	require.Equal(t, "80016", resp.Header.Get("X-Ably-Errorcode"))

	// send: refused — no frame reaches the victim's run loop.
	req, err := http.NewRequest(http.MethodPost, ts.srv.URL+"/comet/"+key+"/send",
		strings.NewReader(`[{"action":10,"channel":"hijack"}]`))
	require.NoError(t, err)
	req.SetBasicAuth(k1, s1)
	req.Header.Set("Content-Type", contentTypeJSON)
	sendResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sendResp.Body.Close() })
	require.Equal(t, http.StatusGone, sendResp.StatusCode)

	// close: 204 (the dead-transport success contract) but a NO-OP — the
	// session must NOT be torn down by a non-owner.
	closeResp := cometGetAs(t, ts, "/comet/"+key+"/close", k1, s1)
	require.Equal(t, http.StatusNoContent, closeResp.StatusCode)

	// Proof the foreign close was inert: the owner still reaches its
	// session (a heartbeat completes the poll). A torn-down session would
	// answer 410 here.
	ownerRecv := cometGet(t, ts, "/comet/"+key+"/recv")
	require.Equal(t, http.StatusOK, ownerRecv.StatusCode,
		"the owning identity must still reach its session after a foreign close attempt")
}

// A1: the same signing key bound to a DIFFERENT clientId is refused — the
// bind is to the session's identity, not merely to the app.
func TestCometForeignClientIDRefused(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// Owner connects via an Ably-JWT (signing key poc.key1) bound to alice.
	aliceTok := mintSessionJWT(t, "alice", time.Now().Add(time.Hour))
	bobTok := mintSessionJWT(t, "bob", time.Now().Add(time.Hour))
	key := cometConnectToken(t, ts, aliceTok)

	// Same key, bound to bob — refused, indistinguishable from unknown.
	resp := cometGetToken(t, ts, "/comet/"+key+"/recv", bobTok)
	require.Equal(t, http.StatusGone, resp.StatusCode)
	require.Equal(t, "80016", resp.Header.Get("X-Ably-Errorcode"))

	// The owner (alice) reaches it — a heartbeat completes the poll.
	ownerRecv := cometGetToken(t, ts, "/comet/"+key+"/recv", aliceTok)
	require.Equal(t, http.StatusOK, ownerRecv.StatusCode)
	require.NotEmpty(t, decodeCometBatch(t, ownerRecv))
}

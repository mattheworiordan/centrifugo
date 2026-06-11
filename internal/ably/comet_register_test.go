package ably

// B2 (M2): the connectionKey is registered on the run goroutine (inside
// writeConnected) BEFORE the CONNECTED frame the client learns the key from
// is flushed. So a per-key request issued the instant /comet/connect
// returns always finds a running, registered session — there is no
// "registered before the run loop starts" window (the audited finding was
// stale: serveCometConnect only SETS the onConnected callback).

import (
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestCometKeyUsableImmediatelyAfterConnect_B2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	// cometConnect returns only after reading CONNECTED from the connect
	// response — i.e. the moment the client first has the key.
	key := cometConnect(t, ts)

	// Use the key immediately: a send is fed and processed (ATTACH →
	// ATTACHED rides recv). A not-yet-registered session would 410; a
	// not-yet-running session would never process the frame.
	resp := cometSend(t, ts, key, `[{"action":10,"channel":"b2-immediate"}]`)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	attached := recvUntil(t, ts, key, 5*time.Second, func(m *protocol.ProtocolMessage) bool {
		return m.Action == protocol.ActionAttached
	})
	require.Equal(t, "b2-immediate", attached.Channel)
}

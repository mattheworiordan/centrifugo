package ably

// B5 (M6): /close and /disconnect answer 2xx promptly without waiting for
// teardown to complete, so a client spamming them on a wedged session
// cannot tie up handler goroutines for up to writeTimeout each.

import (
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/stretchr/testify/require"
)

func TestCometCloseReturnsPromptly_B5(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)

	// A comet-backed session registered but NOT running: nothing drains the
	// injected close frame, so teardown (cc.closeCh) never completes. The
	// old handler blocked the full 5s waiting for it.
	cc := newCometConn()
	t.Cleanup(func() { _ = cc.close() })
	sess := newBareSession(ts, cc, capability)
	const key = "b5conn!b5token"
	ts.handler.registry.register(sess, sessionRecord{keyName: testKeyName})
	ts.handler.registry.registerKey(key, sess)

	start := time.Now()
	resp := cometGet(t, ts, "/comet/"+key+"/close")
	elapsed := time.Since(start)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Less(t, elapsed, 2*time.Second,
		"/close must answer promptly even when teardown never completes (M6)")
}

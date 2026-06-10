package ably

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// The grace expiry removes an abruptly-disconnected connection's members
// and synthesizes LEAVEs — except for identities that re-entered on
// another connection, which the grace window exists to protect.
func TestPresenceGraceExpiry(t *testing.T) {
	t.Parallel()
	store := newPresenceStore()
	store.set("ch", &protocol.PresenceMessage{ClientID: "alice", ConnectionID: "dead"})
	store.set("ch", &protocol.PresenceMessage{ClientID: "bob", ConnectionID: "dead"})
	// alice re-enters on a new connection before expiry.
	store.set("ch", &protocol.PresenceMessage{ClientID: "alice", ConnectionID: "fresh"})

	leaves := store.expireConnection("dead")
	require.Len(t, leaves["ch"], 1, "only bob leaves; alice re-entered")
	require.Equal(t, "bob", leaves["ch"][0].ClientID)

	members := store.members("ch")
	require.Len(t, members, 1)
	require.Equal(t, "fresh", members[0].ConnectionID)
}

// scheduleExpiry fires the fanout after the grace window with LEAVE
// action stamped.
func TestPresenceGraceTimer(t *testing.T) {
	t.Parallel()
	store := newPresenceStore()
	store.grace = 30 * time.Millisecond
	store.set("ch", &protocol.PresenceMessage{ClientID: "carol", ConnectionID: "gone"})

	var mu sync.Mutex
	var got []*protocol.PresenceMessage
	store.scheduleExpiry("gone", func(channel string, m *protocol.PresenceMessage) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	})

	// Member persists through the window...
	require.Len(t, store.members("ch"), 1, "member persists during grace")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1
	}, 2*time.Second, 5*time.Millisecond)
	mu.Lock()
	require.Equal(t, protocol.PresenceLeave, got[0].Action)
	require.Equal(t, "carol", got[0].ClientID)
	mu.Unlock()
	require.Empty(t, store.members("ch"))
}

// A server-side disconnect (node shutdown drives handleTransportClose
// first, winning beginClose) must still clean presence up: the member
// expires after the grace window. Regression guard for the
// teardown-early-return leak.
func TestPresenceServerSideDisconnectExpiry(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	ts.handler.presence.grace = 30 * time.Millisecond

	params := defaultDialParams()
	params.Set("clientId", "shutdown-sam")
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: "shutdown-pres", MsgSerial: 0,
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action)
	require.Len(t, ts.handler.presence.members("shutdown-pres"), 1)

	go func() { _ = ts.node.Shutdown(context.Background()) }()

	require.Eventually(t, func() bool {
		return len(ts.handler.presence.members("shutdown-pres")) == 0
	}, 3*time.Second, 10*time.Millisecond, "member must expire after grace despite server-side disconnect")
}

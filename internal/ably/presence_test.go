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

// Recovery inside the grace window disarms the pending expiry: the
// connection never died, so its members neither vanish nor LEAVE —
// including members RE-ENTERED by the recovered session under the same
// connectionId key (review-caught regression: the timer used to delete
// the live entry and synthesize a spurious LEAVE).
func TestPresenceCancelExpiryOnRecovery(t *testing.T) {
	t.Parallel()
	store := newPresenceStore()
	store.grace = 30 * time.Millisecond
	store.set("ch", &protocol.PresenceMessage{ClientID: "alice", ConnectionID: "c1"})

	var mu sync.Mutex
	var leaves []*protocol.PresenceMessage
	store.scheduleExpiry("c1", func(channel string, m *protocol.PresenceMessage) {
		mu.Lock()
		leaves = append(leaves, m)
		mu.Unlock()
	})
	store.cancelExpiry("c1")
	// The recovered session re-enters under the SAME connectionId key.
	store.set("ch", &protocol.PresenceMessage{ClientID: "alice", ConnectionID: "c1"})

	time.Sleep(120 * time.Millisecond) // well past the grace window
	require.Len(t, store.members("ch"), 1, "recovered member persists")
	mu.Lock()
	require.Empty(t, leaves, "no spurious LEAVE after cancellation")
	mu.Unlock()
}

// End-to-end: an abrupt drop arms the expiry; recovering the connection
// disarms it. The recovered session's re-entered member survives the
// grace window and a watcher sees no LEAVE — asserted with a marker
// message published after the window (per-channel delivery order).
func TestPresenceSurvivesRecovery(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	ts.handler.presence.grace = 50 * time.Millisecond

	// Watcher observes presence on the channel.
	watcher := connectRealtime(t, ts)
	writeFrame(t, watcher, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "recover-pres"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, watcher).Action)

	// Member enters, then drops abruptly (no CLOSE).
	params := defaultDialParams()
	params.Set("clientId", "recovering-rita")
	member := dialRealtime(t, ts.wsURL, params)
	connected := readFrame(t, member)
	require.Equal(t, protocol.ActionConnected, connected.Action)
	key := connected.ConnectionDetails.ConnectionKey
	writeFrame(t, member, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: "recover-pres", MsgSerial: 0,
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	enter := readNonHeartbeatFrame(t, watcher)
	require.Equal(t, protocol.ActionPresence, enter.Action)
	require.Equal(t, protocol.PresenceEnter, enter.Presence[0].Action)
	require.NoError(t, member.Close()) // abrupt: arms the grace expiry

	// Recover within the window and re-enter.
	params = defaultDialParams()
	params.Set("clientId", "recovering-rita")
	params.Set("recover", key)
	recovered := dialRealtime(t, ts.wsURL, params)
	reconnected := readFrame(t, recovered)
	require.Equal(t, protocol.ActionConnected, reconnected.Action)
	require.Equal(t, connected.ConnectionID, reconnected.ConnectionID)
	writeFrame(t, recovered, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: "recover-pres", MsgSerial: 0,
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	reenter := readNonHeartbeatFrame(t, watcher)
	require.Equal(t, protocol.ActionPresence, reenter.Action)
	require.Equal(t, protocol.PresenceEnter, reenter.Presence[0].Action)

	// Past the grace window: the member is still present and no LEAVE
	// reached the watcher — its next frame is the marker, not a LEAVE.
	time.Sleep(200 * time.Millisecond)
	require.Len(t, ts.handler.presence.members("recover-pres"), 1, "recovered member persists past grace")
	writeFrame(t, recovered, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "recover-pres", MsgSerial: 1,
		Messages: []*protocol.Message{{Name: "marker", Data: "x"}},
	})
	next := readNonHeartbeatFrame(t, watcher)
	require.Equal(t, protocol.ActionMessage, next.Action, "no LEAVE between re-enter and marker")
	require.Equal(t, "marker", next.Messages[0].Name)
}

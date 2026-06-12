package ably

// P6.0 — two-node multi-node harness (the substrate for Phase 6).
//
// Boots TWO Handler+Node instances against ONE Redis CONCURRENTLY (D7's
// node1/node2 pattern but both live at once), driving SDK-shaped traffic
// through both. This pins the cross-node behaviours that come FREE from the
// centrifuge Redis broker — message fan-out, shared history, and (by
// construction) D4 materialized state — so they are locked in before
// P6.1–P6.4 tackle the parts that do NOT work out of the box (cross-node
// presence, revocation/nonce/idempotency).
//
// What is deliberately NOT asserted here (it FAILS today and is the substrate
// for later items): cross-node live PRESENCE (the member set is in-process →
// P6.1) and cross-node REVOCATION/nonce (per-node → P6.2). Those assertions
// are written as the fail-before tests of P6.1/P6.2.
//
// Opt-in (ABLY_REDIS_TEST=1 + Redis on 127.0.0.1:6399), same as D7.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"
	"github.com/stretchr/testify/require"
)

// readUntilMessage reads frames (skipping heartbeats/ATTACHED) until a MESSAGE
// on the given channel arrives, or fails via the per-read 5s deadline.
func readUntilMessage(t *testing.T, conn *websocket.Conn, channel string) *protocol.Message {
	t.Helper()
	for i := 0; i < 10; i++ {
		f := readNonHeartbeatFrame(t, conn)
		if f.Action == protocol.ActionMessage && f.Channel == channel && len(f.Messages) > 0 {
			return f.Messages[0]
		}
	}
	t.Fatalf("no MESSAGE frame on %s after 10 frames", channel)
	return nil
}

// presenceMembersREST reads the current presence member set via REST.
func presenceMembersREST(t *testing.T, ts *realtimeTestServer, channel string) []protocol.PresenceMessage {
	t.Helper()
	resp := restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/presence", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var members []protocol.PresenceMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&members))
	return members
}

// P6.1 — cross-node presence. A member entered on node A must be visible in
// node B's presence (HAS_PRESENCE/SYNC/REST), via the shared Redis presence
// manager. This is the assertion P6.0 deliberately left failing.
func TestMultiNodeCrossNodePresence_P6_1(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p61-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	presChan := "persisted:" + uniq + "-pres"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// Enter presence on node A via a realtime connection.
	params := defaultDialParams()
	params.Set("clientId", "alice-p61")
	connA := dialRealtime(t, nodeA.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, connA).Action)
	writeFrame(t, connA, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   presChan,
		MsgSerial: 0,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "hi from A"}},
	})
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, connA).Action)

	// node B reflects the member entered on node A (cross-node, from Redis).
	require.Eventually(t, func() bool {
		members := presenceMembersREST(t, nodeB, presChan)
		return len(members) == 1 && members[0].ClientID == "alice-p61"
	}, 3*time.Second, 50*time.Millisecond, "node B must reflect the member entered on node A")

	// And node A sees its own member (single-node read still works under Redis).
	membersA := presenceMembersREST(t, nodeA, presChan)
	require.Len(t, membersA, 1)
	require.Equal(t, "alice-p61", membersA[0].ClientID)

	nodeA.stop()
	nodeB.stop()
}

func TestMultiNodeCrossNode_P6_0(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p60-%d", time.Now().UnixNano())
	prefix := uniq + ":"

	// Two nodes, ONE Redis, both live concurrently.
	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// --- Assertion 1: cross-node pub/sub delivery (Redis broker fan-out). ---
	// Subscriber on node B; publisher on node A; B must receive.
	xchan := "persisted:" + uniq + "-xnode"
	subB := connectRealtime(t, nodeB)
	writeFrame(t, subB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: xchan})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, subB).Action, "node B attached")

	resp := restRequest(t, nodeA, http.MethodPost, "/channels/"+xchan+"/messages",
		[]byte(`{"name":"x","data":"cross-node-hello"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "node A published")

	got := readUntilMessage(t, subB, xchan)
	require.Equal(t, "cross-node-hello", got.Data, "subscriber on node B received node A's publish via the Redis broker")

	// --- Assertion 2: cross-node history (shared Redis stream). ---
	histChan := "persisted:" + uniq + "-xhist"
	for i := 0; i < 3; i++ {
		d7Publish(t, nodeA, histChan, fmt.Sprintf(`{"name":"m","data":"h-%d"}`, i))
	}
	require.Equal(t, 3, d7HistoryCount(t, nodeB, histChan),
		"node B reads the history node A published (shared Redis stream)")

	// --- Assertion 3: cross-node materialized state (D4 is cross-node by
	// construction — the materialized store rebuilds from the SHARED Redis op
	// stream, so a mutable message created on A reconstructs on B). ---
	mutChan := "mutable:" + uniq + "-xmat"
	createSerials := d7Publish(t, nodeA, mutChan, `{"name":"orig","data":"Hello"}`)
	require.Len(t, createSerials, 1)
	createMsgSerial := createSerials[0]
	patchResp := restRequest(t, nodeA, http.MethodPatch, "/channels/"+mutChan+"/messages/"+url.PathEscape(createMsgSerial),
		[]byte(`{"action":5,"data":" World","version":{"clientId":"op"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, patchResp.StatusCode, "node A appended")

	getResp := restRequest(t, nodeB, http.MethodGet, "/channels/"+mutChan+"/messages/"+url.PathEscape(createMsgSerial), nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode, "node B reconstructs the mutable message")
	var matGot protocol.Message
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&matGot))
	require.Equal(t, "Hello World", matGot.Data, "materialized state created on A reconstructs on B from shared Redis history (D4 cross-node)")

	nodeA.stop()
	nodeB.stop()
}

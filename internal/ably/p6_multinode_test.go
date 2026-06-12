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
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
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

// P6.2(c) — cross-node idempotent publish. A republish carrying the same
// client-supplied message id (RSL1k2 → centrifuge WithIdempotencyKey) is
// deduplicated by the Redis broker's idempotency cache ACROSS nodes: publish
// id X on node A and again on node B → history holds exactly ONE copy. This
// is free from the Redis broker (D1); the test locks it in.
func TestMultiNodeCrossNodeIdempotency_P6_2(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p62idem-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	ch := "persisted:" + uniq + "-idem"
	id := uniq + ":0" // client-supplied id = idempotency key (RSL1k2)

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	body := `{"id":"` + id + `","name":"x","data":"once"}`
	// Same id published on BOTH nodes.
	d7Publish(t, nodeA, ch, body)
	d7Publish(t, nodeB, ch, body)

	// History (shared Redis stream) holds exactly one copy — the second
	// publish was deduped cross-node by the broker idempotency cache.
	require.Equal(t, 1, d7HistoryCount(t, nodeA, ch),
		"the same idempotency key published on two nodes yields ONE message (cross-node dedup)")
	require.Equal(t, 1, d7HistoryCount(t, nodeB, ch))

	// Control: a DISTINCT id is NOT deduped — proves the dedup is keyed, not
	// the channel trivially capping at one.
	d7Publish(t, nodeB, ch, `{"id":"`+uniq+`:1","name":"x","data":"twice"}`)
	require.Equal(t, 2, d7HistoryCount(t, nodeA, ch), "a distinct id adds a second message")

	nodeA.stop()
	nodeB.stop()
}

// P6.2(a) — cross-node revocation. A revoke accepted on node A must
// live-disconnect the matching session on node B AND make node B refuse the
// revoked token's new connections — via the broker revocation feed + each
// node's sweep.
func TestMultiNodeCrossNodeRevocation_P6_2(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p62rev-%d", time.Now().UnixNano())
	prefix := uniq + ":"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	clientID := "victim-" + uniq
	token := mintSessionJWTWithKey(t, "poc.key4", "secret_key4_0123456789abcdef", clientID, time.Now().Add(time.Hour))

	// A live token session on NODE B.
	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", token)
	connB := dialRealtime(t, nodeB.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, connB).Action)

	// Revoke the clientId on NODE A.
	resp := restRequestWithKey(t, nodeA, revocableKey, http.MethodPost,
		"/keys/poc.key4/revokeTokens", revokeBody(t, []string{"clientId:" + clientID}, nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// NODE B live-disconnects the matching session with 40141 — applied from
	// the feed by B's sweep (the per-read deadline covers the sync interval).
	var disconnected *protocol.ProtocolMessage
	for i := 0; i < 4 && disconnected == nil; i++ {
		f := readNonHeartbeatFrame(t, connB)
		if f.Action == protocol.ActionDisconnected {
			disconnected = f
		}
	}
	require.NotNil(t, disconnected, "node B disconnects the session revoked on node A")
	require.Equal(t, 40141, disconnected.Error.Code)

	// Connect-time: the revoked token cannot establish a NEW connection on
	// node B either (its store now carries the synced revoke).
	connB2 := dialRealtime(t, nodeB.wsURL, params)
	refused := readFrame(t, connB2)
	require.Equal(t, protocol.ActionError, refused.Action)
	require.Equal(t, 40141, refused.Error.Code)

	nodeA.stop()
	nodeB.stop()
}

// P6.3 — serial-order audit (verify, don't rewrite). Channel serials embed a
// per-process seriesId and each node mints independently, so cross-node a
// serial's LEXICOGRAPHIC order need not match broker OFFSET order. The audit
// proved every consumer is offset-driven (delivery = broker offset order;
// resume/untilAttach resolve a serial to its offset by EXACT-MATCH tag
// lookup, never a lexicographic compare). This test publishes alternately via
// both nodes and asserts (1) history/delivery order == offset order
// regardless of minting node, and (2) a serial minted on node A resolves
// correctly on node B (cross-node cursor).
func TestMultiNodeSerialOrder_P6_3(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p63ord-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	ch := "persisted:" + uniq + "-order"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// Publish 6 messages ALTERNATING nodes (even→A, odd→B).
	const n = 6
	for i := 0; i < n; i++ {
		node := nodeA
		if i%2 == 1 {
			node = nodeB
		}
		d7Publish(t, node, ch, fmt.Sprintf(`{"name":"m","data":"msg-%d"}`, i))
	}

	// (1) History on node B, forwards = broker OFFSET order = publish order,
	// regardless of which node minted each serial.
	resp := restRequest(t, nodeB, http.MethodGet, "/channels/"+ch+"/messages?limit=100&direction=forwards", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var msgs []protocol.Message
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&msgs))
	require.Len(t, msgs, n)
	for i := 0; i < n; i++ {
		require.Equal(t, fmt.Sprintf("msg-%d", i), msgs[i].Data,
			"history (offset) order matches publish order regardless of minting node")
	}

	// Sanity that this really is cross-node minting (not all on one node):
	// each node mints with its OWN per-process seriesId, so even-index
	// (node A) and odd-index (node B) serials must carry different seriesIds.
	seriesOf := func(messageSerial string) string {
		cs, _, e := serial.ParseMessageSerial(messageSerial)
		require.NoError(t, e)
		_, after, found := strings.Cut(cs, "@")
		require.True(t, found, "channelSerial carries a @seriesId")
		return after
	}
	seriesA, seriesB := seriesOf(msgs[0].Serial), seriesOf(msgs[1].Serial)
	require.NotEqual(t, seriesA, seriesB, "node A and node B mint with distinct seriesIds")
	require.Equal(t, seriesA, seriesOf(msgs[2].Serial), "node A's serials share a seriesId")
	require.Equal(t, seriesB, seriesOf(msgs[3].Serial), "node B's serials share a seriesId")

	// (2) Cross-node cursor: msg[2] was minted on node A (even index); resolve
	// its channelSerial on node B via untilAttach (from_serial). Exact-match
	// serial→offset lookup must bound the read at msg-2 inclusive — proving
	// resolution is offset-driven, not dependent on serial lexicographic order.
	cs2, _, err := serial.ParseMessageSerial(msgs[2].Serial)
	require.NoError(t, err)
	resp2 := restRequest(t, nodeB, http.MethodGet,
		"/channels/"+ch+"/messages?direction=forwards&from_serial="+url.QueryEscape(cs2), nil, nil)
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	var bounded []protocol.Message
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&bounded))
	got := make(map[string]bool)
	for _, m := range bounded {
		got[m.Data.(string)] = true
	}
	require.True(t, got["msg-0"] && got["msg-1"] && got["msg-2"],
		"untilAttach with a node-A serial, resolved on node B, includes msg-0..2")
	require.False(t, got["msg-3"] || got["msg-4"] || got["msg-5"],
		"untilAttach bounds at the resolved offset — later messages excluded")

	nodeA.stop()
	nodeB.stop()
}

// P6.4 — comet cross-node affinity. WebSocket needs nothing multi-node
// (long-lived to one node; message/presence/revocation fan-out via Redis).
// But comet per-key state (the cometConn + its connectionKey registry entry)
// is in-process PER-NODE — NOT shared via Redis — so a /comet/<key>/* request
// that lands on a node other than the one that established the session finds
// no such key and returns 410 GONE (graceful: the SDK treats it as nonfatal
// transport death and reconnects). The PoC therefore PINS comet to one node
// via an LB rule on /comet/* (documented in DIVERGENCES.md + the deploy
// README); this test proves the affinity and the graceful failure that makes
// the fallback safe. (Lifting the pin would need the fly-replay routing — a
// node identity in the connectionKey + a fly-replay response — which is fly
// infrastructure and not locally testable.)
func TestMultiNodeCometAffinity_P6_4(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p64comet-%d", time.Now().UnixNano())
	prefix := uniq + ":"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// A comet session established on node A (valid connectionKey, registered
	// in node A's in-process registry).
	key := cometConnect(t, nodeA)
	require.NotEmpty(t, key)

	// The key is genuinely live on node A but absent on node B — comet state
	// is per-node in-process, NOT shared via Redis (unlike messages/presence/
	// revocations). This is the affinity, not a malformed key.
	require.NotNil(t, nodeA.handler.registry.lookupKey(key), "node A holds the comet session")
	require.Nil(t, nodeB.handler.registry.lookupKey(key), "node B has no record of it (comet state is not Redis-shared)")

	// The SAME key, addressed to node B over HTTP, is 410 GONE — graceful
	// (the SDK reconnects), which is what makes the LB-pinning fallback safe.
	respB := cometGet(t, nodeB, "/comet/"+key+"/recv")
	require.Equal(t, http.StatusGone, respB.StatusCode,
		"comet is node-affine: a per-key request to the wrong node is 410 (LB must pin /comet/*)")

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

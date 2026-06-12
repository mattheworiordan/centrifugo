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

// waitClusterVisible blocks until ts's node sees n cluster members. Node
// info is broadcast on Run and every 3s thereafter; comet forwarding only
// targets nodes the forwarder can SEE, so tests must not race discovery.
func waitClusterVisible(t *testing.T, ts *realtimeTestServer, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		info, err := ts.handler.node.Info()
		return err == nil && len(info.Nodes) >= n
	}, 10*time.Second, 100*time.Millisecond, "cluster never reached %d visible nodes", n)
}

// P6.4 — comet cross-node forwarding (research/14-multinode-comet.md;
// supersedes the affinity-pinning assertion this test previously made,
// the way P6.1 superseded P6.0's deliberate-fail presence assertion).
// Comet per-key state stays in-process on the owning node — that is the
// design, asserted below — but a per-key request landing on ANOTHER node
// is now FORWARDED to the owner over the broker control channel
// (node.Survey, the centrifuge emulation-layer mechanism) instead of
// failing 410. The whole comet lifecycle — attach, publish, ACK + echo,
// close — is driven through the WRONG node here.
func TestMultiNodeCometCrossNode_P6_4(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p64comet-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	ch := uniq + "-comet"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)
	// B can only forward to a node it can see.
	waitClusterVisible(t, nodeB, 2)

	// A comet session established on node A (valid connectionKey, registered
	// in node A's in-process registry).
	key := cometConnect(t, nodeA)
	require.NotEmpty(t, key)

	// The per-node locality is unchanged by forwarding: the session lives on
	// node A only (comet state is still NOT Redis-shared — by design).
	require.NotNil(t, nodeA.handler.registry.lookupKey(key), "node A holds the comet session")
	require.Nil(t, nodeB.handler.registry.lookupKey(key), "node B has no record of it (comet state is not Redis-shared)")

	// ATTACH via send on the WRONG node: forwarded to node A, 204.
	resp := cometSend(t, nodeB, key, `[{"action":10,"channel":"`+ch+`"}]`)
	require.Equal(t, http.StatusNoContent, resp.StatusCode,
		"send on the wrong node must forward to the owner, not 410")

	// ATTACHED rides a recv on the WRONG node (forwarded park).
	attached := recvUntil(t, nodeB, key, 10*time.Second, func(m *protocol.ProtocolMessage) bool {
		return m.Action == protocol.ActionAttached
	})
	require.Equal(t, ch, attached.Channel)

	// Publish via send on B; the ACK and the echoed MESSAGE ride recv on B.
	resp = cometSend(t, nodeB, key,
		`[{"action":15,"channel":"`+ch+`","msgSerial":0,"messages":[{"name":"ev","data":"cross-node"}]}]`)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	sawAck, sawMsg := false, false
	recvUntil(t, nodeB, key, 10*time.Second, func(m *protocol.ProtocolMessage) bool {
		switch m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionMessage:
			require.Equal(t, ch, m.Channel)
			require.Equal(t, "ev", m.Messages[0].Name)
			sawMsg = true
		}
		return sawAck && sawMsg
	})

	// Clean close via the WRONG node: 204, and the session dies on node A —
	// the key goes 410 everywhere (on A locally; on B the forward finds the
	// owner has no such session).
	respClose := cometGet(t, nodeB, "/comet/"+key+"/close")
	require.Equal(t, http.StatusNoContent, respClose.StatusCode)
	require.Eventually(t, func() bool {
		return nodeA.handler.registry.lookupKey(key) == nil
	}, 5*time.Second, 100*time.Millisecond, "close forwarded from B must tear the session down on A")
	require.Equal(t, http.StatusGone, cometGet(t, nodeA, "/comet/"+key+"/recv").StatusCode)
	require.Equal(t, http.StatusGone, cometGet(t, nodeB, "/comet/"+key+"/recv").StatusCode)

	nodeA.stop()
	nodeB.stop()
}

// P6.4 — a dead owner keeps the 410 contract. When the node named in the
// connectionKey is no longer a live cluster member (stop/restart — a
// restarted node has a NEW id), per-key requests are an immediate-or-bounded
// 410 and the SDK reconnects fresh; close stays 204 (closing a dead
// transport is success). This is the self-healing edge of the forwarding
// design — no pinning, no replay, no stuck clients.
func TestMultiNodeCometDeadOwner_P6_4(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p64dead-%d", time.Now().UnixNano())
	prefix := uniq + ":"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)
	waitClusterVisible(t, nodeB, 2)

	key := cometConnect(t, nodeA)

	// Shorten B's forward timeouts: until A's node info ages out of B's
	// registry (~7s), a forward to the dead node would wait the full survey
	// window; the test bounds that to keep the suite fast.
	nodeB.handler.forwardRecvTimeout = 1500 * time.Millisecond
	nodeB.handler.forwardOpTimeout = 1500 * time.Millisecond

	nodeA.stop()

	require.Equal(t, http.StatusGone, cometGet(t, nodeB, "/comet/"+key+"/recv").StatusCode,
		"recv for a dead owner is 410 (SDK reconnects fresh)")
	respSend := cometSend(t, nodeB, key, `[{"action":10,"channel":"x"}]`)
	require.Equal(t, http.StatusGone, respSend.StatusCode, "send for a dead owner is 410")
	require.Equal(t, http.StatusNoContent, cometGet(t, nodeB, "/comet/"+key+"/disconnect").StatusCode,
		"closing a dead transport is success (204), exactly as single-node")

	nodeB.stop()
}

// P6.4 — A1 holds cross-node. A caller authenticated with a DIFFERENT app
// key driving a forwarded per-key request is refused exactly as a dead key
// (410, no liveness oracle): the forwarding node passes the RESOLVED
// identity and the OWNING node runs the same ownsSession bind as the local
// paths.
func TestMultiNodeCometForeignIdentity_P6_4(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p64a1-%d", time.Now().UnixNano())
	prefix := uniq + ":"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)
	waitClusterVisible(t, nodeB, 2)

	key := cometConnect(t, nodeA) // owned by poc.key0

	// poc.key1 (a different key, same app config) addressing the key via the
	// WRONG node: forwarded, and refused by the owner's identity bind.
	resp := cometGetAs(t, nodeB, "/comet/"+key+"/recv", "poc.key1", "secret_key1_0123456789abcdef")
	require.Equal(t, http.StatusGone, resp.StatusCode,
		"a non-owner identity must be indistinguishable from a dead key across nodes")

	// The owner still works end-to-end through the wrong node.
	respOwner := cometGet(t, nodeB, "/comet/"+key+"/disconnect")
	require.Equal(t, http.StatusNoContent, respOwner.StatusCode)

	nodeA.stop()
	nodeB.stop()
}

// P6.5 assertion (3) — cross-node resume (the headline). A connection on
// node A notes a channelSerial; the connection drops; a NEW connection on
// node B re-attaches with that serial and resumes cleanly — RESUMED, with the
// gap published meanwhile replayed in order, no loss. The cursor minted on
// node A resolves on node B because resolveCursor reads the SHARED Redis
// history by exact-match tag lookup (P6.3).
func TestMultiNodeResumeCrossNode_P6_5(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p65resume-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	ch := "persisted:" + uniq + "-resume"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// On node A: attach, publish msg-0, take its channelSerial as the resume
	// cursor, then DROP the connection.
	connA := connectRealtime(t, nodeA)
	writeFrame(t, connA, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: ch})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connA).Action)
	cursor := publishAndTakeSerial(t, connA, ch, "msg-0", 0)
	_ = connA.Close() // the connection drops (node A loses it)

	// The gap is published via node B while the client is away.
	connB1 := connectRealtime(t, nodeB)
	writeFrame(t, connB1, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: ch})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connB1).Action)
	publishAndTakeSerial(t, connB1, ch, "gap-1", 0)
	publishAndTakeSerial(t, connB1, ch, "gap-2", 1)

	// Resume on NODE B with node A's cursor → RESUMED + the two gap messages
	// in order. Cross-node continuity, no loss.
	connB2 := connectRealtime(t, nodeB)
	writeFrame(t, connB2, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       ch,
		ChannelSerial: cursor,
	})
	attached := readNonHeartbeatFrame(t, connB2)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagResumed, attached.Flags&protocol.FlagResumed,
		"cross-node continuity: a cursor minted on node A resumes RESUMED on node B")
	first := readNonHeartbeatFrame(t, connB2)
	require.Equal(t, protocol.ActionMessage, first.Action)
	require.Equal(t, "gap-1", first.Messages[0].Name, "gap replayed on the other node, no loss")
	second := readNonHeartbeatFrame(t, connB2)
	require.Equal(t, protocol.ActionMessage, second.Action)
	require.Equal(t, "gap-2", second.Messages[0].Name)

	nodeA.stop()
	nodeB.stop()
}

// P6.5 assertion (5) — AIT cross-node: create+append a mutable message via
// node A, then a realtime rewind on node B reconstructs the materialized
// state byte-exact (D4 is cross-node by construction — node B rebuilds from
// the shared Redis op stream).
func TestMultiNodeAITRewindCrossNode_P6_5(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("p65ait-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	mutChan := "mutable:" + uniq + "-ait"

	nodeA := buildRedisServer(t, prefix)
	nodeB := buildRedisServer(t, prefix)

	// Create + append on node A.
	createSerials := d7Publish(t, nodeA, mutChan, `{"name":"orig","data":"Hello"}`)
	require.Len(t, createSerials, 1)
	patchResp := restRequest(t, nodeA, http.MethodPatch, "/channels/"+mutChan+"/messages/"+url.PathEscape(createSerials[0]),
		[]byte(`{"action":5,"data":" World","version":{"clientId":"op"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, patchResp.StatusCode)

	// Realtime rewind on node B reconstructs the materialized message.
	conn := connectRealtime(t, nodeB)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: mutChan,
		Params:  map[string]string{"rewind": "1"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagHasBacklog, attached.Flags&protocol.FlagHasBacklog)
	backlog := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, backlog.Action)
	require.Len(t, backlog.Messages, 1)
	require.Equal(t, "Hello World", backlog.Messages[0].Data,
		"node B's realtime rewind reconstructs the materialized state created on node A (D4 cross-node)")

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

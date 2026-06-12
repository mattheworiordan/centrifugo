package ably

// D7 — restart-durability regression suite (the proof, grown across D2–D4).
//
// The faithful test: build centrifuge.Node #1 + Handler against a REAL Redis,
// exercise it, FULLY tear node1 down, then build a FRESH Node #2 + Handler
// against the SAME Redis. Node2's process memory is empty — anything read
// back came from Redis. Assertions:
//  1. History persists (D2)            — publish N → restart → history = N.
//  2. Serial monotonicity (D3)         — a post-restart publish gets a serial
//                                         strictly after the pre-restart
//                                         high-water (seed-from-history).
//  3. Materialized state (D4)          — create+append a mutable message →
//                                         restart → REST GET and realtime
//                                         rewind reconstruct it byte-exact.
//  4. Resume/epoch continuity          — the broker epoch is unchanged across
//                                         a node restart (it changes only on
//                                         Redis DATA loss, per D1), so a
//                                         history/rewind read spanning the
//                                         restart is continuous (covered by 1
//                                         and the rewind in 3).
//
// The memory-engine control (TestRestartDurabilityMemoryControl_D7) runs the
// SAME read-back assertions and proves they FAIL without a durable broker —
// so the Redis pass is genuinely Redis, not in-process state leaking across
// the "restart". That control is the anti-theater guard and runs always.
//
// The Redis test is opt-in: set ABLY_REDIS_TEST=1 with Redis reachable on
// 127.0.0.1:6399 (docker compose -f deploy/ably-poc/docker-compose.yml up -d
// redis). Without it the default `go test` stays hermetic and skips.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const d7RedisAddr = "127.0.0.1:6399"

// requireRedisOrSkip skips unless the test is opted in AND Redis answers.
func requireRedisOrSkip(t *testing.T) {
	t.Helper()
	if os.Getenv("ABLY_REDIS_TEST") != "1" {
		t.Skip("set ABLY_REDIS_TEST=1 (with Redis on " + d7RedisAddr + ") to run the Redis integration suites (D5/D7 durability, P6 multi-node)")
	}
	conn, err := net.DialTimeout("tcp", d7RedisAddr, time.Second)
	if err != nil {
		t.Skipf("Redis not reachable at %s: %v", d7RedisAddr, err)
	}
	_ = conn.Close()
}

// buildRedisServer builds a Handler over a Redis-backed centrifuge.Node using
// the given key prefix. Two servers sharing a prefix share Redis state — the
// substrate of a faithful restart. Lifecycle is caller-controlled (stop());
// a cleanup is registered as a safety net for the failure path.
func buildRedisServer(t *testing.T, prefix string) *realtimeTestServer {
	t.Helper()
	return buildEngineServer(t, prefix)
}

// buildMemoryServer builds a Handler over a fresh memory-engine node — two
// of these share NOTHING, which is exactly what makes them the control.
func buildMemoryServer(t *testing.T) *realtimeTestServer {
	t.Helper()
	return buildEngineServer(t, "")
}

// buildEngineServer constructs the node (Redis when prefix != "", else
// memory), wires the same OnConnect/OnSubscribe as the standard test server,
// builds the Handler, and serves it. The returned server's stop() must be
// called to tear it down deterministically before building its successor.
func buildEngineServer(t *testing.T, redisPrefix string) *realtimeTestServer {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	if redisPrefix != "" {
		shard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{Address: d7RedisAddr})
		require.NoError(t, err)
		shards := []*centrifuge.RedisShard{shard}
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{Shards: shards, Prefix: redisPrefix})
		require.NoError(t, err)
		node.SetBroker(broker)
		pm, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{Shards: shards, Prefix: redisPrefix})
		require.NoError(t, err)
		node.SetPresenceManager(pm)
	}
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{
				AllowTagsFilter: true,
				EnableRecovery:  e.Recoverable,
			}}, nil)
		})
	})
	require.NoError(t, node.Run())

	// Cross-node presence (P6.1): in Redis mode give the handler a Redis
	// presence manager sharing the same Redis + a prefix derived from the
	// run prefix, so two nodes built with the same redisPrefix see each
	// other's members — mirroring production wiring (mux.go).
	var presenceMgr centrifuge.PresenceManager
	if redisPrefix != "" {
		shard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{Address: d7RedisAddr})
		require.NoError(t, err)
		presenceMgr, err = centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
			Shards: []*centrifuge.RedisShard{shard},
			Prefix: redisPrefix + "ablypres",
		})
		require.NoError(t, err)
	}

	h, err := NewHandler(node, configtypes.Ably{Enabled: true, KeysFile: "auth/testdata/static-app.json"}, presenceMgr, func(r *http.Request) bool { return true })
	require.NoError(t, err)
	srv := httptest.NewServer(h)

	ts := &realtimeTestServer{
		wsURL:   "ws" + strings.TrimPrefix(srv.URL, "http"),
		node:    node,
		srv:     srv,
		handler: h,
	}
	// Safety net for the failure path; the happy path calls stop() explicitly
	// before building the successor node. node.Shutdown after a clean stop is
	// a harmless no-op; httptest Close is guarded by stop()'s nil-ing of srv.
	t.Cleanup(func() { ts.stop() })
	return ts
}

// stop tears the server down deterministically and is idempotent.
func (ts *realtimeTestServer) stop() {
	if ts.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = ts.node.Shutdown(ctx)
	cancel()
	ts.srv.Close()
	ts.srv = nil
}

// d7Publish publishes one message to a channel via REST and returns the
// decoded response (serials present only for mutable channels).
func d7Publish(t *testing.T, ts *realtimeTestServer, channel, body string) []string {
	t.Helper()
	resp := restRequest(t, ts, http.MethodPost, "/channels/"+channel+"/messages",
		[]byte(body), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var pub struct {
		Serials []string `json:"serials"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pub))
	return pub.Serials
}

// d7HistoryCount returns how many messages REST history reports for a channel.
func d7HistoryCount(t *testing.T, ts *realtimeTestServer, channel string) int {
	t.Helper()
	resp := restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/messages?limit=100", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var msgs []protocol.Message
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&msgs))
	return len(msgs)
}

// serialStrictlyAfter reports whether channelSerial b is strictly after a by
// (timestamp, counter) — the comparison that survives a per-process series-id
// change (the series suffix differs between node1 and node2; the position
// (ts,counter) is what D3 keeps monotonic).
func serialStrictlyAfter(t *testing.T, b, a string) bool {
	t.Helper()
	bts, bctr, err := serial.ParseChannelSerial(b)
	require.NoError(t, err, "parse newer serial %q", b)
	ats, actr, err := serial.ParseChannelSerial(a)
	require.NoError(t, err, "parse older serial %q", a)
	return bts > ats || (bts == ats && bctr > actr)
}

// channelSerialOf strips the :idx suffix from a message serial, yielding the
// channelSerial.
func channelSerialOf(t *testing.T, messageSerial string) string {
	t.Helper()
	cs, _, err := serial.ParseMessageSerial(messageSerial)
	require.NoError(t, err, "parse message serial %q", messageSerial)
	return cs
}

func TestRestartDurability_D7(t *testing.T) {
	requireRedisOrSkip(t)
	// Unique per run so the suite is isolated from the gate and prior runs;
	// keys TTL out (24h history TTL). time.Now is fine in a Go test.
	uniq := fmt.Sprintf("d7-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	histChan := "persisted:" + uniq + "-hist"
	mutChan := "mutable:" + uniq + "-mat"

	// ---- NODE 1: exercise, then tear down fully. ----
	n1 := buildRedisServer(t, prefix)

	const wantHistory = 5
	for i := 0; i < wantHistory; i++ {
		d7Publish(t, n1, histChan, fmt.Sprintf(`{"name":"m","data":"msg-%d"}`, i))
	}
	require.Equal(t, wantHistory, d7HistoryCount(t, n1, histChan), "node1 sees its own publishes")

	// Mutable: create + append. Capture the create's channelSerial and the
	// post-create high-water (the append's versionSerial).
	createSerials := d7Publish(t, n1, mutChan, `{"name":"orig","data":"Hello"}`)
	require.Len(t, createSerials, 1, "mutable create returns a serial")
	createMsgSerial := createSerials[0]
	createCS := channelSerialOf(t, createMsgSerial)

	patchResp := restRequest(t, n1, http.MethodPatch, "/channels/"+mutChan+"/messages/"+url.PathEscape(createMsgSerial),
		[]byte(`{"action":5,"data":" World","version":{"clientId":"op"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, patchResp.StatusCode)
	var patched struct {
		VersionSerial string `json:"versionSerial"`
	}
	require.NoError(t, json.NewDecoder(patchResp.Body).Decode(&patched))
	require.NotEmpty(t, patched.VersionSerial)
	highWater := patched.VersionSerial // the latest serial minted on mutChan by node1

	n1.stop() // FULLY tear node1 down — node2's memory will be empty.

	// ---- NODE 2: fresh process memory, SAME Redis. ----
	n2 := buildRedisServer(t, prefix)

	// Assertion 1 — history persists (D2).
	require.Equal(t, wantHistory, d7HistoryCount(t, n2, histChan),
		"history survived the restart: node2 read node1's publishes from Redis")

	// Assertion 3 (REST) — materialized state rebuilt from Redis op stream (D4).
	getResp := restRequest(t, n2, http.MethodGet, "/channels/"+mutChan+"/messages/"+url.PathEscape(createMsgSerial), nil, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode, "materialized message reconstructed after restart")
	var got protocol.Message
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&got))
	require.Equal(t, "Hello World", got.Data, "materialized state survived the restart byte-exact")
	require.Equal(t, protocol.MessageActionUpdate, got.Action)

	// Assertion 3 (realtime) — rewind reconstructs the materialized message
	// from Redis after the restart (the live AIT-style read path).
	conn := connectRealtime(t, n2)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: mutChan,
		Params:  map[string]string{"rewind": "1"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagHasBacklog, attached.Flags&protocol.FlagHasBacklog, "rewind has backlog after restart")
	backlog := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, backlog.Action)
	require.Len(t, backlog.Messages, 1)
	require.Equal(t, "Hello World", backlog.Messages[0].Data, "realtime rewind reconstructed materialized state from Redis")

	// Assertion 2 — serial monotonicity (D3): a post-restart create gets a
	// serial strictly after node1's high-water. Proves node2 seeded its
	// generator from broker history rather than resetting.
	postSerials := d7Publish(t, n2, mutChan, `{"name":"after","data":"Second"}`)
	require.Len(t, postSerials, 1)
	postCS := channelSerialOf(t, postSerials[0])
	require.True(t, serialStrictlyAfter(t, postCS, highWater),
		"post-restart serial %s must be strictly after the pre-restart high-water %s", postCS, highWater)
	require.True(t, serialStrictlyAfter(t, postCS, createCS),
		"post-restart serial %s must be strictly after the pre-restart create %s", postCS, createCS)

	n2.stop()
}

// TestRestartDurabilityMemoryControl_D7 is the anti-theater control: the same
// read-back assertions on the memory engine must FAIL to find the data,
// proving the Redis test passes because of Redis, not in-process leakage.
// Runs always (no Redis needed).
func TestRestartDurabilityMemoryControl_D7(t *testing.T) {
	t.Parallel()
	uniq := fmt.Sprintf("ctrl-%d", time.Now().UnixNano())
	histChan := "persisted:" + uniq + "-hist"
	mutChan := "mutable:" + uniq + "-mat"

	// NODE 1 (memory): publish history + a mutable create.
	n1 := buildMemoryServer(t)
	for i := 0; i < 5; i++ {
		d7Publish(t, n1, histChan, fmt.Sprintf(`{"name":"m","data":"msg-%d"}`, i))
	}
	createSerials := d7Publish(t, n1, mutChan, `{"name":"orig","data":"Hello"}`)
	require.Len(t, createSerials, 1)
	createMsgSerial := createSerials[0]
	require.Equal(t, 5, d7HistoryCount(t, n1, histChan), "node1 sees its own publishes")

	n1.stop() // tear down — a memory node shares nothing with its successor.

	// NODE 2 (memory): a fresh, independent node. The data is GONE.
	n2 := buildMemoryServer(t)
	require.Equal(t, 0, d7HistoryCount(t, n2, histChan),
		"CONTROL: history did NOT survive on the memory engine — the read-back probe correctly reports loss")
	getResp := restRequest(t, n2, http.MethodGet, "/channels/"+mutChan+"/messages/"+url.PathEscape(createMsgSerial), nil, nil)
	require.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"CONTROL: materialized state did NOT survive on the memory engine — proves the Redis pass is genuinely durable storage")

	n2.stop()
}

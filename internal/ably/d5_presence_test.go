package ably

// D5 — presence durability (assessed; live-set Redis-backing DEFERRED, by
// design). Decision and rationale:
//
// The adapter splits presence into two layers:
//   - Presence HISTORY (the enter/leave event log) is published to a
//     client-unreachable shadow channel through the broker, so on the Redis
//     engine it is DURABLE — it survives a node restart exactly like message
//     history (it inherits the live channel's retention tier).
//   - The live presence SET (current members, presenceStore) is in-process
//     and connection-scoped: a member exists only while its connection lives.
//
// Decision: do NOT Redis-back the live set, and do NOT rebuild it from the
// shadow history on restart. Rationale — a node restart drops EVERY client
// connection, so every member has legitimately left; the Ably SDK re-enters
// presence on reconnect (RTP5/RTP17), rebuilding the live set within seconds.
// Replaying the shadow history to repopulate the live set would manufacture
// GHOST members (shown present though their connection is gone) until they
// timed out — strictly worse than the connection-authoritative re-sync. So
// the in-process live set is the correct model and its loss on restart is a
// bounded, self-healing divergence, not a durability gap.
//
// This test pins both halves on a real cross-process Redis restart: the
// history survives, and the live set is (intentionally) empty until re-entry.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// d5PresenceHistoryCount returns how many presence events REST presence
// history reports for a channel.
func d5PresenceHistoryCount(t *testing.T, ts *realtimeTestServer, channel string) int {
	t.Helper()
	resp := restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/presence/history?limit=100", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var events []protocol.PresenceMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	return len(events)
}

func TestPresenceDurability_D5(t *testing.T) {
	requireRedisOrSkip(t)
	uniq := fmt.Sprintf("d5-%d", time.Now().UnixNano())
	prefix := uniq + ":"
	// persisted: so the shadow presence history gets the long-retention tier.
	presChan := "persisted:" + uniq + "-pres"

	// ---- NODE 1: a client enters presence. ----
	n1 := buildRedisServer(t, prefix)
	params := defaultDialParams()
	params.Set("clientId", "alice-d5")
	conn := dialRealtime(t, n1.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   presChan,
		MsgSerial: 0,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "hi"}},
	})
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action)
	require.Len(t, n1.handler.presence.members(presChan), 1, "node1 holds the live member")
	require.GreaterOrEqual(t, d5PresenceHistoryCount(t, n1, presChan), 1, "node1 recorded the enter in presence history")

	n1.stop() // FULLY tear node1 down — the WS drops with it.

	// ---- NODE 2: fresh process memory, SAME Redis. ----
	n2 := buildRedisServer(t, prefix)

	// DURABLE: the presence enter event survived in Redis (shadow channel).
	require.GreaterOrEqual(t, d5PresenceHistoryCount(t, n2, presChan), 1,
		"presence HISTORY survived the restart via the Redis shadow channel")

	// DIVERGENCE (intentional, accepted): the live presence set is NOT rebuilt
	// from history — members re-sync on reconnect (the SDK re-enters). A node
	// restart dropped every connection, so the empty live set is correct, not
	// a loss; rebuilding it would manufacture ghost members.
	require.Empty(t, n2.handler.presence.members(presChan),
		"live presence set is intentionally empty post-restart — re-syncs on reconnect (D5 decision)")

	n2.stop()
}

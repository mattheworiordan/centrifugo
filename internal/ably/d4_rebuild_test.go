package ably

// D4 — materialized-state durability. After the in-memory materialized store
// is lost (a process restart), a read rebuilds the materialized state from
// the broker op stream in history: create + ops replayed in offset order, so
// GET, /versions and realtime rewind all return the reconstructed state
// byte-exact. The broker history survives a restart on the Redis engine
// (D1/D7); this same-process test wipes only the materialized store, leaving
// the (memory) broker history intact — the exact post-restart shape against a
// shared broker.
//
// Bound: this proves the common case (<=1000 ops, create still in the
// retained history window). A stream exceeding persistedHistorySize loses its
// create + early ops to MAXLEN trimming; lifting that needs snapshot-
// compaction, scoped as a D4 follow-on (see materialized_load.go).

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// wipeForTest clears all in-memory materialized state, simulating a process
// restart. The broker history is untouched — as it would be against the same
// Redis after a real restart.
func (s *materializedStore) wipeForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels = make(map[string]map[string]*materializedEntry)
	s.order = make(map[string][]string)
	s.loaded = make(map[string]bool)
}

func TestMaterializedRebuildFromHistory_D4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	const channel = "mutable:d4-rebuild"

	// Create + append via REST → history + store both populated.
	resp := restRequest(t, ts, http.MethodPost, "/channels/"+channel+"/messages",
		[]byte(`{"name":"orig","data":"Hello"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var pub struct {
		Serials []string `json:"serials"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pub))
	require.Len(t, pub.Serials, 1)
	msgSerial := pub.Serials[0]
	escaped := url.PathEscape(msgSerial)

	resp = restRequest(t, ts, http.MethodPatch, "/channels/"+channel+"/messages/"+escaped,
		[]byte(`{"action":5,"data":" World","version":{"clientId":"op","description":"d"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var patched struct {
		VersionSerial string `json:"versionSerial"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patched))
	require.NotEmpty(t, patched.VersionSerial)

	// Simulate a restart: drop ALL in-memory materialized state.
	ts.handler.materialized.wipeForTest()

	// Fail-before guard: the store is genuinely empty — a direct read misses.
	// Without the rebuild-on-miss hook this is exactly what the REST handler
	// would observe, returning 404.
	_, ok := ts.handler.materialized.get(channel, msgSerial)
	require.False(t, ok, "store is empty after the simulated restart")

	// GET rebuilds from history → reconstructed concatenated state, byte-exact.
	resp = restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/messages/"+escaped, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got protocol.Message
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "Hello World", got.Data, "materialized state reconstructed from the op stream")
	require.Equal(t, protocol.MessageActionUpdate, got.Action, "append materializes as update")
	require.NotNil(t, got.Version)
	require.Equal(t, patched.VersionSerial, got.Version.Serial, "latest version recovered from history")
	require.Equal(t, "op", got.Version.ClientID)
	require.Equal(t, "orig", got.Name, "create name recovered")

	// /versions rebuilds too: create + the append op, oldest first.
	ts.handler.materialized.wipeForTest()
	resp = restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/messages/"+escaped+"/versions", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var versions []protocol.Message
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&versions))
	require.Len(t, versions, 2, "create + append reconstructed")
	require.Equal(t, protocol.MessageActionCreate, versions[0].Action)
	require.Equal(t, protocol.MessageActionUpdate, versions[1].Action)
	require.Equal(t, patched.VersionSerial, versions[1].Version.Serial)

	// Realtime rewind rebuilds too: a fresh attach with rewind=1 after the
	// restart delivers the reconstructed materialized message.
	ts.handler.materialized.wipeForTest()
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Params:  map[string]string{"rewind": "1"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagHasBacklog, attached.Flags&protocol.FlagHasBacklog, "rewind has backlog after rebuild")
	backlog := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, backlog.Action)
	require.Len(t, backlog.Messages, 1)
	require.Equal(t, "Hello World", backlog.Messages[0].Data, "rewind reconstructs materialized state from history")
	require.Equal(t, protocol.MessageActionUpdate, backlog.Messages[0].Action)
}

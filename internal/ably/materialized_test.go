package ably

// Materialized mutable-message state (M8): store semantics (TM5 actions,
// version snapshots) and the realtime mutation wire (RTL32) — op
// fan-out with version, ACK res carrying versionSerial, materialized
// rewind, and the 93002/40400 rejections.

import (
	"encoding/json"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestMaterializedStoreSemantics(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	store.create("ch", &protocol.Message{Serial: "s1", Name: "n", Data: "Hello", Timestamp: 100})

	// Append concatenates string data and replaces extras wholesale; the
	// materialized action becomes update.
	op, prob := store.mutate("ch", "s1", protocol.MessageActionAppend, " World", "",
		map[string]any{"ai": 1}, &protocol.MessageVersion{Serial: "v1"})
	require.Nil(t, prob)
	require.Equal(t, " World", op.Data, "the op carries the DELTA")
	require.Equal(t, protocol.MessageActionAppend, op.Action)
	state, ok := store.get("ch", "s1")
	require.True(t, ok)
	require.Equal(t, "Hello World", state.Data, "materialized data concatenates")
	require.Equal(t, protocol.MessageActionUpdate, state.Action, "append materializes as update")
	require.Equal(t, int64(100), state.Timestamp, "timestamp stays create-anchored")
	require.Equal(t, "v1", state.Version.Serial)

	// Update replaces.
	_, prob = store.mutate("ch", "s1", protocol.MessageActionUpdate, "Replaced", "",
		nil, &protocol.MessageVersion{Serial: "v2"})
	require.Nil(t, prob)
	state, _ = store.get("ch", "s1")
	require.Equal(t, "Replaced", state.Data)

	// Matching NON-empty chains do not concatenate either: base64+base64
	// string-concat would corrupt the payload (review-caught).
	_, prob = store.mutate("ch", "s1", protocol.MessageActionUpdate, "SGVsbG8=", "base64",
		nil, &protocol.MessageVersion{Serial: "v2a"})
	require.Nil(t, prob)
	_, prob = store.mutate("ch", "s1", protocol.MessageActionAppend, "IFdvcmxk", "base64",
		nil, &protocol.MessageVersion{Serial: "v2b"})
	require.Nil(t, prob)
	state, _ = store.get("ch", "s1")
	require.Equal(t, "IFdvcmxk", state.Data, "same-chain append REPLACES — concat would corrupt base64")
	require.Equal(t, "base64", state.Encoding)
	_, prob = store.mutate("ch", "s1", protocol.MessageActionUpdate, "Replaced", "",
		nil, &protocol.MessageVersion{Serial: "v2c"})
	require.Nil(t, prob)

	// Mismatched encodings do not concatenate (append replaces).
	_, prob = store.mutate("ch", "s1", protocol.MessageActionAppend, `{"x":1}`, "json",
		nil, &protocol.MessageVersion{Serial: "v3"})
	require.Nil(t, prob)
	state, _ = store.get("ch", "s1")
	require.Equal(t, `{"x":1}`, state.Data)
	require.Equal(t, "json", state.Encoding)

	// Delete takes the op's data and materializes as delete.
	_, prob = store.mutate("ch", "s1", protocol.MessageActionDelete, "{}", "json",
		nil, &protocol.MessageVersion{Serial: "v4"})
	require.Nil(t, prob)
	state, _ = store.get("ch", "s1")
	require.Equal(t, protocol.MessageActionDelete, state.Action)

	// Version snapshots accumulated oldest-first.
	versions, ok := store.versions("ch", "s1")
	require.True(t, ok)
	require.Len(t, versions, 7)
	require.Equal(t, "v1", versions[0].Version.Serial)
	require.Equal(t, "v4", versions[6].Version.Serial)

	// Unknown serial: not found.
	_, prob = store.mutate("ch", "nope", protocol.MessageActionUpdate, "x", "", nil, &protocol.MessageVersion{})
	require.NotNil(t, prob)
	require.Equal(t, errCodeNotFound, prob.code)
}

// RTL32: a realtime mutation frame applies the op, fans it out with the
// original serial and name plus the version, and ACKs with res
// serials[0] = versionSerial.
func TestRealtimeMutationRoundTrip_RTL32(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "mutable:rt-mutate"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	// Create.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:rt-mutate",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "orig", Data: "Hello"}},
	})
	var createdSerial string
	for createdSerial == "" {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		if probe.Action == protocol.ActionMessage {
			createdSerial = probe.Messages[0].Serial
		}
	}

	// Append by serial with an operation.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:rt-mutate",
		MsgSerial: 1,
		Messages: []*protocol.Message{{
			Serial: createdSerial,
			Action: protocol.MessageActionAppend,
			Data:   " World",
			Version: &protocol.MessageVersion{
				ClientID:    "operator",
				Description: "native append",
				Metadata:    map[string]any{"k": "v"},
			},
		}},
	})
	var versionSerial string
	var opMsg *protocol.Message
	for versionSerial == "" || opMsg == nil {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		switch probe.Action {
		case protocol.ActionAck:
			ack := readAckRes(t, raw)
			require.Len(t, ack.Res, 1)
			require.Len(t, ack.Res[0].Serials, 1)
			versionSerial = ack.Res[0].Serials[0]
		case protocol.ActionMessage:
			opMsg = probe.Messages[0]
		default:
			t.Fatalf("unexpected frame action %d", probe.Action)
		}
	}
	require.Equal(t, createdSerial, opMsg.Serial, "op carries the ORIGINAL serial")
	require.Equal(t, "orig", opMsg.Name, "op enriched with the original name")
	require.Equal(t, " World", opMsg.Data, "op carries the delta")
	require.Equal(t, protocol.MessageActionAppend, opMsg.Action)
	require.NotNil(t, opMsg.Version)
	require.Equal(t, versionSerial, opMsg.Version.Serial, "broadcast version.serial equals the ACKed versionSerial")
	require.Equal(t, "operator", opMsg.Version.ClientID)
	require.Equal(t, "native append", opMsg.Version.Description)
}

// Mutations on channels without the mutableMessages rule fail with 93002;
// unknown serials with 40400.
func TestMutationRejections(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "plain-mutate"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "plain-mutate",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Serial: "x:000", Action: protocol.MessageActionUpdate, Data: "y"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, errCodeMutableRequired, nack.Error.Code)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "mutable:rejections"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:rejections",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Serial: "nope:000", Action: protocol.MessageActionUpdate, Data: "y"}},
	})
	nack = readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, errCodeNotFound, nack.Error.Code)
}

// Mutable-channel rewind serves MATERIALIZED state: create + append then
// rewind=1 delivers ONE concatenated message with the latest version and
// action=update, HAS_BACKLOG set.
func TestMutableRewindServesMaterialized(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "mutable:rewind-mat"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:rewind-mat",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "orig", Data: "Hello"}},
	})
	var createdSerial string
	for createdSerial == "" {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		if probe.Action == protocol.ActionMessage {
			createdSerial = probe.Messages[0].Serial
		}
	}
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mutable:rewind-mat",
		MsgSerial: 1,
		Messages: []*protocol.Message{{
			Serial: createdSerial, Action: protocol.MessageActionAppend, Data: " World",
		}},
	})
	// Drain the append's ACK + op delivery.
	seen := 0
	for seen < 2 {
		raw := readRawNonHeartbeat(t, conn)
		var probe protocol.ProtocolMessage
		require.NoError(t, json.Unmarshal(raw, &probe))
		seen++
		_ = raw
	}

	// Fresh connection, rewind=1: one materialized message.
	conn2 := connectRealtime(t, ts)
	writeFrame(t, conn2, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "mutable:rewind-mat",
		Params:  map[string]string{"rewind": "1"},
	})
	attached := readNonHeartbeatFrame(t, conn2)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagHasBacklog, attached.Flags&protocol.FlagHasBacklog)

	backlog := readNonHeartbeatFrame(t, conn2)
	require.Equal(t, protocol.ActionMessage, backlog.Action)
	require.Len(t, backlog.Messages, 1)
	got := backlog.Messages[0]
	require.Equal(t, createdSerial, got.Serial)
	require.Equal(t, "Hello World", got.Data, "rewind delivers the CONCATENATED state, not the op stream")
	require.Equal(t, protocol.MessageActionUpdate, got.Action)
	require.Equal(t, "orig", got.Name)
	require.NotNil(t, got.Version)
}

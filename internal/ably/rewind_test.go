package ably

// Rewind (RTL2i): a fresh attach with a rewind param replays a backlog
// of retained messages after ATTACHED — HAS_BACKLOG set when non-empty,
// never RESUMED. Suppressed when the attach resumes (channelSerial
// cursor or ATTACH_RESUME flag; pinned by ably-js resume_rewind_1).

import (
	"fmt"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// RTL2i: rewind=N replays the last N retained messages, oldest first,
// with HAS_BACKLOG on ATTACHED.
func TestRewindCountReplaysBacklog_RTL2i(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// Seed three messages.
	pub := connectRealtime(t, ts)
	writeFrame(t, pub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "rewind-count-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, pub).Action)
	for i := range 3 {
		publishAndTakeSerial(t, pub, "rewind-count-test", fmt.Sprintf("m%d", i), int64(i))
	}

	// Fresh attach with rewind=2: HAS_BACKLOG, then m1 and m2 in order.
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "rewind-count-test",
		Params:  map[string]string{"rewind": "2"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagHasBacklog, attached.Flags&protocol.FlagHasBacklog, "backlog exists: HAS_BACKLOG")
	require.Zero(t, attached.Flags&protocol.FlagResumed, "rewind is a fresh attach: no RESUMED")

	first := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, first.Action)
	require.Equal(t, "m1", first.Messages[0].Name)
	second := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, second.Action)
	require.Equal(t, "m2", second.Messages[0].Name)
}

// RTL2i: rewind on an empty channel attaches without HAS_BACKLOG and
// replays nothing (rewind_has_backlog_0).
func TestRewindEmptyChannelNoBacklog(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "rewind-empty-test",
		Params:  map[string]string{"rewind": "1"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Zero(t, attached.Flags&protocol.FlagHasBacklog)

	// Nothing replayed: the next frame is the echo of a fresh publish.
	serial := publishAndTakeSerial(t, conn, "rewind-empty-test", "fresh", 0)
	require.NotEmpty(t, serial)
}

// Pinned by resume_rewind_1: an ATTACH carrying ATTACH_RESUME suppresses
// the rewind param — no backlog, no HAS_BACKLOG.
func TestRewindSuppressedOnAttachResume(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	pub := connectRealtime(t, ts)
	writeFrame(t, pub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "rewind-resume-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, pub).Action)
	publishAndTakeSerial(t, pub, "rewind-resume-test", "retained", 0)

	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "rewind-resume-test",
		Params:  map[string]string{"rewind": "1"},
		Flags:   protocol.FlagAttachResume,
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Zero(t, attached.Flags&protocol.FlagHasBacklog)

	// No replay: a fresh publish echo is the next frame on this channel.
	serial := publishAndTakeSerial(t, conn, "rewind-resume-test", "after", 0)
	require.NotEmpty(t, serial)
}

// A channelSerial cursor outranks the rewind param: the attach resumes
// from the cursor (RESUMED, the gap only) rather than rewinding.
func TestCursorOutranksRewind(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "rewind-cursor-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	publishAndTakeSerial(t, conn, "rewind-cursor-test", "seen-0", 0)
	cursor := publishAndTakeSerial(t, conn, "rewind-cursor-test", "seen-1", 1)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "rewind-cursor-test"})
	require.Equal(t, protocol.ActionDetached, readNonHeartbeatFrame(t, conn).Action)

	pub := connectRealtime(t, ts)
	writeFrame(t, pub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "rewind-cursor-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, pub).Action)
	publishAndTakeSerial(t, pub, "rewind-cursor-test", "gap-2", 0)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       "rewind-cursor-test",
		ChannelSerial: cursor,
		Params:        map[string]string{"rewind": "10"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagResumed, attached.Flags&protocol.FlagResumed, "cursor wins: resumed")

	// Only the gap replays — not the rewind-10 backlog.
	replayed := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, replayed.Action)
	require.Equal(t, "gap-2", replayed.Messages[0].Name)
}

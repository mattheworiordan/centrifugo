package ably

// B1 (M1): an inbound frame with an action this server does not handle, but
// carrying a msgSerial the client awaits an ACK for, must be NACKed — not
// silently dropped, which left the client's pending publish queue hanging
// forever.

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestUnknownActionWithMsgSerialNacked_B1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	// An action outside the handled set, carrying a msgSerial.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.Action(99),
		MsgSerial: 7,
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action, "an unhandled ACK-bearing frame must NACK, not hang")
	require.EqualValues(t, 7, nack.MsgSerial, "the NACK settles the client's pending serial")
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeBadRequest, nack.Error.Code)

	// The connection is NOT torn down by the NACK — a normal attach still works.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "after-unknown"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
}

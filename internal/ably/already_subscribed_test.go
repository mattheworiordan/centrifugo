package ably

// D2 (Redis engine): a centrifuge "already subscribed" (105) reply to an
// opSubscribe must be treated as SUCCESS — confirm ATTACHED, not fail the
// channel. Two rapid ATTACHes for one channel can both pass attach()'s
// alreadyAttached check before the first opSubscribe reply sets
// attachedModes; harmless on the synchronous memory engine, but on Redis
// the reply lags and the second Subscribe returns 105. Failing the channel
// (the old behaviour) drives the SDK channel to FAILED and abandons the
// live subscription → message delivery stalls (manifested as the
// updates-deletes "Should append to a message over realtime" timeout on
// Redis).

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
	cproto "github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAlreadySubscribedReplyAttaches_D2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)

	// A pending opSubscribe (as attach() would register before sending the
	// SubscribeRequest), then a 105 "already subscribed" reply for it.
	id := s.addPending(pendingOp{kind: opSubscribe, channel: "ai:dup", modes: protocol.FlagModeSubscribe})
	s.handleReply(&cproto.Reply{
		Id:    id,
		Error: &cproto.Error{Code: centrifuge.ErrorAlreadySubscribed.Code, Message: "already subscribed"},
	})

	// The channel is confirmed ATTACHED, NOT failed with an ERROR.
	var sawAttached, sawError bool
	conn.mu.Lock()
	for _, raw := range conn.frames {
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(raw, protocol.FormatJSON, &m); err != nil {
			continue
		}
		switch m.Action {
		case protocol.ActionAttached:
			if m.Channel == "ai:dup" {
				sawAttached = true
			}
		case protocol.ActionError:
			sawError = true
		}
	}
	conn.mu.Unlock()
	require.True(t, sawAttached, "a 105 'already subscribed' reply must confirm ATTACHED")
	require.False(t, sawError, "a 105 reply must NOT fail the channel with an ERROR")
}

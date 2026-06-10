package ably

// Channel modes and params (RTL4k/RTL4m): an ATTACH may restrict the
// attachment to a subset of abilities, requested either as params
// {"modes": "a,b"} (channelOptions.params form) or as mode flag bits
// (channelOptions.modes form). ATTACHED echoes the request — params
// verbatim (RTL4k1), granted modes as flag bits — and the session
// enforces the restriction: publish/presence NACK 40160, and
// subscribe/presence_subscribe gate delivery. Pinned by the ably-js
// attachWithChannelParams* / attachWithChannelModes tests
// (checkCantPublish and checkCantEnterPresence assert err.code 40160).

import (
	"net/http"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// RTL4k1 + RTL4m: params are echoed verbatim on ATTACHED, the granted
// mode appears as a flag bit, and abilities outside the granted modes
// NACK with 40160 (statusCode 401).
func TestChannelModesParamsEchoAndEnforcement_RTL4k_RTL4m(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-params-test",
		Params:  map[string]string{"modes": "subscribe", "delta": "vcdiff"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, map[string]string{"modes": "subscribe", "delta": "vcdiff"}, attached.Params)
	require.Equal(t, protocol.FlagModeSubscribe, attached.Flags&protocol.FlagModeSubscribe)
	require.Zero(t, attached.Flags&(protocol.FlagModePublish|protocol.FlagModePresence|protocol.FlagModePresenceSubscribe))

	// Publish on a subscribe-only attachment: 40160.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "modes-params-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "denied", Data: "x"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeOperationNotPermitted, nack.Error.Code)
	require.Equal(t, http.StatusUnauthorized, nack.Error.StatusCode)

	// Presence ENTER on a subscribe-only attachment: 40160.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-params-test",
		MsgSerial: 1,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "someone"}},
	})
	nack = readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeOperationNotPermitted, nack.Error.Code)
	require.Equal(t, http.StatusUnauthorized, nack.Error.StatusCode)
}

// RTL4m, flag-bits form (channelOptions.modes): the granted bits are
// echoed on ATTACHED, granted abilities work, and a DETACH clears the
// restriction so a fresh default attach is unrestricted again.
func TestChannelModesFlagBitsForm_RTL4m(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-flags-test",
		Flags:   protocol.FlagModePublish | protocol.FlagModeSubscribe,
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagModePublish|protocol.FlagModeSubscribe,
		attached.Flags&(protocol.FlagModePresence|protocol.FlagModePublish|protocol.FlagModeSubscribe|protocol.FlagModePresenceSubscribe))

	// Publish is within the granted modes: ACK, and the echo arrives (the
	// subscribe mode is granted too). ACK and echo order between
	// goroutines is not fixed.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "modes-flags-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "granted", Data: "x"}},
	})
	var sawAck, sawEcho bool
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionMessage:
			sawEcho = true
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.True(t, sawAck)
	require.True(t, sawEcho)

	// Presence is outside the granted modes: 40160.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-flags-test",
		MsgSerial: 1,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "someone"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, errCodeOperationNotPermitted, nack.Error.Code)

	// DETACH clears the restriction; a fresh default ATTACH grants the
	// full default mode set (real Ably always carries mode bits on
	// ATTACHED) and presence works again.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "modes-flags-test"})
	require.Equal(t, protocol.ActionDetached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "modes-flags-test"})
	reattached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, reattached.Action)
	require.Equal(t, defaultChannelModes, reattached.Flags&defaultChannelModes)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-flags-test",
		MsgSerial: 2,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "someone"}},
	})
	// ACK plus the presence event delivery (unrestricted now), order not
	// fixed across goroutines.
	var sawPresenceAck, sawPresenceEvent bool
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			sawPresenceAck = true
		case protocol.ActionPresence:
			sawPresenceEvent = true
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.True(t, sawPresenceAck)
	require.True(t, sawPresenceEvent)
}

// RTL4m delivery filtering, subscribe: an attachment without the
// subscribe mode receives no messages. Ordering marker (same idiom as
// TestRealtimeNoEcho_RTL7f): publications on one channel arrive in
// publish order, so if the filtered message had been delivered it would
// precede the presence marker on A's connection.
func TestChannelModesSubscribeFilter_RTL4m(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// A: publish + presence_subscribe only — no subscribe mode.
	connA := connectRealtime(t, ts)
	writeFrame(t, connA, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-subfilter-test",
		Params:  map[string]string{"modes": "publish,presence_subscribe"},
	})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connA).Action)

	// B: unrestricted.
	connB := connectRealtime(t, ts)
	writeFrame(t, connB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "modes-subfilter-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connB).Action)

	// B publishes a message (filtered for A), then enters presence (the
	// marker — A holds presence_subscribe so receives it).
	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "modes-subfilter-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "filtered", Data: "x"}},
	})
	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-subfilter-test",
		MsgSerial: 1,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "b-client"}},
	})

	// A's first frame is the presence marker: the message published before
	// it was never delivered.
	first := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionPresence, first.Action)
	require.Len(t, first.Presence, 1)
	require.Equal(t, "b-client", first.Presence[0].ClientID)
}

// RTL4m delivery filtering, presence_subscribe: an attachment without
// the presence_subscribe mode receives no presence events. Marker: B
// enters presence (filtered for A), then publishes a message — A holds
// subscribe so the message arrives, and it must arrive first.
func TestChannelModesPresenceSubscribeFilter_RTL4m(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// A: subscribe only — no presence_subscribe mode.
	connA := connectRealtime(t, ts)
	writeFrame(t, connA, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-presfilter-test",
		Params:  map[string]string{"modes": "subscribe"},
	})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connA).Action)

	// B: unrestricted.
	connB := connectRealtime(t, ts)
	writeFrame(t, connB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "modes-presfilter-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, connB).Action)

	// B enters presence (filtered for A), then publishes the marker.
	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-presfilter-test",
		MsgSerial: 0,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "b-client"}},
	})
	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "modes-presfilter-test",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "marker", Data: "x"}},
	})

	// A's first frame is the marker message: the presence event published
	// before it was never delivered.
	first := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionMessage, first.Action)
	require.Len(t, first.Messages, 1)
	require.Equal(t, "marker", first.Messages[0].Name)
}

// Pinned by ably-js attachWithInvalidChannelParams: a default attach is
// granted the full default mode set (including annotation_publish), and
// unrecognized params are dropped from the ATTACHED echo rather than
// echoed verbatim.
func TestChannelModesDefaultGrantAndParamsFilter(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-default-test",
		Params:  map[string]string{"nonexistent": "foo"},
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, defaultChannelModes, attached.Flags&defaultChannelModes)
	require.Equal(t, protocol.FlagModeAnnotationPublish, attached.Flags&protocol.FlagModeAnnotationPublish)
	require.Empty(t, attached.Params)
}

// Pinned by ably-js attachWithChannelParamsModesAndChannelModes ("modes is
// ignored when params.modes is present"): when an ATTACH carries both
// params.modes and mode flag bits, params.modes wins outright — no union.
func TestChannelModesParamsPrecedenceOverFlags(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-precedence-test",
		Params:  map[string]string{"modes": "presence,subscribe"},
		Flags:   protocol.FlagModePublish | protocol.FlagModePresenceSubscribe,
	})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, protocol.FlagModePresence|protocol.FlagModeSubscribe,
		attached.Flags&(protocol.FlagModePresence|protocol.FlagModePublish|protocol.FlagModeSubscribe|protocol.FlagModePresenceSubscribe))
}

// RTL4h territory, pinned by ably-js setOptionsCallbackBehaviour: an
// ATTACH for an already-attached channel is an options update — confirmed
// with a fresh ATTACHED carrying the new grant (no detach round-trip),
// and the new modes take effect.
func TestChannelReattachUpdatesModes(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	// Default attach: unrestricted, presence works.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "modes-reattach-test"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-reattach-test",
		MsgSerial: 0,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "someone"}},
	})
	var sawAck, sawEvent bool
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionPresence:
			sawEvent = true
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.True(t, sawAck)
	require.True(t, sawEvent)

	// Re-attach restricting to publish: a fresh ATTACHED confirms the
	// update with the new mode bits echoed.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "modes-reattach-test",
		Params:  map[string]string{"modes": "publish"},
	})
	// The member entered above is still present, but the new grant lacks
	// presence_subscribe, so no HAS_PRESENCE/SYNC follows — the update
	// frame itself is next.
	updated := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, updated.Action)
	require.Equal(t, map[string]string{"modes": "publish"}, updated.Params)
	require.Equal(t, protocol.FlagModePublish,
		updated.Flags&(protocol.FlagModePresence|protocol.FlagModePublish|protocol.FlagModeSubscribe|protocol.FlagModePresenceSubscribe))

	// The restriction is live: presence now NACKs 40160.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "modes-reattach-test",
		MsgSerial: 1,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceUpdate, ClientID: "someone"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, errCodeOperationNotPermitted, nack.Error.Code)
}

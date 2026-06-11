package ably

// Broker channel-name mapping: Ably names with arbitrary colons (legal —
// pinned by ably-js's own `publish` tests, whose channel names embed
// JSON) must work end-to-end despite centrifuge treating the first
// ':'-segment as a namespace. The escape is bijective; everything wire-
// visible stays in Ably names.

import (
	"net/url"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestBrokerChannelMappingBijective(t *testing.T) {
	t.Parallel()
	cases := []struct{ ably, broker string }{
		{"plain", "plain"},
		{"with:colon", "with~1colon"},
		{`publish {"transports":["web_socket"],"useBinaryProtocol":true}`,
			`publish {"transports"~1["web_socket"],"useBinaryProtocol"~1true}`},
		{"tilde~name", "tilde~0name"},
		{"literal~1seq", "literal~01seq"},
		{":presence:foo:bar", "~1presence~1foo~1bar"},
		{"persisted:history", "persisted~1history"},
		{"~", "~0"}, {":", "~1"}, {"~:~", "~0~1~0"},
	}
	for _, c := range cases {
		require.Equal(t, c.broker, brokerChannel(c.ably), "encode %q", c.ably)
		require.Equal(t, c.ably, ablyChannel(c.broker), "decode %q", c.broker)
	}
	// No two distinct Ably names may share a broker name.
	seen := map[string]string{}
	for _, c := range cases {
		if prev, ok := seen[c.broker]; ok {
			t.Fatalf("collision: %q and %q both map to %q", prev, c.ably, c.broker)
		}
		seen[c.broker] = c.ably
	}
}

// End-to-end on a colon-bearing channel name: attach, publish/echo with
// the frame carrying the ABLY name, history via REST, and a rewind —
// the full lifecycle that used to die on centrifuge namespace resolution.
func TestColonChannelNameLifecycle(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)
	name := `weird {"transports":["web_socket"]}:channel`

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: name})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, name, attached.Channel, "ATTACHED carries the ABLY name")

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   name,
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "ev", Data: "x"}},
	})
	var delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.Equal(t, name, delivered.Channel, "delivery decodes back to the ABLY name")
	require.Equal(t, "ev", delivered.Messages[0].Name)

	// REST history on the same name (URL-encoded path segment).
	resp := restRequest(t, ts, "GET", "/channels/"+url.PathEscape(name)+"/messages", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	items := decodeMessagesBody(t, resp)
	require.Len(t, items, 1)
	require.Equal(t, "ev", items[0].Name)

	// Rewind on a fresh connection sees the backlog.
	conn2 := connectRealtime(t, ts)
	writeFrame(t, conn2, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: name,
		Params:  map[string]string{"rewind": "1"},
	})
	att2 := readNonHeartbeatFrame(t, conn2)
	require.Equal(t, protocol.ActionAttached, att2.Action)
	require.Equal(t, protocol.FlagHasBacklog, att2.Flags&protocol.FlagHasBacklog)
	backlog := readNonHeartbeatFrame(t, conn2)
	require.Equal(t, protocol.ActionMessage, backlog.Action)
	require.Equal(t, name, backlog.Channel)
}

// Presence on a colon-bearing name: enter fans out, REST presence and
// presence history see the member (shadow channel escaping).
func TestColonChannelPresence(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Set("clientId", "colon-carl")
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)
	name := "presence:in:parts"
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: name})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: name, MsgSerial: 0,
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	var sawAck, sawEnter bool
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionPresence:
			sawEnter = true
			require.Equal(t, name, m.Channel)
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.True(t, sawAck)
	require.True(t, sawEnter)

	resp := restRequest(t, ts, "GET", "/channels/"+url.PathEscape(name)+"/presence", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp = restRequest(t, ts, "GET", "/channels/"+url.PathEscape(name)+"/presence/history", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
}

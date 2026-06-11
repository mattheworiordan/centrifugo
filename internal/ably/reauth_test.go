package ably

// Mid-connection reauth (RTC8): an AUTH frame carrying a fresh token is
// verified by the same path as connect-time auth — success adopts the
// new capability/expiry and is acknowledged with CONNECTED (same
// connectionId, an update); failure fails the CONNECTION with a
// channel-less ERROR. Token expiry without renewal disconnects with
// 40142 (the SDK's renew-and-reconnect signal).

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// RTC8a: a capability upgrade via AUTH takes effect live — a channel the
// first token could not attach becomes attachable after renewal, and the
// ack CONNECTED preserves the connectionId.
func TestAuthFrameUpgradesCapability_RTC8(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWTCapability(t, "auth-amy",
		time.Now().Add(time.Hour), `{"allowed":["*"]}`))
	conn := dialRealtime(t, ts.wsURL, params)
	connected := readFrame(t, conn)
	require.Equal(t, protocol.ActionConnected, connected.Action)

	// Outside the first token's capability: channel fails 40160.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "upgraded"})
	denied := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionError, denied.Action)
	require.Equal(t, errCodeOperationNotPermitted, denied.Error.Code)

	// AUTH with a broader token: CONNECTED ack, same connectionId.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth: &protocol.AuthDetails{AccessToken: mintSessionJWTCapability(t, "auth-amy",
			time.Now().Add(time.Hour), `{"*":["*"]}`)},
	})
	ack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionConnected, ack.Action)
	require.Equal(t, connected.ConnectionID, ack.ConnectionID, "reauth is an update, not a new connection")

	// The upgrade is live.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "upgraded"})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
}

// A renewal carrying a DIFFERENT identity is incompatible (RTC8a1-lite):
// the connection fails with 40102.
func TestAuthFrameIncompatibleClientID(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWT(t, "auth-original", time.Now().Add(time.Hour)))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: mintSessionJWT(t, "auth-imposter", time.Now().Add(time.Hour))},
	})
	failure := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionError, failure.Action)
	require.Empty(t, failure.Channel, "connection-level error carries no channel")
	require.Equal(t, errCodeIncompatibleCredentials, failure.Error.Code)
}

// An invalid renewal token fails the connection with the token error.
func TestAuthFrameInvalidToken(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWT(t, "auth-iris", time.Now().Add(time.Hour)))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: "not.a.token"},
	})
	failure := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionError, failure.Action)
	require.Equal(t, errCodeInvalidCredentials, failure.Error.Code)
}

// RTN15-territory: a token that expires mid-connection without renewal
// draws DISCONNECTED 40142 and the transport drops.
func TestTokenExpiryDisconnects40142(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWT(t, "auth-evan", time.Now().Add(1200*time.Millisecond)))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	disconnected := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionDisconnected, disconnected.Action)
	require.Equal(t, 40142, disconnected.Error.Code)
}

// A successful AUTH renewal extends the expiry: the connection outlives
// the FIRST token's exp.
func TestAuthFrameExtendsExpiry(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWT(t, "auth-ed", time.Now().Add(1200*time.Millisecond)))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: mintSessionJWT(t, "auth-ed", time.Now().Add(time.Hour))},
	})
	require.Equal(t, protocol.ActionConnected, readNonHeartbeatFrame(t, conn).Action)

	// Past the first token's exp: still connected — a publish round-trips.
	time.Sleep(1500 * time.Millisecond)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "extended"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
}

// RTN22: the server asks for renewal with a client-bound AUTH ahead of
// token expiry (the ~30s service lead). A token living longer than the
// lead draws the AUTH at exp-lead; the test shrinks the margin by minting
// just over the lead.
func TestServerInitiatedAuthBeforeExpiry_RTN22(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	// Expires just past the lead: the pre-expiry AUTH fires almost
	// immediately (exp - 30s ≈ +1.2s).
	params.Set("access_token", mintSessionJWT(t, "auth-renew",
		time.Now().Add(serverAuthLeadTime+1200*time.Millisecond)))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	// The next non-heartbeat frame is the server-initiated AUTH.
	frame := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAuth, frame.Action)

	// Renewing re-arms: the connection survives past the original exp.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: mintSessionJWT(t, "auth-renew", time.Now().Add(time.Hour))},
	})
	require.Equal(t, protocol.ActionConnected, readNonHeartbeatFrame(t, conn).Action)
	time.Sleep(1500 * time.Millisecond)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "renewed"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
}

// RTC8a1: a reauth whose capability drops an attached channel fails that
// channel with 40160 while unaffected attachments continue working.
func TestAuthFrameDowngradeFailsRevokedChannel(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Del("key")
	params.Set("access_token", mintSessionJWTCapability(t, "downgrade-dora",
		time.Now().Add(time.Hour), `{"keep":["*"],"lose":["*"]}`))
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)

	for _, ch := range []string{"keep", "lose"} {
		writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: ch})
		require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	}

	// Reauth without "lose".
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth: &protocol.AuthDetails{AccessToken: mintSessionJWTCapability(t, "downgrade-dora",
			time.Now().Add(time.Hour), `{"keep":["*"]}`)},
	})
	require.Equal(t, protocol.ActionConnected, readNonHeartbeatFrame(t, conn).Action)
	failed := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionError, failed.Action)
	require.Equal(t, "lose", failed.Channel)
	require.Equal(t, errCodeOperationNotPermitted, failed.Error.Code)

	// "keep" still works end-to-end.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionMessage, Channel: "keep", MsgSerial: 0,
		Messages: []*protocol.Message{{Name: "still-alive", Data: "x"}},
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

	// Re-attaching the revoked channel is refused by the normal gate.
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "lose"})
	refused := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionError, refused.Action)
	require.Equal(t, errCodeOperationNotPermitted, refused.Error.Code)
}

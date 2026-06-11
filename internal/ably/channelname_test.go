package ably

// A4 (HIGH): channel names are length-capped at the boundary, so a hostile
// over-length name can never become a permanent key in the channel-keyed
// stores (the trivial OOM vector). Over-length names are rejected as
// invalid channel names (40010), the same code the SDK already handles.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// Unit: the pure validator caps length while still accepting names at the
// limit and rejecting empty / colon-prefixed names.
func TestValidChannelNameLengthCap_A4(t *testing.T) {
	t.Parallel()
	require.True(t, validChannelName("normal-channel"))
	require.True(t, validChannelName(strings.Repeat("a", maxChannelNameBytes)), "a name AT the cap is valid")
	require.False(t, validChannelName(strings.Repeat("a", maxChannelNameBytes+1)), "a name OVER the cap is invalid")
	// Existing rules still hold.
	require.False(t, validChannelName(""))
	require.False(t, validChannelName(":leading-colon"))
}

// Wire: a realtime publish to an over-length channel NACKs 40010 and never
// reaches the broker / mint.
func TestRealtimePublishOverlongChannelNacked_A4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	overlong := strings.Repeat("c", maxChannelNameBytes+1)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   overlong,
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "ev", Data: "x"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeInvalidChannelName, nack.Error.Code)
}

// Wire: a REST publish to an over-length channel is rejected 400/40010.
func TestRESTPublishOverlongChannelRejected_A4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	overlong := strings.Repeat("c", maxChannelNameBytes+1)
	resp := restRequest(t, ts, http.MethodPost, "/channels/"+overlong+"/messages",
		[]byte(`{"name":"ev","data":"x"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40010", resp.Header.Get("X-Ably-Errorcode"))
}

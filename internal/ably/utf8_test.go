package ably

// C3: channel names and clientIds must be valid UTF-8 — they become map
// keys (presence, serial generators, materialized state) and broker names,
// so invalid UTF-8 must be rejected at the boundary rather than flowing
// through raw. (JSON coerces invalid UTF-8 to U+FFFD on the wire, so the
// realistic vectors are the REST URL path and msgpack.)

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUTF8Validation_C3(t *testing.T) {
	t.Parallel()
	bad := string([]byte{0xff, 0xfe}) // invalid UTF-8

	require.False(t, validChannelName(bad), "invalid-UTF-8 channel name is rejected")
	require.True(t, validChannelName("normal:channel"))

	require.False(t, validClientID(bad), "invalid-UTF-8 clientId is rejected")
	require.True(t, validClientID("alice"))
	require.True(t, validClientID(""), "empty clientId (unidentified) is valid")
}

// A REST publish to a channel whose percent-decoded path is invalid UTF-8 is
// rejected 40010 — channelRoute's url.PathUnescape yields the raw bytes,
// which validChannelName now refuses.
func TestRESTPublishInvalidUTF8ChannelRejected_C3(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	resp := restRequest(t, ts, http.MethodPost, "/channels/%FF%FE/messages",
		[]byte(`{"name":"x","data":"y"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40010", resp.Header.Get("X-Ably-Errorcode"))
}

// A REST publish whose X-Ably-ClientId header Base64-decodes to invalid UTF-8
// is rejected 40012 — the decoded bytes would otherwise be stamped onto
// messages as the publisher identity.
func TestRESTPublishInvalidUTF8ClientIDHeaderRejected_C3(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	badClientID := base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe})
	resp := restRequest(t, ts, http.MethodPost, "/channels/c3-cid/messages",
		[]byte(`{"name":"x","data":"y"}`),
		map[string]string{"Content-Type": "application/json", "X-Ably-ClientId": badClientID})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40012", resp.Header.Get("X-Ably-Errorcode"))
}

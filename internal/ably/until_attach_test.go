package ably

// untilAttach history (RSL2b-adjacent): ably-js turns history
// {untilAttach: true} into from_serial=<attachSerial> on the REST
// request; the server bounds the read at that serial's publication,
// inclusive — only pre-attach messages return.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

// seedSerials publishes n messages over realtime and returns each
// delivered frame's channelSerial, in publish order.
func seedSerials(t *testing.T, ts *realtimeTestServer, channel string, n int) []string {
	t.Helper()
	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: channel})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)
	serials := make([]string, 0, n)
	for i := range n {
		serials = append(serials, publishAndTakeSerial(t, conn, channel, fmt.Sprintf("m%d", i), int64(i)))
	}
	return serials
}

// nextLinkQuery extracts the query string from the response's
// rel="next" Link header ("" when absent).
func nextLinkQuery(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, l := range resp.Header.Values("Link") {
		if !strings.Contains(l, `rel="next"`) {
			continue
		}
		start := strings.Index(l, "<")
		end := strings.Index(l, ">")
		require.True(t, start >= 0 && end > start)
		link := l[start+1 : end]
		require.True(t, strings.HasPrefix(link, "./messages?"))
		return strings.TrimPrefix(link, "./messages?")
	}
	return ""
}

func historyNames(t *testing.T, items []*protocol.Message) []string {
	t.Helper()
	names := make([]string, 0, len(items))
	for _, m := range items {
		names = append(names, m.Name)
	}
	return names
}

// from_serial bounds a backwards read at the attach point inclusive:
// only messages up to that serial return, newest first.
func TestHistoryFromSerialBackwards(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	serials := seedSerials(t, ts, "until-attach-test", 5)

	// Unbounded: all five.
	resp := restRequest(t, ts, http.MethodGet, "/channels/until-attach-test/messages", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, decodeMessagesBody(t, resp), 5)

	// Bounded at message 2 (index 2): exactly m2, m1, m0.
	resp = restRequest(t, ts, http.MethodGet,
		"/channels/until-attach-test/messages?from_serial="+serials[2], nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items := decodeMessagesBody(t, resp)
	require.Equal(t, []string{"m2", "m1", "m0"}, historyNames(t, items))
}

// The bound survives pagination: page links continue below the attach
// point and stop at the channel start.
func TestHistoryFromSerialPagination(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	serials := seedSerials(t, ts, "until-attach-page-test", 5)

	resp := restRequest(t, ts, http.MethodGet,
		"/channels/until-attach-page-test/messages?limit=2&from_serial="+serials[3], nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, []string{"m3", "m2"}, historyNames(t, decodeMessagesBody(t, resp)))

	next := nextLinkQuery(t, resp)
	require.NotEmpty(t, next, "more pre-attach messages exist: next link present")
	resp = restRequest(t, ts, http.MethodGet, "/channels/until-attach-page-test/messages?"+next, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, []string{"m1", "m0"}, historyNames(t, decodeMessagesBody(t, resp)))
}

// Forwards reads enforce the same inclusive bound from the oldest end.
func TestHistoryFromSerialForwards(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	serials := seedSerials(t, ts, "until-attach-fwd-test", 5)

	resp := restRequest(t, ts, http.MethodGet,
		"/channels/until-attach-fwd-test/messages?direction=forwards&from_serial="+serials[2], nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, []string{"m0", "m1", "m2"}, historyNames(t, decodeMessagesBody(t, resp)))
}

// An attach point that has left the retention window returns an empty
// page — nothing retained is pre-attach.
func TestHistoryFromSerialUnresolvable(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	seedSerials(t, ts, "until-attach-gone-test", 2)

	resp := restRequest(t, ts, http.MethodGet,
		"/channels/until-attach-gone-test/messages?from_serial=00000000000000-000@aaaaaaaaaa", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, decodeMessagesBody(t, resp))
}

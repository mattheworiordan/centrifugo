package ably

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/stretchr/testify/require"
)

// restRequest performs an authenticated REST request against the test
// server and returns the response.
func restRequest(t *testing.T, ts *realtimeTestServer, method, path string, body []byte, header map[string]string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.srv.URL+path, reader)
	require.NoError(t, err)
	req.SetBasicAuth("poc.key0", "secret_key0_0123456789abcdef")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func decodeMessagesBody(t *testing.T, resp *http.Response) []*protocol.Message {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var msgs []*protocol.Message
	require.NoError(t, json.Unmarshal(data, &msgs))
	return msgs
}

// RSL1: POST /channels/{ch}/messages accepts a single message object and a
// bare array, returns 201, and the messages fan out to realtime
// subscribers and into history.
func TestRESTPublish_RSL1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "persisted:rest-pub"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	// Single object body (RSL1a).
	resp := restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-pub/messages",
		[]byte(`{"name":"one","data":"d1"}`), map[string]string{"Content-Type": contentTypeJSON})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	delivered := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionMessage, delivered.Action)
	require.Equal(t, "one", delivered.Messages[0].Name)
	require.NotEmpty(t, delivered.Messages[0].ID)        // TM2a server-assigned
	require.Empty(t, delivered.Messages[0].ConnectionID) // REST: no connection attribution
	require.NotZero(t, delivered.Messages[0].Timestamp)

	// Bare array body (RSL1c shape): two messages, in order.
	resp = restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-pub/messages",
		[]byte(`[{"name":"two","data":"d2"},{"name":"three","data":"d3"}]`),
		map[string]string{"Content-Type": contentTypeJSON})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "two", readNonHeartbeatFrame(t, conn).Messages[0].Name)
	require.Equal(t, "three", readNonHeartbeatFrame(t, conn).Messages[0].Name)

	// History sees all three (RSL2), backwards default.
	resp = restRequest(t, ts, http.MethodGet, "/channels/persisted:rest-pub/messages", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	msgs := decodeMessagesBody(t, resp)
	require.Len(t, msgs, 3)
	require.Equal(t, "three", msgs[0].Name) // newest first
	require.Equal(t, "one", msgs[2].Name)
}

// REST publish error contract: auth (40101), channel name (40010),
// oversize (40009) — all with X-Ably-Errorcode headers.
func TestRESTPublishErrors(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// No credentials.
	req, err := http.NewRequest(http.MethodPost, ts.srv.URL+"/channels/c/messages",
		strings.NewReader(`{"name":"x"}`))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "40101", resp.Header.Get("X-Ably-Errorcode"))

	// Invalid channel name.
	resp = restRequest(t, ts, http.MethodPost, "/channels/%3Ahell/messages",
		[]byte(`{"name":"x"}`), map[string]string{"Content-Type": contentTypeJSON})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40010", resp.Header.Get("X-Ably-Errorcode"))

	// Oversize (CD2c/TO3l8 counterpart RSL1i).
	big := fmt.Sprintf(`{"name":"big","data":%q}`, strings.Repeat("x", maxMessageSize+1))
	resp = restRequest(t, ts, http.MethodPost, "/channels/c/messages",
		[]byte(big), map[string]string{"Content-Type": contentTypeJSON})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40009", resp.Header.Get("X-Ably-Errorcode"))
}

// RSA7e2: the Base64 X-Ably-ClientId header identifies a Basic-auth REST
// publisher — implicit stamping (RSL1m1) and mismatch reject (RSL1m4).
func TestRESTPublishClientIDHeader_RSA7e2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	b64 := base64.StdEncoding.EncodeToString([]byte("rest-bob"))
	resp := restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-cid/messages",
		[]byte(`{"name":"implicit"}`),
		map[string]string{"Content-Type": contentTypeJSON, "X-Ably-ClientId": b64})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = restRequest(t, ts, http.MethodGet, "/channels/persisted:rest-cid/messages", nil, nil)
	msgs := decodeMessagesBody(t, resp)
	require.Len(t, msgs, 1)
	require.Equal(t, "rest-bob", msgs[0].ClientID) // RSL1m1

	// Incompatible explicit clientId → 40012, nothing published (RSL1m4).
	resp = restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-cid/messages",
		[]byte(`{"name":"bad","clientId":"alice"}`),
		map[string]string{"Content-Type": contentTypeJSON, "X-Ably-ClientId": b64})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40012", resp.Header.Get("X-Ably-Errorcode"))
}

// RSL2 pagination: limit pages walk the stream via Link rel="next" until
// exhausted, newest first, with the relative `./messages?...` URL shape
// ably-js requires.
func TestRESTHistoryPagination_RSL2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	for i := range 5 {
		resp := restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-page/messages",
			fmt.Appendf(nil, `{"name":"m%d"}`, i), map[string]string{"Content-Type": contentTypeJSON})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	var got []string
	path := "/channels/persisted:rest-page/messages?limit=2&direction=backwards"
	for page := 0; ; page++ {
		require.Less(t, page, 5, "pagination must terminate")
		resp := restRequest(t, ts, http.MethodGet, path, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		for _, m := range decodeMessagesBody(t, resp) {
			got = append(got, m.Name)
		}
		next := ""
		for _, l := range resp.Header.Values("Link") {
			if strings.Contains(l, `rel="next"`) {
				start, end := strings.IndexByte(l, '<'), strings.IndexByte(l, '>')
				require.True(t, start >= 0 && end > start)
				next = l[start+1 : end]
			}
		}
		if next == "" {
			break
		}
		require.True(t, strings.HasPrefix(next, "./messages?"), "ably-js requires relative ./messages link, got %q", next)
		path = "/channels/persisted:rest-page/" + strings.TrimPrefix(next, "./")
	}
	require.Equal(t, []string{"m4", "m3", "m2", "m1", "m0"}, got)
}

// GET /channels/{ch}: the status0 ChannelDetails shape — channelId plus
// the six numeric occupancy metrics.
func TestRESTChannelDetails(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	conn := connectRealtime(t, ts)
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "details-chan"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, conn).Action)

	resp := restRequest(t, ts, http.MethodGet, "/channels/details-chan", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var details struct {
		ChannelID string `json:"channelId"`
		Status    struct {
			IsActive  bool `json:"isActive"`
			Occupancy struct {
				Metrics map[string]int `json:"metrics"`
			} `json:"occupancy"`
		} `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&details))
	require.Equal(t, "details-chan", details.ChannelID)
	require.True(t, details.Status.IsActive)
	require.GreaterOrEqual(t, details.Status.Occupancy.Metrics["subscribers"], 1)
	for _, k := range []string{"connections", "publishers", "subscribers", "presenceConnections", "presenceMembers", "presenceSubscribers"} {
		_, present := details.Status.Occupancy.Metrics[k]
		require.True(t, present, "metric %s must be present", k)
	}
}

// RSL1k2/RSL1k5: republishing a message with the same client-supplied id
// within the dedup window stores it once; server-generated ids never
// dedup.
func TestRESTPublishIdempotent_RSL1k2(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	body := []byte(`{"name":"idem","data":"d","id":"client-id:0"}`)
	for range 3 {
		resp := restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-idem/messages",
			body, map[string]string{"Content-Type": contentTypeJSON})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}
	// Distinct server-generated ids: two more stored messages.
	for range 2 {
		resp := restRequest(t, ts, http.MethodPost, "/channels/persisted:rest-idem/messages",
			[]byte(`{"name":"fresh","data":"d"}`), map[string]string{"Content-Type": contentTypeJSON})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	resp := restRequest(t, ts, http.MethodGet, "/channels/persisted:rest-idem/messages", nil, nil)
	msgs := decodeMessagesBody(t, resp)
	require.Len(t, msgs, 3, "3x same-id stores once; 2x fresh store twice")
	require.Equal(t, "client-id:0", msgs[2].ID, "client-supplied id preserved (oldest)")
}

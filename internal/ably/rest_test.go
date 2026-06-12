package ably

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

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

// RSA8/RSA9: POST /keys/{keyName}/requestToken verifies the HMAC-signed
// TokenRequest (the mac is the authentication) and returns TokenDetails
// whose token the adapter's own verifier accepts.
func TestRequestToken_RSA8(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	const keyName = "poc.key0"
	const secret = "secret_key0_0123456789abcdef"
	sign := func(ttl, capability, clientID, timestamp, nonce string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(keyName + "\n" + ttl + "\n" + capability + "\n" + clientID + "\n" + timestamp + "\n" + nonce + "\n"))
		return base64.StdEncoding.EncodeToString(mac.Sum(nil))
	}
	post := func(t *testing.T, body string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, ts.srv.URL+"/keys/"+keyName+"/requestToken", strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", contentTypeJSON)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	ts1 := time.Now().UnixMilli()
	tsStr := strconv.FormatInt(ts1, 10)

	t.Run("valid signed request mints a verifiable token", func(t *testing.T) {
		mac := sign("", "", "minted-bob", tsStr, "nonce-1")
		resp := post(t, fmt.Sprintf(`{"keyName":%q,"clientId":"minted-bob","timestamp":%d,"nonce":"nonce-1","mac":%q}`, keyName, ts1, mac))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var details struct {
			Token    string `json:"token"`
			KeyName  string `json:"keyName"`
			Issued   int64  `json:"issued"`
			Expires  int64  `json:"expires"`
			ClientID string `json:"clientId"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&details))
		require.Equal(t, keyName, details.KeyName)
		require.Equal(t, "minted-bob", details.ClientID)
		require.Greater(t, details.Expires, details.Issued)
		// Default TTL one hour (TK2a).
		require.InDelta(t, time.Hour.Milliseconds(), details.Expires-details.Issued, 1000)

		// The minted token authenticates a realtime connection with the
		// token-bound identity.
		params := url.Values{}
		params.Set("format", "json")
		params.Set("v", "6")
		params.Set("access_token", details.Token)
		conn := dialRealtime(t, ts.wsURL, params)
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionConnected, m.Action)
		require.Equal(t, "minted-bob", m.ConnectionDetails.ClientID)
	})

	t.Run("ttl signs as its literal decimal", func(t *testing.T) {
		mac := sign("60000", "", "", tsStr, "nonce-2")
		resp := post(t, fmt.Sprintf(`{"keyName":%q,"ttl":60000,"timestamp":%d,"nonce":"nonce-2","mac":%q}`, keyName, ts1, mac))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var details struct{ Issued, Expires int64 }
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&details))
		require.InDelta(t, int64(60000), details.Expires-details.Issued, 1000)
	})

	t.Run("bad mac rejected 40101", func(t *testing.T) {
		resp := post(t, fmt.Sprintf(`{"keyName":%q,"timestamp":%d,"nonce":"nonce-3","mac":"AAAA"}`, keyName, ts1))
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Equal(t, "40101", resp.Header.Get("X-Ably-Errorcode"))
	})

	t.Run("keyName mismatch rejected", func(t *testing.T) {
		mac := sign("", "", "", tsStr, "nonce-4")
		resp := post(t, fmt.Sprintf(`{"keyName":"poc.key1","timestamp":%d,"nonce":"nonce-4","mac":%q}`, ts1, mac))
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	// RSA9d replay protection at the HTTP layer: the SAME signed
	// TokenRequest (identical nonce+timestamp+mac) mints once, burning the
	// nonce; a re-POST within the tolerance window is rejected 401. The nonce
	// is burned only AFTER the mac verifies (resttoken.go), so this exercises
	// nonceCache.use() on the live requestToken path — not just the unit test
	// in noncecache_test.go. This pins the replay-rejection claim in
	// resttoken.go.
	t.Run("replayed nonce rejected 40101", func(t *testing.T) {
		mac := sign("", "", "replay-bob", tsStr, "nonce-replay")
		body := fmt.Sprintf(`{"keyName":%q,"clientId":"replay-bob","timestamp":%d,"nonce":"nonce-replay","mac":%q}`, keyName, ts1, mac)

		first := post(t, body)
		require.Equal(t, http.StatusOK, first.StatusCode, "fresh nonce mints a token")

		replay := post(t, body)
		require.Equal(t, http.StatusUnauthorized, replay.StatusCode, "same nonce within the window is a replay")
		require.Equal(t, "40101", replay.Header.Get("X-Ably-Errorcode"))
	})
}

// REST presence: the member set as a bare array (fixture members seeded
// at startup for persisted:presence_fixtures), plus live members from
// realtime ENTERs; capability-gated.
func TestRESTPresence(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// The harness fixture seeds 6 members with verbatim encodings.
	resp := restRequest(t, ts, http.MethodGet, "/channels/persisted:presence_fixtures/presence", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var members []*protocol.PresenceMessage
	require.NoError(t, json.Unmarshal(data, &members))
	require.Len(t, members, 6)
	byClient := map[string]*protocol.PresenceMessage{}
	for _, m := range members {
		require.Equal(t, protocol.PresencePresent, m.Action)
		byClient[m.ClientID] = m
	}
	require.Contains(t, byClient, "client_string")
	require.Equal(t, "json/utf-8/cipher+aes-128-cbc/base64", byClient["client_encoded"].Encoding, "cipher chain verbatim")

	// A live ENTER appears in REST presence.
	params := defaultDialParams()
	params.Set("clientId", "live-carol")
	conn := dialRealtime(t, ts.wsURL, params)
	require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action: protocol.ActionPresence, Channel: "rest-pres-live", MsgSerial: 0,
		Presence: []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	require.Equal(t, protocol.ActionAck, readNonHeartbeatFrame(t, conn).Action)

	resp = restRequest(t, ts, http.MethodGet, "/channels/rest-pres-live/presence", nil, nil)
	data, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	members = nil
	require.NoError(t, json.Unmarshal(data, &members))
	require.Len(t, members, 1)
	require.Equal(t, "live-carol", members[0].ClientID)
}

// Retention tiers: mutable-messages channels (mutable:/ai:) keep the
// persisted tier, not the ephemeral one — AIT late-join hydration reads
// channel history, so an ai: conversation must outlive the ephemeral
// window (caught live: a second demo tab found no backlog once the 2-min
// TTL passed). The size cap distinguishes the tiers without waiting on
// TTL: ephemeral retains 100, persisted 1000.
func TestRetentionTierByChannelConvention(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	const n = 120 // above the ephemeral cap, below the persisted one
	publish := func(channel string) {
		t.Helper()
		var batch []map[string]any
		for i := 0; i < n; i++ {
			batch = append(batch, map[string]any{"name": fmt.Sprintf("m%03d", i)})
		}
		body, err := json.Marshal(batch)
		require.NoError(t, err)
		resp := restRequest(t, ts, http.MethodPost, "/channels/"+url.PathEscape(channel)+"/messages",
			body, map[string]string{"Content-Type": contentTypeJSON})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}
	count := func(channel string) int {
		t.Helper()
		total := 0
		path := "/channels/" + url.PathEscape(channel) + "/messages?limit=100&direction=backwards"
		for page := 0; ; page++ {
			resp := restRequest(t, ts, http.MethodGet, path, nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			items := decodeMessagesBody(t, resp)
			total += len(items)
			next := nextLinkQuery(t, resp)
			t.Logf("%s page%d: items=%d next=%q", channel, page, len(items), next)
			require.Less(t, page, 5, "pagination must terminate")
			if next == "" || len(items) == 0 {
				return total
			}
			path = "/channels/" + url.PathEscape(channel) + "/messages?" + next
		}
	}

	for _, channel := range []string{"ai:tier", "mutable:tier", "persisted:tier"} {
		publish(channel)
		require.Equal(t, n, count(channel), "%s retains the full backlog (persisted tier)", channel)
	}
	publish("plain-tier")
	require.Equal(t, ephemeralHistorySize, count("plain-tier"),
		"unprefixed channels keep the ephemeral cap")
}

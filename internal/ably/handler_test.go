package ably

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"

	"github.com/centrifugal/centrifuge"
	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Keys from auth/testdata/static-app.json.
const (
	testKey     = "poc.key0:secret_key0_0123456789abcdef"
	testKeyName = "poc.key0"
)

// deniedChannel is rejected by the test node's subscribe handler, standing
// in for centrifugo's permission chain (which rejects identically, with
// centrifuge.ErrorPermissionDenied).
const deniedChannel = "denied"

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	h, err := NewHandler(node, configtypes.Ably{Enabled: true, KeysFile: "auth/testdata/static-app.json"}, func(r *http.Request) bool { return true })
	require.NoError(t, err)
	return h
}

func requireTimeWithinSkew(t *testing.T, ms int64) {
	t.Helper()
	now := time.Now().UnixMilli()
	require.InDelta(t, now, ms, float64((time.Minute).Milliseconds()))
}

// RSC16: GET /time returns [ms] as JSON by default.
func TestTimeJSON_RSC16(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/time")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), contentTypeJSON)

	var times []int64
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&times))
	require.Len(t, times, 1)
	requireTimeWithinSkew(t, times[0])
}

// RSC16 + RSC8c: GET /time honors msgpack via format param and Accept header.
func TestTimeMsgPack_RSC16(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	for name, do := range map[string]func() (*http.Response, error){
		"format_param": func() (*http.Response, error) {
			return http.Get(srv.URL + "/time?format=msgpack")
		},
		"accept_header": func() (*http.Response, error) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+"/time", nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Accept", contentTypeMsgPack)
			return http.DefaultClient.Do(req)
		},
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := do()
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Contains(t, resp.Header.Get("Content-Type"), contentTypeMsgPack)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var times []int64
			require.NoError(t, msgpack.Unmarshal(body, &times))
			require.Len(t, times, 1)
			requireTimeWithinSkew(t, times[0])
		})
	}
}

// Catch-all REST error contract: 404 with code 40400 and Ably error headers.
func TestNotFoundError(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nonexistent")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "40400", resp.Header.Get("X-Ably-Errorcode"))

	var envelope struct {
		Error struct {
			Message    string `json:"message"`
			Code       int    `json:"code"`
			StatusCode int    `json:"statusCode"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Equal(t, 40400, envelope.Error.Code)
	require.Equal(t, http.StatusNotFound, envelope.Error.StatusCode)
}

// The adapter enabled without a keys file is a startup error: it cannot
// authenticate anyone (RSA11), so booting in that state would be useless.
func TestNewHandlerRequiresKeysFile(t *testing.T) {
	t.Parallel()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	_, err = NewHandler(node, configtypes.Ably{Enabled: true}, nil)
	require.ErrorContains(t, err, "keys_file")

	_, err = NewHandler(node, configtypes.Ably{Enabled: true, KeysFile: "auth/testdata/does-not-exist.json"}, nil)
	require.Error(t, err)
}

// realtimeTestServer is a real centrifuge node behind the Ably handler,
// with a subscribe handler standing in for centrifugo's permission chain.
type realtimeTestServer struct {
	wsURL    string
	node     *centrifuge.Node
	srv      *httptest.Server
	lastUser func() string // centrifuge UserID of the most recently connected client
}

func newRealtimeServer(t *testing.T) *realtimeTestServer {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	// Run registers the node as the broker's event handler — without it
	// node.Publish (the adapter publish path) has no delivery pipeline.
	require.NoError(t, node.Run())

	var mu sync.Mutex
	var lastUser string
	node.OnConnect(func(client *centrifuge.Client) {
		mu.Lock()
		lastUser = client.UserID()
		mu.Unlock()
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if e.Channel == deniedChannel {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			// AllowTagsFilter mirrors centrifugo's allow_tags_filter channel
			// option (internal/client/handler.go maps chOpts.AllowTagsFilter
			// into the SubscribeReply options); without it centrifuge rejects
			// any SubscribeRequest carrying a Tf filter — the echo=false
			// attach path (RTL7f).
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{AllowTagsFilter: true},
			}, nil)
		})
	})

	h, err := NewHandler(node, configtypes.Ably{Enabled: true, KeysFile: "auth/testdata/static-app.json"}, func(r *http.Request) bool { return true })
	require.NoError(t, err)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	return &realtimeTestServer{
		wsURL: "ws" + strings.TrimPrefix(srv.URL, "http"),
		node:  node,
		srv:   srv,
		lastUser: func() string {
			mu.Lock()
			defer mu.Unlock()
			return lastUser
		},
	}
}

// defaultDialParams mirrors what an Ably SDK puts on the upgrade
// querystring (RTN2).
func defaultDialParams() url.Values {
	params := url.Values{}
	params.Set("format", "json") // RTN2a
	params.Set("echo", "true")   // RTN2b
	params.Set("v", "6")         // RTN2f
	params.Set("key", testKey)   // RTN2e
	return params
}

func dialRealtime(t *testing.T, wsURL string, params url.Values) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{}
	conn, _, _, err := dialer.Dial(wsURL+"/?"+params.Encode(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// wsMessageType returns the WS frame type a format must travel as:
// msgpack frames are binary messages, JSON frames are text messages.
func wsMessageType(format protocol.Format) int {
	if format == protocol.FormatMsgpack {
		return websocket.BinaryMessage
	}
	return websocket.TextMessage
}

// readFrameFormat reads one frame in the given wire format, asserting the
// server used the matching WS frame type.
func readFrameFormat(t *testing.T, conn *websocket.Conn, format protocol.Format) *protocol.ProtocolMessage {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	messageType, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, wsMessageType(format), messageType, "WS frame type must match the %s wire format", format)
	var m protocol.ProtocolMessage
	require.NoError(t, protocol.Unmarshal(data, format, &m))
	return &m
}

func readFrame(t *testing.T, conn *websocket.Conn) *protocol.ProtocolMessage {
	t.Helper()
	return readFrameFormat(t, conn, protocol.FormatJSON)
}

func writeFrameFormat(t *testing.T, conn *websocket.Conn, m *protocol.ProtocolMessage, format protocol.Format) {
	t.Helper()
	data, err := protocol.Marshal(m, format)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(wsMessageType(format), data))
}

func writeFrame(t *testing.T, conn *websocket.Conn, m *protocol.ProtocolMessage) {
	t.Helper()
	writeFrameFormat(t, conn, m, protocol.FormatJSON)
}

// readNonHeartbeatFrameFormat reads frames until one that is not a server
// heartbeat arrives — the 10s heartbeat ticker (RTN23a) may interleave
// with any read.
func readNonHeartbeatFrameFormat(t *testing.T, conn *websocket.Conn, format protocol.Format) *protocol.ProtocolMessage {
	t.Helper()
	for {
		m := readFrameFormat(t, conn, format)
		if m.Action != protocol.ActionHeartbeat {
			return m
		}
	}
}

func readNonHeartbeatFrame(t *testing.T, conn *websocket.Conn) *protocol.ProtocolMessage {
	t.Helper()
	return readNonHeartbeatFrameFormat(t, conn, protocol.FormatJSON)
}

// connectRealtimeFormat dials with the given wire format (RTN2a) and
// consumes the initial CONNECTED frame.
func connectRealtimeFormat(t *testing.T, ts *realtimeTestServer, format protocol.Format) *websocket.Conn {
	t.Helper()
	params := defaultDialParams()
	params.Set("format", format.String())
	conn := dialRealtime(t, ts.wsURL, params)
	connected := readFrameFormat(t, conn, format)
	require.Equal(t, protocol.ActionConnected, connected.Action)
	return conn
}

// connectRealtime dials and consumes the initial CONNECTED frame.
func connectRealtime(t *testing.T, ts *realtimeTestServer) *websocket.Conn {
	t.Helper()
	return connectRealtimeFormat(t, ts, protocol.FormatJSON)
}

// RTN6: the connection is successful once the initial CONNECTED
// ProtocolMessage is received, carrying connectionId and the
// connectionDetails constraints (TR4o, CD2).
func TestRealtimeConnect_RTN6(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	conn := dialRealtime(t, ts.wsURL, defaultDialParams())
	m := readFrame(t, conn)

	require.Equal(t, protocol.ActionConnected, m.Action)
	require.NotEmpty(t, m.ConnectionID)

	// TR4o/CD2: connectionDetails advertises the connection constraints.
	require.NotNil(t, m.ConnectionDetails)
	require.NotEmpty(t, m.ConnectionDetails.ConnectionKey)                              // CD2b
	require.Equal(t, int64(maxMessageSize), m.ConnectionDetails.MaxMessageSize)         // CD2c
	require.Equal(t, int64(maxFrameSize), m.ConnectionDetails.MaxFrameSize)             // CD2d
	require.Equal(t, int64(maxInboundRate), m.ConnectionDetails.MaxInboundRate)         // CD2e
	require.Equal(t, int64(connectionStateTTL), m.ConnectionDetails.ConnectionStateTTL) // CD2f
	require.Equal(t, int64(15000), m.ConnectionDetails.MaxIdleInterval)                 // CD2h, RTN23a

	// The centrifuge client is not anonymous: its UserID is the
	// authenticated key name (credentials set via centrifuge.SetCredentials
	// before NewClient).
	require.Eventually(t, func() bool { return ts.lastUser() == testKeyName }, 2*time.Second, 10*time.Millisecond)
}

// RTN2d/CD2a: a clientId querystring param is assumed for the connection
// and echoed in connectionDetails.clientId.
func TestRealtimeConnectClientID_RTN2d(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Set("clientId", "bob")
	conn := dialRealtime(t, ts.wsURL, params)
	m := readFrame(t, conn)

	require.Equal(t, protocol.ActionConnected, m.Action)
	require.NotNil(t, m.ConnectionDetails)
	require.Equal(t, "bob", m.ConnectionDetails.ClientID) // CD2a
	require.Eventually(t, func() bool { return ts.lastUser() == "bob" }, 2*time.Second, 10*time.Millisecond)
}

// RTN14a: an invalid API key fails the connection with an in-band ERROR
// ProtocolMessage (empty channel attribute), after which the server
// terminates the connection (RTN14g). Code 40101 (invalid credentials),
// statusCode 401.
func TestRealtimeAuthError_RTN14a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	cases := map[string]func(params url.Values){
		"bad_key":     func(params url.Values) { params.Set("key", "poc.key0:wrong-secret") },
		"unknown_key": func(params url.Values) { params.Set("key", "poc.nokey:secret") },
		"missing_key": func(params url.Values) { params.Del("key") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			params := defaultDialParams()
			mutate(params)
			conn := dialRealtime(t, ts.wsURL, params)

			m := readFrame(t, conn)
			require.Equal(t, protocol.ActionError, m.Action)
			require.Empty(t, m.Channel)
			require.NotNil(t, m.Error)
			require.Equal(t, 40101, m.Error.Code)
			require.Equal(t, http.StatusUnauthorized, m.Error.StatusCode)

			// RTN14g: the server terminates the connection after the ERROR.
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, _, err := conn.ReadMessage()
			require.Error(t, err)
		})
	}
}

// RTN2a: json and msgpack are the served wire formats; unknown formats
// are rejected before the upgrade.
func TestRealtimeFormatRejected_RTN2a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	for _, format := range []string{"xml", "protobuf"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			params := defaultDialParams()
			params.Set("format", format)
			dialer := websocket.Dialer{}
			_, resp, _, err := dialer.Dial(ts.wsURL+"/?"+params.Encode(), nil)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// RTN13a: a client HEARTBEAT expects a HEARTBEAT in response, with the id
// echoed for correlation.
func TestRealtimeHeartbeatEcho_RTN13a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionHeartbeat, ID: "hb-1"})
	m := readFrame(t, conn)
	require.Equal(t, protocol.ActionHeartbeat, m.Action)
	require.Equal(t, "hb-1", m.ID)
}

// RTL4c: ATTACH is confirmed by an ATTACHED carrying the channel.
// RTL5d: DETACH is confirmed by a DETACHED carrying the channel.
func TestRealtimeAttachDetach_RTL4c_RTL5d(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "test"})
	attached := readFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, "test", attached.Channel)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionDetach, Channel: "test"})
	detached := readFrame(t, conn)
	require.Equal(t, protocol.ActionDetached, detached.Action)
	require.Equal(t, "test", detached.Channel)
}

// RTL14: a failed ATTACH (here: subscribe permission denied) is reported
// as an ERROR ProtocolMessage carrying the channel, failing that channel
// on the client. Permission denied maps to 40160 (operation not permitted
// with provided capability), statusCode 401.
func TestRealtimeAttachDenied_RTL14(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: deniedChannel})
	m := readFrame(t, conn)
	require.Equal(t, protocol.ActionError, m.Action)
	require.Equal(t, deniedChannel, m.Channel)
	require.NotNil(t, m.Error)
	require.Equal(t, errCodeOperationNotPermitted, m.Error.Code)
	require.Equal(t, http.StatusUnauthorized, m.Error.StatusCode)
}

// RTN12a: CLOSE is confirmed by CLOSED, after which the connection is
// dropped.
func TestRealtimeClose_RTN12a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionClose})
	m := readFrame(t, conn)
	require.Equal(t, protocol.ActionClosed, m.Action)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	requireServerClosed(t, err)
}

// requireServerClosed asserts a read error came from the server actually
// closing the connection rather than our read deadline expiring — the
// regression guard for the teardown re-entrancy deadlock: a teardown
// wedged inside closeFn() (centrifuge re-enters via Transport.Close)
// never reaches conn.Close(), so the read ends in a timeout instead.
func requireServerClosed(t *testing.T, err error) {
	t.Helper()
	var nerr net.Error
	require.False(t, errors.As(err, &nerr) && nerr.Timeout(),
		"server never closed the connection: close-path deadlock")
}

// When centrifuge closes the client server-side (here: node shutdown) the
// session reports DISCONNECTED with code 80003 and drops the connection —
// the genuine handleTransportClose path, the other ordering of the
// close-path re-entrancy interaction. RTN15h3: a DISCONNECTED with a
// non-token error makes the client attempt an immediate reconnect.
func TestRealtimeServerInitiatedClose(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	go func() { _ = ts.node.Shutdown(context.Background()) }()

	m := readFrame(t, conn)
	require.Equal(t, protocol.ActionDisconnected, m.Action)
	require.NotNil(t, m.Error)
	require.Equal(t, errCodeDisconnected, m.Error.Code)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	requireServerClosed(t, err)
}

// RTN7a/RTN7b: every inbound MESSAGE is confirmed with an ACK carrying the
// exact inbound msgSerial (first publish on a connection is serial 0) and
// count 1 (one frame = one serial). No ATTACH precedes the publishes: with
// the connection CONNECTED the messages are published immediately (RTL6c1).
func TestRealtimePublishAck_RTN7(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	for serial := int64(0); serial < 2; serial++ {
		writeFrame(t, conn, &protocol.ProtocolMessage{
			Action:    protocol.ActionMessage,
			Channel:   "ack-test",
			MsgSerial: serial,
			Messages:  []*protocol.Message{{Name: "greeting", Data: "hello"}},
		})
		ack := readNonHeartbeatFrame(t, conn)
		require.Equal(t, protocol.ActionAck, ack.Action)
		require.Equal(t, serial, ack.MsgSerial) // RTN7b: exact round trip
		require.Equal(t, 1, ack.Count)
		require.Nil(t, ack.Error)
	}
}

// RTL6/RTL7: a transient publish (no prior ATTACH on the publisher,
// RTL6c1) fans out to an attached connection as a MESSAGE frame whose
// Message carries the full server-built envelope: generated id, the
// publisher's clientId (RTL6g1b) and connectionId (TM2c), and a server
// timestamp (TM2f). The publisher gets its ACK (RTN7a).
func TestRealtimePublishFanOut_RTL6_RTL7(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// A: subscriber, attached to the channel.
	connA := connectRealtime(t, ts)
	writeFrame(t, connA, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "fanout-test"})
	attached := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionAttached, attached.Action)

	// B: identified publisher, never attaches.
	paramsB := defaultDialParams()
	paramsB.Set("clientId", "bob")
	connB := dialRealtime(t, ts.wsURL, paramsB)
	connectedB := readFrame(t, connB)
	require.Equal(t, protocol.ActionConnected, connectedB.Action)
	publisherConnID := connectedB.ConnectionID

	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "fanout-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello"}},
	})

	ack := readNonHeartbeatFrame(t, connB)
	require.Equal(t, protocol.ActionAck, ack.Action)
	require.Equal(t, int64(0), ack.MsgSerial)
	require.Equal(t, 1, ack.Count)

	delivered := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionMessage, delivered.Action)
	require.Equal(t, "fanout-test", delivered.Channel)
	requireTimeWithinSkew(t, delivered.Timestamp)
	require.Len(t, delivered.Messages, 1)
	msg := delivered.Messages[0]
	require.Equal(t, "greeting", msg.Name)
	require.Equal(t, "hello", msg.Data)
	require.Equal(t, publisherConnID+":0:0", msg.ID) // generated <connectionId>:<msgSerial>:<idx>
	require.Equal(t, "bob", msg.ClientID)            // RTL6g1b
	require.Equal(t, publisherConnID, msg.ConnectionID)
	requireTimeWithinSkew(t, msg.Timestamp) // TM2f
}

// RTC1a: echoMessages is on by default, so an attached publisher receives
// its own message back via its subscription, alongside the ACK. (RTL7f —
// echo=false suppression — is TestRealtimeNoEcho_RTL7f.) The two frames
// originate from different goroutines, so order is not asserted.
func TestRealtimePublishEcho_RTC1a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "echo-test"})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "echo-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello"}},
	})

	var ack, delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			ack = m
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotNil(t, ack)
	require.Equal(t, int64(0), ack.MsgSerial)
	require.Equal(t, 1, ack.Count)
	require.NotNil(t, delivered)
	require.Equal(t, "echo-test", delivered.Channel)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "hello", delivered.Messages[0].Data)
}

// RTL7f: a connection with echo=false (RTN2b; RTC1a — echo defaults to on)
// never receives its own messages back, while other attached connections
// do, and messages from others still reach it (the filter suppresses own
// messages only). Suppression is asserted with a marker rather than a
// sleep: publications on one channel reach a subscriber in publish order,
// so if A's own message had been delivered it would precede B's marker on
// A's connection.
func TestRealtimeNoEcho_RTL7f(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// A: echo=false, attached.
	paramsA := defaultDialParams()
	paramsA.Set("echo", "false")
	connA := dialRealtime(t, ts.wsURL, paramsA)
	connectedA := readFrame(t, connA)
	require.Equal(t, protocol.ActionConnected, connectedA.Action)
	writeFrame(t, connA, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "noecho-test"})
	attachedA := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionAttached, attachedA.Action)

	// B: echo default (on), attached.
	connB := connectRealtime(t, ts)
	writeFrame(t, connB, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "noecho-test"})
	attachedB := readNonHeartbeatFrame(t, connB)
	require.Equal(t, protocol.ActionAttached, attachedB.Action)

	// A publishes: A gets its ACK (RTN7a), B receives the message, A must
	// not — asserted via the marker below.
	writeFrame(t, connA, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "noecho-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "from-a", Data: "a-1"}},
	})
	ackA := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionAck, ackA.Action)
	require.Equal(t, int64(0), ackA.MsgSerial)

	deliveredB := readNonHeartbeatFrame(t, connB)
	require.Equal(t, protocol.ActionMessage, deliveredB.Action)
	require.Len(t, deliveredB.Messages, 1)
	require.Equal(t, "from-a", deliveredB.Messages[0].Name)

	// Marker: B publishes next. B (echo on) gets ACK plus its own echo, in
	// either order (different goroutines).
	writeFrame(t, connB, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "noecho-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "from-b", Data: "b-1"}},
	})
	var ackB, echoB *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, connB); m.Action {
		case protocol.ActionAck:
			ackB = m
		case protocol.ActionMessage:
			echoB = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotNil(t, ackB)
	require.NotNil(t, echoB)
	require.Len(t, echoB.Messages, 1)
	require.Equal(t, "from-b", echoB.Messages[0].Name)

	// A's FIRST received MESSAGE is B's marker: A's own "from-a" — published
	// before it — was never delivered, and messages from others get through.
	deliveredA := readNonHeartbeatFrame(t, connA)
	require.Equal(t, protocol.ActionMessage, deliveredA.Action)
	require.Len(t, deliveredA.Messages, 1)
	require.Equal(t, "from-b", deliveredA.Messages[0].Name)
}

// CD2c/TO3l8: a MESSAGE frame whose summed message payload size exceeds the
// advertised maxMessageSize is rejected as a whole — NACK with 40009
// (maximum message length exceeded), statusCode 400, the realtime
// counterpart of REST's RSL1i — publishing nothing, and the connection
// survives (application-level reject; contrast the read-limit drop below).
func TestRealtimePublishMaxMessageSize(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "maxsize-test"})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)

	// Two messages, each under the limit, summing over it: the limit binds
	// on the frame's summed payload size (TO3l8), not per message.
	big := strings.Repeat("x", 40_000)
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "maxsize-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "big-0", Data: big}, {Name: "big-1", Data: big}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, int64(0), nack.MsgSerial)
	require.Equal(t, 1, nack.Count)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeMaxMessageLength, nack.Error.Code)
	require.Equal(t, http.StatusBadRequest, nack.Error.StatusCode)

	// The connection stays usable: a follow-up publish ACKs, and its echo is
	// the FIRST delivered MESSAGE — nothing from the rejected frame was
	// published (delivery is in publish order).
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "maxsize-test",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "small", Data: "ok"}},
	})
	var ack, delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			ack = m
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotNil(t, ack)
	require.Equal(t, int64(1), ack.MsgSerial)
	require.NotNil(t, delivered)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "small", delivered.Messages[0].Name)
}

// CD2d: a frame beyond the advertised maxFrameSize trips the WS read limit
// set in serveRealtime — the protocol-level guard. Unlike the
// application-level maxMessageSize NACK above, this kills the connection.
func TestRealtimeReadLimitExceeded_CD2d(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	frame := &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "readlimit-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "huge", Data: strings.Repeat("x", maxFrameSize+1024)}},
	}
	data, err := protocol.Marshal(frame, protocol.FormatJSON)
	require.NoError(t, err)
	// The write itself may error mid-frame: the server aborts as soon as the
	// frame header announces an over-limit payload, possibly before the
	// client drains its send buffer.
	_ = conn.WriteMessage(websocket.TextMessage, data)

	// The connection dies (heartbeats may interleave before the close
	// arrives). The read must end in a server-side close, not a timeout.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			requireServerClosed(t, err)
			return
		}
	}
}

// RTL6g: an identified connection publishing a Message with an
// incompatible explicit clientId is rejected by the service with a NACK
// carrying 40012 (invalid client id) — the server-side reject expected by
// RTL6g4's test — and the connection remains usable for further publishes
// (the rejected frame still consumed its msgSerial).
func TestRealtimePublishClientIDMismatch_RTL6g(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Set("clientId", "bob")
	conn := dialRealtime(t, ts.wsURL, params)
	connected := readFrame(t, conn)
	require.Equal(t, protocol.ActionConnected, connected.Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "clientid-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello", ClientID: "alice"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, int64(0), nack.MsgSerial)
	require.Equal(t, 1, nack.Count)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeInvalidClientID, nack.Error.Code)
	require.Equal(t, http.StatusBadRequest, nack.Error.StatusCode)

	// Matching explicit clientId is accepted (RTL6g2); serial advanced.
	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "clientid-test",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello", ClientID: "bob"}},
	})
	ack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAck, ack.Action)
	require.Equal(t, int64(1), ack.MsgSerial)
	require.Equal(t, 1, ack.Count)
}

// RTL6g4 (first half): an UNidentified connection may publish a Message
// carrying any explicit clientId — the mismatch reject only applies to
// identified connections — and the explicit clientId survives to
// subscribers verbatim.
func TestRealtimePublishExplicitClientIDUnidentified_RTL6g4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts) // no clientId param: unidentified

	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "explicit-test"})
	attached := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionAttached, attached.Action)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "explicit-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello", ClientID: "carol"}},
	})

	var ack, delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrame(t, conn); m.Action {
		case protocol.ActionAck:
			ack = m
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotNil(t, ack)
	require.NotNil(t, delivered)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "carol", delivered.Messages[0].ClientID)
}

// A MESSAGE frame without a channel attribute cannot be published: NACK
// with 40000 (bad request), consuming the msgSerial (RTN7a).
func TestRealtimePublishEmptyChannel(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "greeting", Data: "hello"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.Equal(t, int64(0), nack.MsgSerial)
	require.Equal(t, 1, nack.Count)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeBadRequest, nack.Error.Code)
	require.Equal(t, http.StatusBadRequest, nack.Error.StatusCode)
}

// RSA7c: the literal '*' clientId value is reserved (the wildcard
// identity) and cannot be assumed by a connection. Code 40012 (invalid
// client id).
func TestRealtimeWildcardClientIDRejected_RSA7c(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	params := defaultDialParams()
	params.Set("clientId", "*")
	conn := dialRealtime(t, ts.wsURL, params)

	m := readFrame(t, conn)
	require.Equal(t, protocol.ActionError, m.Action)
	require.NotNil(t, m.Error)
	require.Equal(t, errCodeInvalidClientID, m.Error.Code)
	require.Equal(t, http.StatusBadRequest, m.Error.StatusCode)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
}

// readRawFrame reads one WS frame and returns both the raw JSON bytes and
// the decoded form — for asserting wire shape (field presence), not just
// decoded values.
func readRawFrame(t *testing.T, conn *websocket.Conn) (map[string]json.RawMessage, *protocol.ProtocolMessage) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, data, err := conn.ReadMessage()
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	var m protocol.ProtocolMessage
	require.NoError(t, protocol.Unmarshal(data, protocol.FormatJSON, &m))
	return fields, &m
}

// TR4j/RTN7b: ACK frames must carry msgSerial and count explicitly even
// for serial 0 — ably-js correlates pending publishes by the literal
// field and a missing msgSerial never settles the publish (found via the
// publish_no_attach probe; regression guard on the raw wire shape).
func TestRealtimeAckExplicitMsgSerial_TR4j(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "serial-wire-test",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "n", Data: "d"}},
	})
	for {
		fields, m := readRawFrame(t, conn)
		if m.Action == protocol.ActionHeartbeat {
			continue
		}
		require.Equal(t, protocol.ActionAck, m.Action)
		require.Contains(t, fields, "msgSerial", "ACK must carry msgSerial even when 0")
		require.Contains(t, fields, "count")
		require.JSONEq(t, "0", string(fields["msgSerial"]))
		require.JSONEq(t, "1", string(fields["count"]))
		break
	}
}

// RTL4d territory, pinned by ably-js channelattachempty/channelattachinvalid:
// an empty or ':'-prefixed channel name fails the CHANNEL with 40010 — the
// ERROR frame carries the channel attribute explicitly (even "" — ably-js
// routes channel-less ERRORs to the connection) — and the connection
// survives.
func TestRealtimeAttachInvalidChannelName_RTL4d(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	for _, name := range []string{"", ":hell"} {
		writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: name})
		for {
			fields, m := readRawFrame(t, conn)
			if m.Action == protocol.ActionHeartbeat {
				continue
			}
			require.Equal(t, protocol.ActionError, m.Action)
			require.Contains(t, fields, "channel", "channel-scoped ERROR must carry the channel attribute (name %q)", name)
			require.JSONEq(t, strconv.Quote(name), string(fields["channel"]))
			require.NotNil(t, m.Error)
			require.Equal(t, errCodeInvalidChannelName, m.Error.Code)
			break
		}
	}

	// The connection survives: a heartbeat still echoes (RTN13a).
	writeFrame(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionHeartbeat, ID: "alive"})
	for {
		m := readFrame(t, conn)
		if m.Action == protocol.ActionHeartbeat && m.ID == "alive" {
			return
		}
	}
}

// Publishing to an invalid channel name NACKs with 40010 — same
// validation as attach, pinned by ably-js channelattach_publish_invalid.
func TestRealtimePublishInvalidChannelName(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtime(t, ts)

	writeFrame(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   ":hell",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "n", Data: "d"}},
	})
	nack := readNonHeartbeatFrame(t, conn)
	require.Equal(t, protocol.ActionNack, nack.Action)
	require.NotNil(t, nack.Error)
	require.Equal(t, errCodeInvalidChannelName, nack.Error.Code)
}

// RTN2a end-to-end in the msgpack wire format: CONNECTED, ATTACH/ATTACHED,
// publish→ACK (explicit msgSerial, TR4j), fan-out — every frame a WS
// binary message carrying msgpack.
func TestRealtimeMsgpackSession_RTN2a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	conn := connectRealtimeFormat(t, ts, protocol.FormatMsgpack)

	writeFrameFormat(t, conn, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "mp"}, protocol.FormatMsgpack)
	attached := readNonHeartbeatFrameFormat(t, conn, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionAttached, attached.Action)
	require.Equal(t, "mp", attached.Channel)

	writeFrameFormat(t, conn, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "mp",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "n", Data: "plain string"}},
	}, protocol.FormatMsgpack)

	var ack, delivered *protocol.ProtocolMessage
	for range 2 {
		switch m := readNonHeartbeatFrameFormat(t, conn, protocol.FormatMsgpack); m.Action {
		case protocol.ActionAck:
			ack = m
		case protocol.ActionMessage:
			delivered = m
		default:
			t.Fatalf("unexpected frame action %d", m.Action)
		}
	}
	require.NotNil(t, ack)
	require.Equal(t, int64(0), ack.MsgSerial)
	require.Equal(t, 1, ack.Count)
	require.NotNil(t, delivered)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "plain string", delivered.Messages[0].Data)
	require.Empty(t, delivered.Messages[0].Encoding)
}

// RSL4c1/RSL4d1 cross-format binary data, msgpack→json: a binary payload
// published over msgpack (raw msgpack bin) is delivered to a JSON session
// as a Base64 string with "base64" appended to the encoding chain.
func TestRealtimeBinaryDataMsgpackToJSON_RSL4d1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	jsonSub := connectRealtime(t, ts)
	writeFrame(t, jsonSub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "bin-mj"})
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrame(t, jsonSub).Action)

	mpPub := connectRealtimeFormat(t, ts, protocol.FormatMsgpack)
	raw := []byte{0x00, 0x01, 0xfe, 0xff, 0x10}
	writeFrameFormat(t, mpPub, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "bin-mj",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "blob", Data: raw}},
	}, protocol.FormatMsgpack)

	delivered := readNonHeartbeatFrame(t, jsonSub)
	require.Equal(t, protocol.ActionMessage, delivered.Action)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "base64", delivered.Messages[0].Encoding) // RSL4d1
	str, ok := delivered.Messages[0].Data.(string)
	require.True(t, ok, "JSON delivery carries Base64 string, got %T", delivered.Messages[0].Data)
	decoded, err := base64.StdEncoding.DecodeString(str)
	require.NoError(t, err)
	require.Equal(t, raw, decoded)
}

// RSL4c1 cross-format binary data, json→msgpack: a Base64+"base64"
// payload published over JSON is delivered to a msgpack session as the
// raw msgpack binary type with the transport "base64" segment popped.
// A non-transport remainder of the chain survives verbatim (RSL6a —
// client-side segments are never touched).
func TestRealtimeBinaryDataJSONToMsgpack_RSL4c1(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	mpSub := connectRealtimeFormat(t, ts, protocol.FormatMsgpack)
	writeFrameFormat(t, mpSub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "bin-jm"}, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrameFormat(t, mpSub, protocol.FormatMsgpack).Action)

	jsonPub := connectRealtime(t, ts)
	raw := []byte("cipher-or-binary-bytes")
	b64 := base64.StdEncoding.EncodeToString(raw)
	writeFrame(t, jsonPub, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "bin-jm",
		MsgSerial: 0,
		Messages: []*protocol.Message{
			{Name: "plain-blob", Data: b64, Encoding: "base64"},
			{Name: "ciphered", Data: b64, Encoding: "utf-8/cipher+aes-128-cbc/base64"},
		},
	})

	// Each Message in a frame is one centrifuge Publication (M1.2 publish
	// semantics; atomic batch delivery is M6/M8), so the two messages
	// arrive as two MESSAGE frames in publish order.
	first := readNonHeartbeatFrameFormat(t, mpSub, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionMessage, first.Action)
	require.Len(t, first.Messages, 1)
	// Transport "base64" popped, data restored to raw bytes.
	require.Empty(t, first.Messages[0].Encoding)
	require.Equal(t, raw, first.Messages[0].Data)

	second := readNonHeartbeatFrameFormat(t, mpSub, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionMessage, second.Action)
	require.Len(t, second.Messages, 1)
	// Client-side chain remainder survives verbatim after the pop.
	require.Equal(t, "utf-8/cipher+aes-128-cbc", second.Messages[0].Encoding)
	require.Equal(t, raw, second.Messages[0].Data)
}

// Encoding chains with no trailing transport "base64" pass through
// completely untouched to every session format (RSL6a: client-side
// segments are the SDK's to decode; crypto depends on the chain
// surviving verbatim).
func TestRealtimeEncodingChainPassthrough_RSL6a(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	mpSub := connectRealtimeFormat(t, ts, protocol.FormatMsgpack)
	writeFrameFormat(t, mpSub, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "chain"}, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionAttached, readNonHeartbeatFrameFormat(t, mpSub, protocol.FormatMsgpack).Action)

	jsonPub := connectRealtime(t, ts)
	writeFrame(t, jsonPub, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "chain",
		MsgSerial: 0,
		Messages:  []*protocol.Message{{Name: "j", Data: `{"a":1}`, Encoding: "json"}},
	})

	delivered := readNonHeartbeatFrameFormat(t, mpSub, protocol.FormatMsgpack)
	require.Equal(t, protocol.ActionMessage, delivered.Action)
	require.Len(t, delivered.Messages, 1)
	require.Equal(t, "json", delivered.Messages[0].Encoding)
	require.Equal(t, `{"a":1}`, delivered.Messages[0].Data)
}

// mintSessionJWT signs an Ably-JWT against a testdata key for token-auth
// tests (kid poc.key1).
func mintSessionJWT(t *testing.T, clientID string, expires time.Time) string {
	t.Helper()
	signer, err := jwt.NewSignerHS(jwt.HS256, paddedTestSecret())
	require.NoError(t, err)
	claims := map[string]any{"exp": expires.Unix()}
	if clientID != "" {
		claims["x-ably-clientId"] = clientID
	}
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	token, err := jwt.NewBuilder(signer, jwt.WithKeyID("poc.key1")).Build(json.RawMessage(payload))
	require.NoError(t, err)
	return token.String()
}

// paddedTestSecret zero-pads poc.key1's secret to 32 bytes, matching the
// verifier's Ably-JWT HMAC key derivation.
func paddedTestSecret() []byte {
	secret := []byte("secret_key1_0123456789abcdef")
	padded := make([]byte, 32)
	copy(padded, secret)
	return padded
}

// Token auth on the realtime surface: access_token connects, the
// token-bound clientId becomes the connection identity (RSA7a), expiry
// maps to 40142, a mismatched clientId param to 40102 (RSA15a), and a
// wildcard token advertises "*" in connectionDetails (RSA15b).
func TestRealtimeTokenAuth(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	dialToken := func(t *testing.T, token string, extra func(url.Values)) *websocket.Conn {
		params := url.Values{}
		params.Set("format", "json")
		params.Set("v", "6")
		params.Set("access_token", token)
		if extra != nil {
			extra(params)
		}
		return dialRealtime(t, ts.wsURL, params)
	}

	t.Run("token-bound identity connects", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "token-bob", time.Now().Add(time.Hour)), nil)
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionConnected, m.Action)
		require.Equal(t, "token-bob", m.ConnectionDetails.ClientID) // CD2a
		require.Eventually(t, func() bool { return ts.lastUser() == "token-bob" }, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("RSA4b territory: expired token 40142", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "x", time.Now().Add(-time.Minute)), nil)
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionError, m.Action)
		require.Equal(t, 40142, m.Error.Code)
		require.Equal(t, http.StatusUnauthorized, m.Error.StatusCode)
	})

	t.Run("RSA15a: clientId param incompatible with token 40102", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "token-bob", time.Now().Add(time.Hour)), func(p url.Values) {
			p.Set("clientId", "alice")
		})
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionError, m.Action)
		require.Equal(t, errCodeIncompatibleCredentials, m.Error.Code)
	})

	t.Run("RSA15a: matching clientId param accepted", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "token-bob", time.Now().Add(time.Hour)), func(p url.Values) {
			p.Set("clientId", "token-bob")
		})
		require.Equal(t, protocol.ActionConnected, readFrame(t, conn).Action)
	})

	t.Run("RSA15b: wildcard token advertises *", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "*", time.Now().Add(time.Hour)), nil)
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionConnected, m.Action)
		require.Equal(t, "*", m.ConnectionDetails.ClientID)
	})

	t.Run("RSA7b4: wildcard token may assume a clientId", func(t *testing.T) {
		conn := dialToken(t, mintSessionJWT(t, "*", time.Now().Add(time.Hour)), func(p url.Values) {
			p.Set("clientId", "carol")
		})
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionConnected, m.Action)
		require.Equal(t, "carol", m.ConnectionDetails.ClientID)
	})

	t.Run("invalid token 40101", func(t *testing.T) {
		conn := dialToken(t, "garbage-token", nil)
		m := readFrame(t, conn)
		require.Equal(t, protocol.ActionError, m.Action)
		require.Equal(t, errCodeInvalidCredentials, m.Error.Code)
	})
}

// Token auth on the REST surface: Authorization Bearer authenticates, the
// token clientId is the publisher identity, expiry maps to 40142.
func TestRESTTokenAuth(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	bearer := func(token string) map[string]string {
		return map[string]string{"Content-Type": contentTypeJSON, "Authorization": "Bearer " + token}
	}
	post := func(t *testing.T, token string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, ts.srv.URL+"/channels/persisted:rest-token/messages",
			strings.NewReader(`{"name":"e"}`))
		require.NoError(t, err)
		for k, v := range bearer(token) {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	resp := post(t, mintSessionJWT(t, "rest-token-bob", time.Now().Add(time.Hour)))
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = restRequest(t, ts, http.MethodGet, "/channels/persisted:rest-token/messages", nil, nil)
	msgs := decodeMessagesBody(t, resp)
	require.Len(t, msgs, 1)
	require.Equal(t, "rest-token-bob", msgs[0].ClientID) // token identity stamped

	resp = post(t, mintSessionJWT(t, "x", time.Now().Add(-time.Minute)))
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "40142", resp.Header.Get("X-Ably-Errorcode"))
}

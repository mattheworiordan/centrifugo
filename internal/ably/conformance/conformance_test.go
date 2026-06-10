// Package conformance is the permanent ably-go smoke for the Ably protocol
// adapter: unmodified Ably SDK clients against a live adapter binary.
//
// The tests need a running server (started by `make ably-conformance`) and
// skip when ABLY_CONFORMANCE_URL is unset, so plain `go test` stays green.
// Test names mirror the ably-js tests they correspond to (the dual-track
// strategy: native tests pin server behavior, SDK tests pin compatibility).
package conformance

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"
	"github.com/stretchr/testify/require"
)

// Key from the static fixture the dev profile loads
// (.working/ably-centrifugo-poc/harness/static-app.json).
const testKey = "poc.key0:secret_key0_0123456789abcdef"

// serverAddr returns the adapter host/port from ABLY_CONFORMANCE_URL
// (host:port, optionally with a scheme), skipping the test when unset.
func serverAddr(t *testing.T) (string, int) {
	t.Helper()
	raw := os.Getenv("ABLY_CONFORMANCE_URL")
	if raw == "" {
		t.Skip("ABLY_CONFORMANCE_URL not set; run via `make ably-conformance`")
	}
	hostPort := raw
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		require.NoError(t, err, "invalid ABLY_CONFORMANCE_URL")
		hostPort = u.Host
	}
	host, portStr, err := net.SplitHostPort(hostPort)
	require.NoError(t, err, "ABLY_CONFORMANCE_URL must carry host:port")
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, port
}

func newRealtime(t *testing.T, opts ...ably.ClientOption) *ably.Realtime {
	t.Helper()
	host, port := serverAddr(t)
	client, err := ably.NewRealtime(append([]ably.ClientOption{
		ably.WithKey(testKey),
		// A hostname endpoint sets both the REST and realtime host (REC1b2);
		// the deprecated WithRealtimeHost/WithRESTHost pair is avoided.
		ably.WithEndpoint(host),
		ably.WithPort(port),
		ably.WithTLS(false),
		// ably-go rejects Basic key auth over ws:// without this opt-in.
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseBinaryProtocol(false), // JSON wire format (M1 scope; msgpack is M2)
		ably.WithAutoConnect(false),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	connect(t, client)
	return client
}

// forBothProtocols runs the test body once per wire format — every
// conformance mirror must pass in both encodings (the M2 acceptance bar).
// The body receives the protocol name (for channel-name isolation between
// variants) and the client option selecting it; options passed to
// newRealtime after the defaults override them, so the JSON default in
// newRealtime is replaced per-variant.
func forBothProtocols(t *testing.T, body func(t *testing.T, proto string, protoOpt ably.ClientOption)) {
	t.Helper()
	for _, variant := range []struct {
		name   string
		binary bool
	}{{"json", false}, {"msgpack", true}} {
		t.Run(variant.name, func(t *testing.T) {
			body(t, variant.name, ably.WithUseBinaryProtocol(variant.binary))
		})
	}
}

func connect(t *testing.T, client *ably.Realtime) {
	t.Helper()
	connected := make(chan struct{})
	failed := make(chan error, 1)
	offConnected := client.Connection.Once(ably.ConnectionEventConnected, func(ably.ConnectionStateChange) {
		close(connected)
	})
	defer offConnected()
	offFailed := client.Connection.Once(ably.ConnectionEventFailed, func(change ably.ConnectionStateChange) {
		failed <- fmt.Errorf("connection failed: %v", change.Reason)
	})
	defer offFailed()
	client.Connect()
	select {
	case <-connected:
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for CONNECTED (state=%s reason=%v)",
			client.Connection.State(), client.Connection.ErrorReason())
	}
}

// subscribeAll attaches the client to the channel and buffers received
// messages.
func subscribeAll(t *testing.T, ctx context.Context, client *ably.Realtime, channel string) <-chan *ably.Message {
	t.Helper()
	received := make(chan *ably.Message, 8)
	unsubscribe, err := client.Channels.Get(channel).SubscribeAll(ctx, func(msg *ably.Message) {
		received <- msg
	})
	require.NoError(t, err)
	t.Cleanup(unsubscribe)
	return received
}

func waitMessage(t *testing.T, who string, ch <-chan *ably.Message) *ably.Message {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timeout waiting for message", who)
		return nil
	}
}

// TestPublishSingle_mirrors_publishonce_RTL6 mirrors ably-js
// realtime/message "publishonce": a publisher publishes one message and an
// attached subscriber receives it. The publisher never attaches, so the
// publish is transient (RTL6c1), and the delivered message must carry the
// server-built envelope (id, connectionId per TM2c, timestamp per TM2f).
func TestPublishSingle_mirrors_publishonce_RTL6(t *testing.T) {
	forBothProtocols(t, func(t *testing.T, proto string, protoOpt ably.ClientOption) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		subscriber := newRealtime(t, protoOpt)
		publisher := newRealtime(t, protoOpt)
		channel := "conformance-publishonce-" + proto
		received := subscribeAll(t, ctx, subscriber, channel)

		require.NoError(t, publisher.Channels.Get(channel).
			Publish(ctx, "greeting", "hello-conformance")) // returns after ACK (RTN7a)

		msg := waitMessage(t, "subscriber (fan-out)", received)
		require.Equal(t, "greeting", msg.Name)
		require.Equal(t, "hello-conformance", msg.Data)
		require.NotEmpty(t, msg.ID)                                      // TM2a
		require.Equal(t, publisher.Connection.ID(), msg.ConnectionID)    // TM2c
		require.InDelta(t, time.Now().UnixMilli(), msg.Timestamp, 60000) // TM2f
	})
}

// TestPublishEcho_mirrors_publishEcho_RTC1a mirrors the echoMessages=true
// half of ably-js realtime/message "publishEcho": with echo on (the
// default) an attached publisher receives its own message back via its
// subscription. The echoMessages=false half is
// TestPublishNoEcho_mirrors_publishEcho_RTL7f.
func TestPublishEcho_mirrors_publishEcho_RTC1a(t *testing.T) {
	forBothProtocols(t, func(t *testing.T, proto string, protoOpt ably.ClientOption) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		publisher := newRealtime(t, protoOpt)
		channel := "conformance-publishecho-" + proto
		received := subscribeAll(t, ctx, publisher, channel)

		require.NoError(t, publisher.Channels.Get(channel).
			Publish(ctx, "greeting", "hello-echo"))

		msg := waitMessage(t, "publisher (echo)", received)
		require.Equal(t, "greeting", msg.Name)
		require.Equal(t, "hello-echo", msg.Data)
		require.Equal(t, publisher.Connection.ID(), msg.ConnectionID)
	})
}

// TestPublishNoEcho_mirrors_publishEcho_RTL7f mirrors the
// echoMessages=false half of ably-js realtime/message "publishEcho": with
// echo disabled in the client options (ably-go WithEchoMessages(false),
// which puts echo=false on the upgrade querystring per RTN2b) the publisher
// does not receive its own message back, while a separate subscriber does
// (RTL7f; RTC1a is the echo-on default). Suppression is asserted with a
// marker rather than a sleep: messages on one channel reach a subscriber in
// publish order, so the suppressed echo, were it delivered, would arrive at
// the publisher before the marker.
func TestPublishNoEcho_mirrors_publishEcho_RTL7f(t *testing.T) {
	forBothProtocols(t, func(t *testing.T, proto string, protoOpt ably.ClientOption) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		publisher := newRealtime(t, protoOpt, ably.WithEchoMessages(false))
		subscriber := newRealtime(t, protoOpt)
		channel := "conformance-publishnoecho-" + proto
		publisherReceived := subscribeAll(t, ctx, publisher, channel)
		subscriberReceived := subscribeAll(t, ctx, subscriber, channel)

		require.NoError(t, publisher.Channels.Get(channel).
			Publish(ctx, "greeting", "hello-noecho"))

		msg := waitMessage(t, "subscriber (fan-out)", subscriberReceived)
		require.Equal(t, "greeting", msg.Name)
		require.Equal(t, "hello-noecho", msg.Data)

		// Marker: the subscriber publishes a follow-up. The publisher's FIRST
		// received message must be the marker — its own message was never
		// echoed.
		require.NoError(t, subscriber.Channels.Get(channel).
			Publish(ctx, "marker", "from-subscriber"))

		marker := waitMessage(t, "publisher (marker)", publisherReceived)
		require.Equal(t, "marker", marker.Name)
		require.Equal(t, "from-subscriber", marker.Data)
	})
}

// TestPublishImplicitClientID_mirrors_implicit_client_id_0_RTL6g1 mirrors
// ably-js realtime/message "implicit_client_id_0": an identified client
// publishes without an explicit Message.clientId and the delivered message
// carries the connection's clientId, assigned by the server (RTL6g1b).
func TestPublishImplicitClientID_mirrors_implicit_client_id_0_RTL6g1(t *testing.T) {
	forBothProtocols(t, func(t *testing.T, proto string, protoOpt ably.ClientOption) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		subscriber := newRealtime(t, protoOpt)
		publisher := newRealtime(t, protoOpt, ably.WithClientID("conformance-bob"))
		channel := "conformance-implicit-clientid-" + proto
		received := subscribeAll(t, ctx, subscriber, channel)

		require.NoError(t, publisher.Channels.Get(channel).
			Publish(ctx, "greeting", "hello-implicit"))

		msg := waitMessage(t, "subscriber", received)
		require.Equal(t, "conformance-bob", msg.ClientID) // RTL6g1b
	})
}

// TestPublishBinaryData_crossProtocol_RSL4c1 verifies the adapter's
// binary-payload normalization through real SDK clients: a []byte payload
// published over the msgpack protocol (raw msgpack bin on the wire)
// arrives intact at BOTH a msgpack subscriber (binary restored, RSL4c1)
// and a JSON subscriber (Base64 + "base64" encoding segment on the wire,
// RSL4d1, decoded client-side by the SDK per RSL6a).
func TestPublishBinaryData_crossProtocol_RSL4c1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jsonSub := newRealtime(t, ably.WithUseBinaryProtocol(false))
	mpSub := newRealtime(t, ably.WithUseBinaryProtocol(true))
	publisher := newRealtime(t, ably.WithUseBinaryProtocol(true))
	const channel = "conformance-binary-data"
	jsonReceived := subscribeAll(t, ctx, jsonSub, channel)
	mpReceived := subscribeAll(t, ctx, mpSub, channel)

	raw := []byte{0x00, 0x01, 0xfe, 0xff, 0x42}
	require.NoError(t, publisher.Channels.Get(channel).Publish(ctx, "blob", raw))

	// ably-go v1.4.1 quirk: on realtime receive it only runs the
	// encoding-chain decode when delta encoding is enabled on the channel
	// (realtime_channel.go:1142 isDeltaEncodingEnabled guard), so the JSON
	// subscriber surfaces the canonical wire form verbatim — which is
	// exactly what this test pins: Base64 string + "base64" encoding
	// (RSL4d1), as the real Ably service would send.
	jsonMsg := waitMessage(t, "json subscriber", jsonReceived)
	require.Equal(t, "base64", jsonMsg.Encoding)
	require.Equal(t, base64.StdEncoding.EncodeToString(raw), jsonMsg.Data)

	// The msgpack subscriber needs no client-side decode: the adapter pops
	// the transport "base64" segment and restores raw msgpack binary
	// (RSL4c1). ably-go's codec surfaces msgpack bin as a Go string
	// (RawToString=true, ablyutil/msgpack.go:16), so the assertion
	// compares content, not type.
	mpMsg := waitMessage(t, "msgpack subscriber", mpReceived)
	require.Empty(t, mpMsg.Encoding)
	require.Equal(t, string(raw), mpMsg.Data)
}

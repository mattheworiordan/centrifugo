package ably

// D3: a cold channel's serial generator is seeded from the broker
// high-water so the first post-restart (or post-A4b-eviction) serial is
// strictly greater than any prior one — continuity never regresses. The
// end-to-end cross-process restart proof is D7; here we pin the mechanism.

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// A cold-channel mint seeded from a known high-water produces a serial
// strictly greater than it. The high-water is placed in the FUTURE so the
// wall clock has not passed it — forcing the seeded-counter path (Restore +
// counter bump), the case a plain wall-clock mint would get wrong.
func TestSerialMintSeedsColdChannel_D3(t *testing.T) {
	t.Parallel()
	futureTs := time.Now().Add(time.Hour).UnixMilli()
	high := fmt.Sprintf("%014d-%03d@%s", futureTs, 5, "seedseries0")
	ts, ctr, err := serial.ParseChannelSerial(high)
	require.NoError(t, err)

	m := newSerialMint(func(string) (int64, int, bool) { return ts, ctr, true })
	got := m.Mint("cold-channel")
	require.Greater(t, got, high,
		"a cold channel seeds from the high-water; the next serial must be strictly greater")
}

// A nil/empty seed (no prior history) mints fresh without error.
func TestSerialMintNoHistoryFresh_D3(t *testing.T) {
	t.Parallel()
	m := newSerialMint(func(string) (int64, int, bool) { return 0, 0, false })
	require.NotEmpty(t, m.Mint("fresh-channel"))
}

// Integration: publish a message (a serial lands in broker history), then a
// FRESH serialMint — simulating a new process — reads the same broker's
// high-water via its seed and mints a serial strictly greater than the
// prior one. Engine-agnostic mechanism check (the cross-process Redis proof
// is D7).
func TestSerialRecoversFromBrokerHistory_D3(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	const channel = "persisted:d3recover"

	resp := restRequest(t, ts, http.MethodPost, "/channels/"+channel+"/messages",
		[]byte(`{"name":"ev","data":"x"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	hist := restRequest(t, ts, http.MethodGet, "/channels/"+channel+"/messages", nil, nil)
	items := decodeMessagesBody(t, hist)
	require.Len(t, items, 1)
	priorCS, _, err := serial.ParseMessageSerial(items[0].Serial) // Message.Serial = <channelSerial>:000
	require.NoError(t, err)

	fresh := newSerialMint(func(ch string) (int64, int, bool) {
		res, e := ts.node.History(brokerChannel(ch), centrifuge.WithLimit(1), centrifuge.WithReverse(true))
		if e != nil || len(res.Publications) == 0 {
			return 0, 0, false
		}
		t2, c2, e := serial.ParseChannelSerial(res.Publications[0].Tags[pubTagSerial])
		if e != nil {
			return 0, 0, false
		}
		return t2, c2, true
	})
	got := fresh.Mint(channel)
	require.Greater(t, got, priorCS,
		"a fresh mint recovers from broker history; next serial > prior high-water")
}

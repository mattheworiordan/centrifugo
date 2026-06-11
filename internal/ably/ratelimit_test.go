package ably

// C5: the advertised maxInboundRate (CD2e) is enforced — over-rate inbound
// publishes are NACKed 42911 (nonfatal per-connection publish-rate code)
// while the connection survives.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/auth"
	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestRateLimiterTokenBucket_C5(t *testing.T) {
	t.Parallel()
	t0 := time.Now()
	rl := newRateLimiter(1000) // 1000/s, capacity 1000

	allowed := 0
	for i := 0; i < 1000; i++ {
		if rl.allow(t0) { // all at the same instant — no refill
			allowed++
		}
	}
	require.Equal(t, 1000, allowed, "a burst up to capacity is allowed")
	require.False(t, rl.allow(t0), "the 1001st at the same instant is denied")
	require.True(t, rl.allow(t0.Add(time.Millisecond)), "1ms refills ~1 token at 1000/s")
}

// Over-rate publishes NACK 42911; the first (within rate) is ACKed.
func TestPublishRateLimitNacks_C5(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	capability, err := auth.ParseCapability(`{"*":["*"]}`)
	require.NoError(t, err)
	conn := &blockingConn{}
	s := newBareSession(ts, conn, capability)
	s.inboundRate = newRateLimiter(1) // 1/s, capacity 1 → the 2nd publish trips it

	msg := func(serial int64) *protocol.ProtocolMessage {
		return &protocol.ProtocolMessage{
			Action: protocol.ActionMessage, Channel: "c5-rate", MsgSerial: serial,
			Messages: []*protocol.Message{{Name: "ev", Data: "x"}},
		}
	}
	s.handleFrame(msg(0)) // within rate → ACK
	s.handleFrame(msg(1)) // over rate → NACK 42911

	var sawAck, sawRateNack bool
	conn.mu.Lock()
	for _, raw := range conn.frames {
		var m protocol.ProtocolMessage
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		switch m.Action {
		case protocol.ActionAck:
			sawAck = true
		case protocol.ActionNack:
			if m.Error != nil && m.Error.Code == errCodePublishRateExceeded {
				sawRateNack = true
			}
		}
	}
	conn.mu.Unlock()
	require.True(t, sawAck, "the first publish (within rate) is ACKed")
	require.True(t, sawRateNack, "the second publish (over rate) is NACKed 42911")
}

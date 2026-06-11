package ably

// Channel serial minting (RTL15 territory). Every publication on an
// Ably-visible channel is stamped with a fresh channelSerial — the
// lexicographically-ordered position identifier SDKs track as
// channel.properties.channelSerial (RTL15b) and present back on
// re-ATTACH as the resume cursor. The serial rides as a publication tag
// (pubTagSerial) so delivery and history reads recover it without
// re-parsing envelopes, and inside each Message envelope as
// Message.Serial (TM2-territory; the M8 mutation surface addresses
// messages by this serial).
//
// One mint per process (handler-owned), one Generator per channel:
// REST and realtime publishes on the same channel draw from the same
// monotonic sequence. Since Phase 3 (T1.2), mint+append are ATOMIC per
// channel: every mint-then-publish path holds the channel's publish
// lock (lockChannel) across the pair, so serial order always matches
// broker offset order — the same guarantee the real Ably service makes.
// Resume cursors still resolve by serial→publication LOOKUP (robust
// regardless), but the lexicographic-order invariant now holds.
//
// Divergence note (documented for M9): a multi-message publish is
// delivered by this adapter as N single-message publications, each with
// its OWN channelSerial (real Ably delivers one frame per atomic batch
// with one shared channelSerial; messages take serial <cs>:<idx>). Here
// every Message.Serial is <cs>:000. Cursor semantics stay exact because
// serial↔publication is 1:1.

import (
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
)

// pubTagSerial is the publication tag carrying the publication's
// channelSerial (alongside pubTagOrigin "o" and pubTagKind "k").
const pubTagSerial = "s"

// serialMint hands out per-channel monotonic channelSerials. Safe for
// concurrent use.
type serialMint struct {
	seriesID string

	mu    sync.Mutex
	gens  map[string]*serial.Generator
	locks map[string]*sync.Mutex
}

func newSerialMint() *serialMint {
	return &serialMint{
		seriesID: serial.NewSeriesID(),
		gens:     make(map[string]*serial.Generator),
		locks:    make(map[string]*sync.Mutex),
	}
}

// lockChannel acquires the channel's publish lock — held across every
// mint+broker-append pair so serial order equals offset order. Returns
// the unlock func. Lock ordering: the channel lock is OUTERMOST; the
// serialMint and materialized-store mutexes nest inside it (the
// presence-store mutex is never held under it at all), and no path ever
// holds two channel locks at once.
func (m *serialMint) lockChannel(channel string) func() {
	m.mu.Lock()
	l, ok := m.locks[channel]
	if !ok {
		l = &sync.Mutex{}
		m.locks[channel] = l
	}
	m.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// Mint returns the next channelSerial for channel.
func (m *serialMint) Mint(channel string) string {
	m.mu.Lock()
	gen, ok := m.gens[channel]
	if !ok {
		gen = serial.NewGenerator(m.seriesID, nil)
		m.gens[channel] = gen
	}
	m.mu.Unlock()
	return gen.Mint()
}

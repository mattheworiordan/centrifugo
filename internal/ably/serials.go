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
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/serial"
)

const (
	// genEvictIdle is how long a channel may go without a publish before its
	// serial generator + lock are evicted (A4b), bounding the channel-keyed
	// maps under high-cardinality / long-soak load. A re-publish to an
	// evicted channel recreates the generator and reseeds it from the broker
	// high-water (D3), so eviction never regresses serials.
	genEvictIdle = 10 * time.Minute
	// genSweepInterval caps how often lockChannel runs the O(channels)
	// eviction sweep, amortizing it to O(1) per publish.
	genSweepInterval = time.Minute
)

// pubTagSerial is the publication tag carrying the publication's
// channelSerial (alongside pubTagOrigin "o" and pubTagKind "k").
const pubTagSerial = "s"

// serialMint hands out per-channel monotonic channelSerials. Safe for
// concurrent use.
type serialMint struct {
	seriesID string
	// seed recovers a channel's high-water (ts, counter) from durable
	// storage when a generator is created cold — a fresh process (D3,
	// restart survival) or an evicted generator (A4b). Returns ok=false
	// when there is no prior serial (empty/aged-out history). nil disables
	// recovery (memory-only callers that never restart).
	seed func(channel string) (ts int64, counter int, ok bool)

	mu    sync.Mutex
	gens  map[string]*serial.Generator
	locks map[string]*sync.Mutex
	// refs counts in-flight lockChannel holders per channel; lastUsed is the
	// ms of the last lockChannel. A channel is evictable only when refs==0
	// AND idle past genEvictIdle — refs protects a channel whose lock is held
	// (or about to be acquired) from having its *sync.Mutex swapped out from
	// under a publisher, which would silently break mint+append atomicity (A4b).
	refs      map[string]int
	lastUsed  map[string]int64
	lastSweep int64
}

func newSerialMint(seed func(channel string) (ts int64, counter int, ok bool)) *serialMint {
	return &serialMint{
		seriesID: serial.NewSeriesID(),
		seed:     seed,
		gens:     make(map[string]*serial.Generator),
		locks:    make(map[string]*sync.Mutex),
		refs:     make(map[string]int),
		lastUsed: make(map[string]int64),
	}
}

// lockChannel acquires the channel's publish lock — held across every
// mint+broker-append pair so serial order equals offset order. Returns
// the unlock func. Lock ordering: the channel lock is OUTERMOST; the
// serialMint and materialized-store mutexes nest inside it (the
// presence-store mutex is never held under it at all), and no path ever
// holds two channel locks at once.
func (m *serialMint) lockChannel(channel string) func() {
	now := time.Now().UnixMilli()
	m.mu.Lock()
	l, ok := m.locks[channel]
	if !ok {
		l = &sync.Mutex{}
		m.locks[channel] = l
	}
	// A4b: mark this channel in-flight (refs) and recently-used BEFORE the
	// sweep runs, so neither this channel nor any concurrently-acquiring one
	// is evicted under us. The sweep is amortized (genSweepInterval-gated).
	m.refs[channel]++
	m.lastUsed[channel] = now
	m.evictIdleLocked(now)
	m.mu.Unlock()
	l.Lock()
	return func() {
		l.Unlock()
		m.mu.Lock()
		m.refs[channel]--
		if m.refs[channel] <= 0 {
			delete(m.refs, channel)
		}
		m.mu.Unlock()
	}
}

// evictIdleLocked drops generators + locks for channels idle past
// genEvictIdle, at most once per genSweepInterval. Assumes m.mu is held. A
// channel with refs>0 (a publisher holding or acquiring its lock) is never
// evicted — swapping its *sync.Mutex would break mint+append atomicity.
func (m *serialMint) evictIdleLocked(now int64) {
	if m.lastSweep != 0 && now-m.lastSweep < genSweepInterval.Milliseconds() {
		return
	}
	m.lastSweep = now
	for ch, used := range m.lastUsed {
		if m.refs[ch] == 0 && now-used > genEvictIdle.Milliseconds() {
			delete(m.gens, ch)
			delete(m.locks, ch)
			delete(m.lastUsed, ch)
		}
	}
}

// sweepGenerators forces an eviction sweep at the given clock and reports how
// many generators were evicted — the test entry point for A4b.
func (m *serialMint) sweepGenerators(now int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	before := len(m.gens)
	m.lastSweep = 0 // bypass the interval gate
	m.evictIdleLocked(now)
	return before - len(m.gens)
}

// Mint returns the next channelSerial for channel.
//
// On a COLD channel (no in-memory generator — a fresh process after a
// restart, or a generator evicted by A4b), the generator is seeded from the
// broker high-water (D3) so the first post-restart serial is strictly
// greater than any pre-restart serial and continuity never regresses. The
// seed read happens OUTSIDE m.mu: every Mint caller holds the channel's
// publish lock (lockChannel), so at most one goroutine mints a given channel
// at a time — there is no concurrent creator to race, and holding m.mu
// (which lockChannel also takes) across the broker read would stall every
// other channel's publishers.
func (m *serialMint) Mint(channel string) string {
	m.mu.Lock()
	gen, ok := m.gens[channel]
	m.mu.Unlock()
	if ok {
		return gen.Mint()
	}
	gen = serial.NewGenerator(m.seriesID, nil)
	if m.seed != nil {
		if ts, counter, ok := m.seed(channel); ok {
			gen.Restore(ts, counter)
		}
	}
	m.mu.Lock()
	m.gens[channel] = gen
	m.mu.Unlock()
	return gen.Mint()
}

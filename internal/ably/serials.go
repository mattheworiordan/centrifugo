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
// monotonic sequence. Serial order matches broker offset order for
// non-overlapping publishes only: mint (buildEnvelopes) and append
// (node.Publish) are not atomic, so two near-simultaneous publishers
// can mint in one order and reach the broker in the other. Real Ably
// serializes mint+append; this single-node PoC does not. CONSEQUENCE
// FOR M6.3: a resume cursor must be resolved by serial→publication
// LOOKUP (find the publication tagged with the cursor serial, replay
// everything after its offset) — NEVER by lexicographic filtering
// (serial > cursor), which an inverted pair would corrupt into a skip
// or double-delivery.
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

	mu   sync.Mutex
	gens map[string]*serial.Generator
}

func newSerialMint() *serialMint {
	return &serialMint{
		seriesID: serial.NewSeriesID(),
		gens:     make(map[string]*serial.Generator),
	}
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

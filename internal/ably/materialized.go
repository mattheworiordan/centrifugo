package ably

// Materialized message state for mutableMessages channels (the AIT
// conformance core). The raw op stream — create, update, delete, append
// publications — lives in centrifuge history and drives live delivery;
// this adapter-owned keyed store holds the LATEST materialized state per
// message serial plus its version history, serving:
//
//   - GET /channels/{ch}/messages/{serial}        (RSL11, latest state)
//   - GET /channels/{ch}/messages/{serial}/versions (RSL14)
//   - rewind on mutable channels (materialized state, NOT the op stream:
//     rewind=1 after create+append delivers ONE concatenated message —
//     pinned by ably-js 'Should append to a message over realtime')
//
// Materialization semantics (M8-AIT-SCOPE, verified against AIT 0.2.0):
// update replaces data and extras wholesale; append concatenates string
// data (AIT token deltas are strings; non-string appends replace —
// documented divergence) and replaces extras wholesale; delete empties
// the data. The materialized action is message.update after any
// update/append (the rewind pin) and message.delete after delete. The
// message timestamp stays create-anchored; version.timestamp moves.
//
// Retention (A3): the store is BOUNDED. Per entry, the version history is
// capped (count + summed bytes) so a long AIT append stream no longer
// grows O(N) full-state snapshots each O(length) → O(N²); the latest
// materialized state is always retained in full regardless. Whole entries
// carry a TTL aligned with the channel's history retention tier and are
// evicted lazily on access and when a sibling create touches the channel
// (no background goroutine — materialized entries only ever live on
// mutable channels, which are all persistent/24h, so eviction is rare).
//
// Single-node PoC posture, like presenceStore: a multi-node engine would
// back this with centrifuge's MapBroker (keyed state + stream, M9 note —
// replace-only semantics there mean the append/version layer stays here).

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

const (
	// maxVersionsRetained caps an entry's retained version snapshots. GET
	// .../versions returns a single page (RSL14), so deep history isn't
	// contractually promised; keeping the last N bounds an AIT append
	// stream whose versions slice otherwise grew one full-state snapshot
	// per token. The latest state lives in entry.state, always complete.
	maxVersionsRetained = 50
	// maxVersionBytesRetained caps the SUMMED string-data bytes across an
	// entry's retained snapshots — a second guard so a handful of very
	// large snapshots can't blow memory even under the count cap. Oldest
	// snapshots are dropped until both caps hold (the create/most-recent
	// are kept where possible).
	maxVersionBytesRetained = 256 * 1024
)

type materializedStore struct {
	mu       sync.Mutex
	channels map[string]map[string]*materializedEntry // channel → serial → entry
	// order tracks creation order per channel for rewind (last N).
	order map[string][]string
	// loaded marks channels whose materialized state has been reconciled
	// with broker history once in this process (D4) — so the lazy
	// rebuild-from-history runs at most once per channel and a single
	// goroutine owns the replay (concurrent replays would double-apply ops).
	loaded map[string]bool
}

type materializedEntry struct {
	state    protocol.Message
	versions []protocol.Message // snapshot AFTER each operation, oldest first
	// expiresAt is the ms-since-epoch eviction deadline, refreshed on each
	// mutation to the channel's retention TTL (A3). 0 means never expires.
	expiresAt int64
}

// retentionExpiryMS returns the eviction deadline for a freshly touched
// entry on channel — aligned with the history retention tier the channel
// name selects (publish.go). Mutable-message channels are persistent
// (24h); the ephemeral tier (2min) never applies in practice since only
// mutable channels hold materialized entries.
func retentionExpiryMS(channel string, now int64) int64 {
	if persistentChannel(channel) {
		return now + persistedHistoryTTL.Milliseconds()
	}
	return now + ephemeralHistoryTTL.Milliseconds()
}

// versionDataBytes is the string-data size of a snapshot (AIT appends are
// strings; non-string payloads are not the O(N²) concern and count 0).
func versionDataBytes(m protocol.Message) int {
	if s, ok := m.Data.(string); ok {
		return len(s)
	}
	return 0
}

// capVersionsLocked drops oldest version snapshots until both the count
// and summed-byte caps hold. The most recent snapshot is always kept.
func capVersionsLocked(entry *materializedEntry) {
	for len(entry.versions) > maxVersionsRetained {
		entry.versions = entry.versions[1:]
	}
	total := 0
	for _, v := range entry.versions {
		total += versionDataBytes(v)
	}
	for total > maxVersionBytesRetained && len(entry.versions) > 1 {
		total -= versionDataBytes(entry.versions[0])
		entry.versions = entry.versions[1:]
	}
}

func newMaterializedStore() *materializedStore {
	return &materializedStore{
		channels: make(map[string]map[string]*materializedEntry),
		order:    make(map[string][]string),
		loaded:   make(map[string]bool),
	}
}

// markLoadedIfAbsent claims the one-time history rebuild for a channel (D4).
// It returns true when no rebuild is needed — the channel is already loaded,
// or already holds entries from live traffic (current, no rebuild). It
// returns false (and marks the channel loaded) for exactly one caller, which
// then owns the replay; clearLoaded reverts the claim if that replay cannot
// proceed (e.g. history unavailable).
//
// loaded is intentionally NOT cleared when A3 TTL-evicts a channel's entries:
// the materialized TTL (persistedHistoryTTL) is aligned with the broker
// WithHistory TTL, so when an entry expires the history it would rebuild from
// has expired too — there is no recoverable state to reload, and skipping the
// rebuild (returning a clean 404) is correct.
func (s *materializedStore) markLoadedIfAbsent(channel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded[channel] || len(s.channels[channel]) > 0 {
		s.loaded[channel] = true
		return true
	}
	s.loaded[channel] = true
	return false
}

// clearLoaded reverts a load claim so a later read retries the rebuild.
func (s *materializedStore) clearLoaded(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.loaded, channel)
}

// create registers a freshly published message as materialized state.
func (s *materializedStore) create(channel string, msg *protocol.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	// A3: a new message touching the channel is the moment to reclaim its
	// expired siblings (no background goroutine; cheap — entries-per-channel
	// is small and nothing expires under normal operation).
	s.evictChannelExpiredLocked(channel, now)
	entries, ok := s.channels[channel]
	if !ok {
		entries = make(map[string]*materializedEntry)
		s.channels[channel] = entries
	}
	if _, exists := entries[msg.Serial]; exists {
		return
	}
	// The create is itself the first version (RSL14: getMessageVersions
	// returns the create alongside subsequent ops — action message.create).
	entries[msg.Serial] = &materializedEntry{
		state:     *msg,
		versions:  []protocol.Message{*msg},
		expiresAt: retentionExpiryMS(channel, now),
	}
	s.order[channel] = append(s.order[channel], msg.Serial)
}

// mutationProblem is the verdict of a failed mutation.
type mutationProblem struct {
	code       int
	statusCode int
	message    string
}

// mutate prepares and immediately commits a mutation — the single-step
// form used by the REST/realtime semantics tests and any caller that does
// not need the publish-then-commit split.
func (s *materializedStore) mutate(channel, serial string, action int, data any, encoding string, extras any, version *protocol.MessageVersion) (*protocol.Message, *mutationProblem) {
	op, commit, prob := s.prepareMutation(channel, serial, action, data, encoding, extras, version)
	if prob != nil {
		return nil, prob
	}
	commit()
	return op, nil
}

// prepareMutation validates an update (1), delete (2) or append (5) and
// computes the OP message to fan out — WITHOUT mutating the store. It
// returns a commit closure the caller invokes ONLY after the broker
// publish succeeds (B7): committing before the publish left a phantom
// version when the publish failed (a real path once the broker is Redis).
// The op carries the original serial and name, the op's data (full for
// update, delta for append, {} for delete), the op action, and the full
// version. The caller owns minting versionSerial and holds the channel
// publish lock across prepare→publish→commit, which serializes mutations
// and creates on this channel, so the entry cannot change between prepare
// and commit. The caller owns minting versionSerial.
func (s *materializedStore) prepareMutation(channel, serial string, action int, data any, encoding string, extras any, version *protocol.MessageVersion) (*protocol.Message, func(), *mutationProblem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.channels[channel][serial]
	if !ok {
		return nil, nil, &mutationProblem{code: errCodeNotFound, statusCode: 404, message: "message not found"}
	}

	// A version is a strictly LATER occurrence than what it mutates: on
	// loopback the create and the op can land in the same millisecond,
	// and SDK tests assert version.timestamp > message.timestamp (the
	// same local-speed artifact class as the heartbeat ping floor).
	// Clamp past both the create and the previous version so version
	// timestamps stay strictly monotonic.
	floor := entry.state.Timestamp
	if entry.state.Version != nil && entry.state.Version.Timestamp > floor {
		floor = entry.state.Version.Timestamp
	}
	if version.Timestamp <= floor {
		version.Timestamp = floor + 1
	}

	// Compute the NEW state on a COPY — the store is not touched until commit.
	newState := entry.state
	switch action {
	case protocol.MessageActionUpdate:
		newState.Data = data
		newState.Encoding = encoding
		newState.Extras = extras
		newState.Action = protocol.MessageActionUpdate
	case protocol.MessageActionAppend:
		// AIT token deltas: PLAIN string data — both encoding chains empty
		// — concatenates; anything else replaces (documented divergence —
		// AIT only appends plain strings). Matching NON-empty chains must
		// NOT concat: base64+base64 or json+json string-concat corrupts
		// the payload (padding mid-string / invalid JSON).
		old, okOld := entry.state.Data.(string)
		delta, okNew := data.(string)
		if okOld && okNew && entry.state.Encoding == "" && encoding == "" {
			newState.Data = old + delta
		} else {
			newState.Data = data
			newState.Encoding = encoding
		}
		newState.Extras = extras
		newState.Action = protocol.MessageActionUpdate
	case protocol.MessageActionDelete:
		// The deletion op's data ({} as sent by SDKs) becomes the state.
		newState.Data = data
		newState.Encoding = encoding
		newState.Extras = extras
		newState.Action = protocol.MessageActionDelete
	default:
		return nil, nil, &mutationProblem{code: errCodeBadRequest, statusCode: 400, message: "unsupported message action"}
	}
	newState.Version = version

	op := &protocol.Message{
		Name:     entry.state.Name,
		Data:     data,
		Encoding: encoding,
		Serial:   serial,
		Action:   action,
		Version:  version,
		Extras:   extras,
		// The op event is its own occurrence in time; the MATERIALIZED
		// state keeps the create-anchored timestamp.
		Timestamp: time.Now().UnixMilli(),
		ClientID:  entry.state.ClientID,
	}

	commit := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		e, ok := s.channels[channel][serial]
		if !ok {
			// Evicted between prepare and commit (only possible if the caller
			// did not hold the channel lock across the pair); drop the commit
			// rather than resurrect a removed entry.
			return
		}
		e.state = newState
		e.versions = append(e.versions, e.state)
		// A3: bound the per-entry version history and refresh the retention
		// deadline — an active message stays live.
		capVersionsLocked(e)
		e.expiresAt = retentionExpiryMS(channel, time.Now().UnixMilli())
	}
	return op, commit, nil
}

// get returns the latest materialized state for a serial.
func (s *materializedStore) get(channel, serial string) (protocol.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.channels[channel][serial]
	if !ok || s.expireEntryLocked(channel, serial, entry, time.Now().UnixMilli()) {
		return protocol.Message{}, false
	}
	return entry.state, true
}

// versions returns the version snapshots for a serial, oldest first.
func (s *materializedStore) versions(channel, serial string) ([]protocol.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.channels[channel][serial]
	if !ok || s.expireEntryLocked(channel, serial, entry, time.Now().UnixMilli()) {
		return nil, false
	}
	out := make([]protocol.Message, len(entry.versions))
	copy(out, entry.versions)
	return out, true
}

// latestWindow returns the materialized messages whose create-anchored
// timestamps fall inside the trailing window, creation order — the
// duration-form rewind source (AIT attaches with rewind='2m').
func (s *materializedStore) latestWindow(channel string, cutoffMS int64) []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	serials := s.order[channel]
	var out []protocol.Message
	for _, serial := range serials {
		if entry, ok := s.channels[channel][serial]; ok && !entryExpired(entry, now) && entry.state.Timestamp >= cutoffMS {
			out = append(out, entry.state)
		}
	}
	return out
}

// latest returns the last n materialized messages in creation order —
// the mutable-channel rewind source.
func (s *materializedStore) latest(channel string, n int) []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	serials := s.order[channel]
	if len(serials) == 0 || n <= 0 {
		return nil
	}
	if n > len(serials) {
		n = len(serials)
	}
	now := time.Now().UnixMilli()
	out := make([]protocol.Message, 0, n)
	for _, serial := range serials[len(serials)-n:] {
		if entry, ok := s.channels[channel][serial]; ok && !entryExpired(entry, now) {
			out = append(out, entry.state)
		}
	}
	return out
}

// --- A3 TTL eviction helpers (all assume s.mu held) ---

// entryExpired reports whether an entry's retention deadline has passed.
func entryExpired(entry *materializedEntry, now int64) bool {
	return entry.expiresAt != 0 && now >= entry.expiresAt
}

// expireEntryLocked drops the entry if expired, reporting true when it did
// (so the caller treats the read as a miss).
func (s *materializedStore) expireEntryLocked(channel, serial string, entry *materializedEntry, now int64) bool {
	if !entryExpired(entry, now) {
		return false
	}
	s.removeEntryLocked(channel, serial)
	return true
}

// evictChannelExpiredLocked reclaims every expired entry on one channel.
func (s *materializedStore) evictChannelExpiredLocked(channel string, now int64) {
	for serial, entry := range s.channels[channel] {
		if entryExpired(entry, now) {
			s.removeEntryLocked(channel, serial)
		}
	}
}

// removeEntryLocked deletes one entry and its order record, cleaning up the
// channel's maps once empty.
func (s *materializedStore) removeEntryLocked(channel, serial string) {
	if entries, ok := s.channels[channel]; ok {
		delete(entries, serial)
		if len(entries) == 0 {
			delete(s.channels, channel)
		}
	}
	order := s.order[channel]
	for i, ser := range order {
		if ser == serial {
			s.order[channel] = append(order[:i], order[i+1:]...)
			break
		}
	}
	if len(s.order[channel]) == 0 {
		delete(s.order, channel)
	}
}

// sweepExpired reclaims expired entries across all channels. Cheap to call
// periodically; also the unit-test entry point for TTL eviction. Returns
// the number of entries evicted.
func (s *materializedStore) sweepExpired(now int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for channel, entries := range s.channels {
		for serial, entry := range entries {
			if entryExpired(entry, now) {
				s.removeEntryLocked(channel, serial)
				evicted++
			}
		}
	}
	return evicted
}

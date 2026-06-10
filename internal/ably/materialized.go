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
// Retention: the store never evicts (no TTL) — single-node PoC posture;
// production would bound it alongside the history retention tiers.
//
// Single-node PoC posture, like presenceStore: a multi-node engine would
// back this with centrifuge's MapBroker (keyed state + stream, M9 note —
// replace-only semantics there mean the append/version layer stays here).

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

type materializedStore struct {
	mu       sync.Mutex
	channels map[string]map[string]*materializedEntry // channel → serial → entry
	// order tracks creation order per channel for rewind (last N).
	order map[string][]string
}

type materializedEntry struct {
	state    protocol.Message
	versions []protocol.Message // snapshot AFTER each operation, oldest first
}

func newMaterializedStore() *materializedStore {
	return &materializedStore{
		channels: make(map[string]map[string]*materializedEntry),
		order:    make(map[string][]string),
	}
}

// create registers a freshly published message as materialized state.
func (s *materializedStore) create(channel string, msg *protocol.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	entries[msg.Serial] = &materializedEntry{state: *msg, versions: []protocol.Message{*msg}}
	s.order[channel] = append(s.order[channel], msg.Serial)
}

// mutationProblem is the verdict of a failed mutation.
type mutationProblem struct {
	code       int
	statusCode int
	message    string
}

// mutate applies an update (1), delete (2) or append (5) to the
// materialized state and records the version snapshot. The returned
// message is the OP message to fan out: original serial and name, the
// op's data (full for update, delta for append, {} for delete), the op
// action, and the full version. The caller owns minting versionSerial.
func (s *materializedStore) mutate(channel, serial string, action int, data any, encoding string, extras any, version *protocol.MessageVersion) (*protocol.Message, *mutationProblem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.channels[channel]
	entry, ok := entries[serial]
	if !ok {
		return nil, &mutationProblem{code: errCodeNotFound, statusCode: 404, message: "message not found"}
	}

	switch action {
	case protocol.MessageActionUpdate:
		entry.state.Data = data
		entry.state.Encoding = encoding
		entry.state.Extras = extras
		entry.state.Action = protocol.MessageActionUpdate
	case protocol.MessageActionAppend:
		// AIT token deltas: PLAIN string data — both encoding chains empty
		// — concatenates; anything else replaces (documented divergence —
		// AIT only appends plain strings). Matching NON-empty chains must
		// NOT concat: base64+base64 or json+json string-concat corrupts
		// the payload (padding mid-string / invalid JSON).
		old, okOld := entry.state.Data.(string)
		delta, okNew := data.(string)
		if okOld && okNew && entry.state.Encoding == "" && encoding == "" {
			entry.state.Data = old + delta
		} else {
			entry.state.Data = data
			entry.state.Encoding = encoding
		}
		entry.state.Extras = extras
		entry.state.Action = protocol.MessageActionUpdate
	case protocol.MessageActionDelete:
		// The deletion op's data ({} as sent by SDKs) becomes the state.
		entry.state.Data = data
		entry.state.Encoding = encoding
		entry.state.Extras = extras
		entry.state.Action = protocol.MessageActionDelete
	default:
		return nil, &mutationProblem{code: errCodeBadRequest, statusCode: 400, message: "unsupported message action"}
	}
	entry.state.Version = version
	entry.versions = append(entry.versions, entry.state)

	op := protocol.Message{
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
	return &op, nil
}

// get returns the latest materialized state for a serial.
func (s *materializedStore) get(channel, serial string) (protocol.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.channels[channel][serial]
	if !ok {
		return protocol.Message{}, false
	}
	return entry.state, true
}

// versions returns the version snapshots for a serial, oldest first.
func (s *materializedStore) versions(channel, serial string) ([]protocol.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.channels[channel][serial]
	if !ok {
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
	serials := s.order[channel]
	var out []protocol.Message
	for _, serial := range serials {
		if entry, ok := s.channels[channel][serial]; ok && entry.state.Timestamp >= cutoffMS {
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
	out := make([]protocol.Message, 0, n)
	for _, serial := range serials[len(serials)-n:] {
		if entry, ok := s.channels[channel][serial]; ok {
			out = append(out, entry.state)
		}
	}
	return out
}

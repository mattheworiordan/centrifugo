package ably

// Adapter-owned presence (RTP): the member set per channel, keyed by
// connectionId+clientId (a connection may hold one entry per identity).
// centrifuge's Node-level presence mutators are unexported and its
// subscription-scoped auto-presence cannot express Ably's explicit
// ENTER/UPDATE/LEAVE lifecycle, so the single-node PoC owns the map —
// multi-node engines would need the PresenceManager interface threaded
// through the app wiring (M9 note).
//
// Presence events fan out as ordinary channel publications carrying the
// pubTagKind tag, riding the existing delivery machinery: subscribers'
// handleReply decodes them into PRESENCE frames instead of MESSAGE. The
// publications deliberately carry NO history options (presence must not
// pollute message history) and NO origin tag (presence events are
// delivered to everyone including the originator, regardless of the
// echo=false message filter).

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

// pubTagKind marks a publication's payload kind: value "p" = a presence
// event (PresenceMessage envelope) rather than a message.
const (
	pubTagKind         = "k"
	pubTagKindPresence = "p"
)

// defaultPresenceGrace is how long an abruptly-disconnected connection's
// members remain present before synthesized LEAVEs fan out — the
// advertised ~15s window that prevents presence flicker across client
// reconnects. A clean CLOSE leaves immediately.
const defaultPresenceGrace = 15 * time.Second

// presenceStore is the in-memory member set.
type presenceStore struct {
	mu       sync.Mutex
	grace    time.Duration
	channels map[string]map[string]*protocol.PresenceMessage
}

func newPresenceStore() *presenceStore {
	return &presenceStore{
		grace:    defaultPresenceGrace,
		channels: make(map[string]map[string]*protocol.PresenceMessage),
	}
}

func memberKey(connectionID, clientID string) string {
	return connectionID + ":" + clientID
}

// set records an ENTER or UPDATE as the member's current state.
func (s *presenceStore) set(channel string, member *protocol.PresenceMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members, ok := s.channels[channel]
	if !ok {
		members = make(map[string]*protocol.PresenceMessage)
		s.channels[channel] = members
	}
	members[memberKey(member.ConnectionID, member.ClientID)] = member
}

// remove drops a member on LEAVE, reporting whether it was present.
func (s *presenceStore) remove(channel, connectionID, clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	members, ok := s.channels[channel]
	if !ok {
		return false
	}
	key := memberKey(connectionID, clientID)
	if _, present := members[key]; !present {
		return false
	}
	delete(members, key)
	if len(members) == 0 {
		delete(s.channels, channel)
	}
	return true
}

// members returns a snapshot of the channel's member set.
func (s *presenceStore) members(channel string) []*protocol.PresenceMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.channels[channel]
	if len(members) == 0 {
		return nil
	}
	out := make([]*protocol.PresenceMessage, 0, len(members))
	for _, m := range members {
		out = append(out, m)
	}
	return out
}

// removeConnection drops every entry a connection holds, returning the
// (channel, member) pairs removed so the caller can fan out LEAVE events
// — the abrupt-disconnect path.
func (s *presenceStore) removeConnection(connectionID string) map[string][]*protocol.PresenceMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := make(map[string][]*protocol.PresenceMessage)
	for channel, members := range s.channels {
		for key, m := range members {
			if m.ConnectionID == connectionID {
				removed[channel] = append(removed[channel], m)
				delete(members, key)
			}
		}
		if len(members) == 0 {
			delete(s.channels, channel)
		}
	}
	return removed
}

// expireConnection implements the post-grace reconciliation: every member
// the dead connection still holds is removed, but a synthesized LEAVE is
// only reported for clientIds that have NOT re-entered the channel on
// another connection in the meantime — the whole point of the grace
// window is that a reconnecting client never flickers.
func (s *presenceStore) expireConnection(connectionID string) map[string][]*protocol.PresenceMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	leaves := make(map[string][]*protocol.PresenceMessage)
	for channel, members := range s.channels {
		for key, m := range members {
			if m.ConnectionID != connectionID {
				continue
			}
			delete(members, key)
			reentered := false
			for _, other := range members {
				if other.ClientID == m.ClientID {
					reentered = true
					break
				}
			}
			if !reentered {
				leaves[channel] = append(leaves[channel], m)
			}
		}
		if len(members) == 0 {
			delete(s.channels, channel)
		}
	}
	return leaves
}

// scheduleExpiry arms the grace timer for an abruptly-disconnected
// connection; fanout receives each synthesized LEAVE after the window.
func (s *presenceStore) scheduleExpiry(connectionID string, fanout func(channel string, member *protocol.PresenceMessage)) {
	time.AfterFunc(s.grace, func() {
		now := time.Now().UnixMilli()
		for channel, members := range s.expireConnection(connectionID) {
			for _, m := range members {
				leave := *m
				leave.Action = protocol.PresenceLeave
				leave.Timestamp = now
				fanout(channel, &leave)
			}
		}
	})
}

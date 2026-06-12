package ably

// Adapter-owned presence (RTP): the member set per channel, keyed by
// connectionId+clientId (a connection may hold one entry per identity).
// centrifuge's Node-level presence mutators are unexported and its
// subscription-scoped auto-presence cannot express Ably's explicit
// ENTER/UPDATE/LEAVE lifecycle, so the adapter owns the member set.
//
// Single-node (memory engine): the in-process `channels` map IS the member
// set. Multi-node (Redis engine, P6.1): the adapter holds a centrifuge
// PresenceManager (Redis-backed, cross-node) — the in-process map then
// tracks only THIS node's own members (for the grace/expiry lifecycle and
// the TTL refresh), while members() reads the union across all nodes from the
// manager so HAS_PRESENCE/SYNC on node B reflect members entered on node A.
// The PresenceManager interface (AddPresence/RemovePresence/Presence) is
// exported even though the Node-level mutators are not, so the adapter drives
// it directly; the Ably member envelope rides in ClientInfo.ChanInfo.
//
// Presence events fan out as ordinary channel publications carrying the
// pubTagKind tag, riding the existing delivery machinery: subscribers'
// handleReply decodes them into PRESENCE frames instead of MESSAGE. The
// publications deliberately carry NO history options (presence must not
// pollute message history) and NO origin tag (presence events are
// delivered to everyone including the originator, regardless of the
// echo=false message filter).

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// pubTagKind marks a publication's payload kind: value "p" = a presence
// event (PresenceMessage envelope) rather than a message.
const (
	pubTagKind         = "k"
	pubTagKindPresence = "p"
)

// presenceHistoryChannel is the shadow channel retaining presence events
// for GET .../presence/history. The ":" prefix makes it unreachable by
// clients: the adapter rejects ':'-prefixed channel names (40010), so no
// Ably client can attach, publish or read it directly.
func presenceHistoryChannel(channel string) string {
	return ":presence:" + channel
}

// defaultPresenceGrace is how long an abruptly-disconnected connection's
// members remain present before synthesized LEAVEs fan out — the
// advertised ~15s window that prevents presence flicker across client
// reconnects. A clean CLOSE leaves immediately.
const defaultPresenceGrace = 15 * time.Second

// defaultPresenceTTL is how long a member entry lives in the cross-node
// (Redis) presence manager before expiring if not refreshed. The refresh
// loop re-asserts this node's members every defaultPresenceTTL/2, so a LIVE
// member never expires; a member whose owning NODE has died stops being
// refreshed and leaves the shared set within this TTL — the documented
// dead-node presence-cleanup bound (P6.1). Matches centrifuge's default.
const defaultPresenceTTL = 60 * time.Second

// presenceStore is the per-channel member set. The in-process map is
// authoritative single-node; with a cross-node manager (mgr != nil) it tracks
// this node's own members and the manager holds the shared view.
type presenceStore struct {
	mu       sync.Mutex
	grace    time.Duration
	channels map[string]map[string]*protocol.PresenceMessage
	// timers holds the pending grace-expiry timer per abruptly-dropped
	// connectionId, so a recovery inside the window can cancel it (the
	// connection never died — its members must neither vanish nor LEAVE).
	timers map[string]*time.Timer

	// mgr is the cross-node presence manager (P6.1); nil on the memory engine
	// (single-node), where the in-process map is the whole story. ttl is the
	// manager entry lifetime; the refresh loop re-asserts at ttl/2.
	mgr centrifuge.PresenceManager
	ttl time.Duration
}

func newPresenceStore() *presenceStore {
	return newPresenceStoreWithManager(nil)
}

// newPresenceStoreWithManager builds a store optionally backed by a cross-node
// presence manager (Redis engine). When mgr is non-nil the caller must also
// call startRefresh so this node's members do not expire from the manager.
func newPresenceStoreWithManager(mgr centrifuge.PresenceManager) *presenceStore {
	return &presenceStore{
		grace:    defaultPresenceGrace,
		channels: make(map[string]map[string]*protocol.PresenceMessage),
		timers:   make(map[string]*time.Timer),
		mgr:      mgr,
		ttl:      defaultPresenceTTL,
	}
}

func memberKey(connectionID, clientID string) string {
	return connectionID + ":" + clientID
}

// set records an ENTER or UPDATE as the member's current state.
func (s *presenceStore) set(channel string, member *protocol.PresenceMessage) {
	s.mu.Lock()
	members, ok := s.channels[channel]
	if !ok {
		members = make(map[string]*protocol.PresenceMessage)
		s.channels[channel] = members
	}
	members[memberKey(member.ConnectionID, member.ClientID)] = member
	s.mu.Unlock()
	// Mirror to the shared store OUTSIDE the lock (network I/O must never run
	// under s.mu).
	s.mgrAdd(channel, member)
}

// remove drops a member on LEAVE, reporting whether it was present on THIS
// node (the member belongs to the leaving connection, which lives here).
func (s *presenceStore) remove(channel, connectionID, clientID string) bool {
	s.mu.Lock()
	members, ok := s.channels[channel]
	if !ok {
		s.mu.Unlock()
		return false
	}
	key := memberKey(connectionID, clientID)
	if _, present := members[key]; !present {
		s.mu.Unlock()
		return false
	}
	delete(members, key)
	if len(members) == 0 {
		delete(s.channels, channel)
	}
	s.mu.Unlock()
	s.mgrRemove(channel, key)
	return true
}

// members returns a snapshot of the channel's member set — the cross-node
// union when a manager is present, else this node's in-process set.
func (s *presenceStore) members(channel string) []*protocol.PresenceMessage {
	if s.mgr != nil {
		if out, ok := s.mgrMembers(channel); ok {
			return out
		}
		// Manager read failed (Redis hiccup): degrade to this node's local
		// view rather than reporting an empty channel.
	}
	return s.localMembers(channel)
}

// localMembers reads this node's in-process member set.
func (s *presenceStore) localMembers(channel string) []*protocol.PresenceMessage {
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
	s.mu.Unlock()
	s.mgrRemoveAll(removed)
	return removed
}

// expireConnection implements the post-grace reconciliation: every member
// the dead connection still holds is removed, but a synthesized LEAVE is
// only reported for clientIds that have NOT re-entered the channel on
// another connection in the meantime — the whole point of the grace
// window is that a reconnecting client never flickers.
//
// Multi-node note (P6.1): the re-entry suppression checks only THIS node's
// map, so a clientId that re-enters on a DIFFERENT node within the grace
// window can still get a redundant synthesized LEAVE event. The shared
// presence STATE stays correct — the re-entered member has a different
// connectionId (so a different memberKey), so mgrRemoveAll below never purges
// it and cross-node members()/SYNC still show it — only an extra LEAVE event
// fans out. A bounded, documented divergence.
func (s *presenceStore) expireConnection(connectionID string) map[string][]*protocol.PresenceMessage {
	s.mu.Lock()
	leaves := make(map[string][]*protocol.PresenceMessage)
	// removed tracks ALL of the connection's deleted members (a superset of
	// leaves — re-entered identities are removed locally too) so every entry
	// is purged from the shared manager.
	removed := make(map[string][]*protocol.PresenceMessage)
	for channel, members := range s.channels {
		for key, m := range members {
			if m.ConnectionID != connectionID {
				continue
			}
			delete(members, key)
			removed[channel] = append(removed[channel], m)
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
	s.mu.Unlock()
	s.mgrRemoveAll(removed)
	return leaves
}

// scheduleExpiry arms the grace timer for an abruptly-disconnected
// connection; fanout receives each synthesized LEAVE after the window.
// A timer already pending for the connection is replaced.
func (s *presenceStore) scheduleExpiry(connectionID string, fanout func(channel string, member *protocol.PresenceMessage)) {
	// Registration happens under the lock BEFORE the timer can fire, and
	// the callback only deregisters ITSELF (a replacement timer registered
	// for the same id survives an old callback's cleanup).
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.timers[connectionID]; ok {
		prev.Stop()
	}
	var timer *time.Timer
	timer = time.AfterFunc(s.grace, func() {
		s.mu.Lock()
		if s.timers[connectionID] == timer {
			delete(s.timers, connectionID)
		}
		s.mu.Unlock()
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
	s.timers[connectionID] = timer
}

// cancelExpiry disarms a pending grace timer: called when a connection
// recovers inside the window (RTN16-lite) — the connection never died, so
// its members stay present and no LEAVE is synthesized. A timer whose
// callback already started may still run; expireConnection then removes
// whatever is left, which the recovered session re-enters as usual (the
// pre-recovery behavior).
func (s *presenceStore) cancelExpiry(connectionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timer, ok := s.timers[connectionID]; ok {
		timer.Stop()
		delete(s.timers, connectionID)
	}
}

// --- cross-node manager mirror (P6.1; all no-ops when mgr == nil) ---

// mgrAdd asserts a member into the shared presence manager, carrying the Ably
// envelope in ClientInfo.ChanInfo keyed by the member's composite key.
func (s *presenceStore) mgrAdd(channel string, m *protocol.PresenceMessage) {
	if s.mgr == nil {
		return
	}
	data, err := json.Marshal(m)
	if err != nil {
		log.Error().Err(err).Str("channel", channel).Msg("ably presence: marshal member for cross-node store")
		return
	}
	if err := s.mgr.AddPresence(channel, memberKey(m.ConnectionID, m.ClientID),
		&centrifuge.ClientInfo{ClientID: m.ClientID, UserID: m.ClientID, ChanInfo: data}); err != nil {
		log.Error().Err(err).Str("channel", channel).Msg("ably presence: cross-node AddPresence failed")
	}
}

// mgrRemove drops one member key from the shared presence manager.
func (s *presenceStore) mgrRemove(channel, key string) {
	if s.mgr == nil {
		return
	}
	if err := s.mgr.RemovePresence(channel, key, ""); err != nil {
		log.Error().Err(err).Str("channel", channel).Msg("ably presence: cross-node RemovePresence failed")
	}
}

// mgrRemoveAll purges every (channel, member) pair from the shared manager.
func (s *presenceStore) mgrRemoveAll(removed map[string][]*protocol.PresenceMessage) {
	if s.mgr == nil {
		return
	}
	for channel, members := range removed {
		for _, m := range members {
			s.mgrRemove(channel, memberKey(m.ConnectionID, m.ClientID))
		}
	}
}

// mgrMembers reads the cross-node member set from the shared manager,
// decoding each entry's Ably envelope. ok=false on a manager error so the
// caller can degrade to the local view.
func (s *presenceStore) mgrMembers(channel string) ([]*protocol.PresenceMessage, bool) {
	got, err := s.mgr.Presence(channel)
	if err != nil {
		log.Error().Err(err).Str("channel", channel).Msg("ably presence: cross-node Presence read failed")
		return nil, false
	}
	out := make([]*protocol.PresenceMessage, 0, len(got))
	for _, ci := range got {
		var m protocol.PresenceMessage
		if err := json.Unmarshal(ci.ChanInfo, &m); err != nil {
			continue
		}
		out = append(out, &m)
	}
	return out, true
}

// startRefresh re-asserts this node's members into the shared manager every
// ttl/2 so live members never expire, until stop is closed (node shutdown).
// A dead node stops refreshing → its members leave the shared set after ttl.
// No-op when there is no manager.
func (s *presenceStore) startRefresh(stop <-chan struct{}) {
	if s.mgr == nil {
		return
	}
	if s.ttl <= 0 {
		s.ttl = defaultPresenceTTL // defensive: never a zero/negative ticker
	}
	go func() {
		ticker := time.NewTicker(s.ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.refreshAll()
			}
		}
	}()
}

// refreshAll re-asserts every local member into the manager (TTL refresh).
func (s *presenceStore) refreshAll() {
	type item struct {
		channel string
		member  *protocol.PresenceMessage
	}
	s.mu.Lock()
	items := make([]item, 0)
	for channel, members := range s.channels {
		for _, m := range members {
			items = append(items, item{channel, m})
		}
	}
	s.mu.Unlock()
	for _, it := range items {
		s.mgrAdd(it.channel, it.member)
	}
}

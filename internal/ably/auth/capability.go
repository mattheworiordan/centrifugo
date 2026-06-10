package auth

// Ably capability model: a capability is a JSON object mapping channel
// resources to the operations permitted on them, carried verbatim on keys
// (the fixture) and tokens (the x-ably-capability claim) and enforced on
// attach, publish and history.
//
// Resource matching implemented for the PoC (the shapes that appear in
// the sandbox fixture and the pinned test suite):
//   - "*"            every channel
//   - exact name     that channel only
//   - "ns:*" / "x*"  trailing-star prefix match
//   - "[*]name"/"[*]*"  qualified wildcard: any qualifier, then the
//     unqualified rules above applied to the remainder
//
// Operation "*" grants every operation.

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Operations enforced by the adapter.
const (
	OpPublish   = "publish"
	OpSubscribe = "subscribe"
	OpHistory   = "history"
	OpPresence  = "presence"
)

// Capability is a parsed capability map.
type Capability struct {
	resources map[string][]string
}

// ParseCapability parses a capability JSON string. The empty string is
// the full capability {"*":["*"]} — an Ably key always has a capability,
// and a token without an x-ably-capability claim inherits its signing
// key's rights, so emptiness only arises for fixtures that omit it.
func ParseCapability(s string) (Capability, error) {
	if s == "" {
		return Capability{resources: map[string][]string{"*": {"*"}}}, nil
	}
	var resources map[string][]string
	if err := json.Unmarshal([]byte(s), &resources); err != nil {
		return Capability{}, fmt.Errorf("invalid capability: %w", err)
	}
	return Capability{resources: resources}, nil
}

// Allows reports whether the capability permits op on channel.
func (c Capability) Allows(op, channel string) bool {
	for resource, ops := range c.resources {
		if !resourceMatches(resource, channel) {
			continue
		}
		for _, granted := range ops {
			if granted == "*" || granted == op {
				return true
			}
		}
	}
	return false
}

// resourceMatches applies the resource rules above.
func resourceMatches(resource, channel string) bool {
	// Qualified wildcard: "[*]rest" matches any qualifier on the channel.
	if strings.HasPrefix(resource, "[*]") {
		rest := resource[len("[*]"):]
		// Strip the channel's own qualifier, if any, then match the
		// remainder against the unqualified rules.
		name := channel
		if strings.HasPrefix(channel, "[") {
			if end := strings.IndexByte(channel, ']'); end >= 0 {
				name = channel[end+1:]
			}
		}
		return unqualifiedMatches(rest, name)
	}
	return unqualifiedMatches(resource, channel)
}

func unqualifiedMatches(resource, channel string) bool {
	if resource == "*" {
		return true
	}
	if strings.HasSuffix(resource, "*") {
		return strings.HasPrefix(channel, strings.TrimSuffix(resource, "*"))
	}
	return resource == channel
}

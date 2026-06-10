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
	"sort"
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

// knownOps is the set of valid capability operations (TC1-adjacent; the
// operation vocabulary from the Ably capability documentation).
var knownOps = map[string]bool{
	"publish": true, "subscribe": true, "presence": true, "history": true,
	"stats": true, "channel-metadata": true, "push-subscribe": true, "push-admin": true,
}

// ValidateCapabilityShape rejects malformed capability documents on token
// requests (pinned by ably-js rest/capability "Invalid capabilities"):
// unknown operations, "*" mixed with other operations, and empty
// operation lists are all 400-class errors.
func ValidateCapabilityShape(capabilityJSON string) error {
	if capabilityJSON == "" {
		return nil
	}
	var resources map[string][]string
	if err := json.Unmarshal([]byte(capabilityJSON), &resources); err != nil {
		return fmt.Errorf("invalid capability: %w", err)
	}
	for resource, ops := range resources {
		if len(ops) == 0 {
			return fmt.Errorf("invalid capability: resource %q has no operations", resource)
		}
		for _, op := range ops {
			if op == "*" {
				if len(ops) > 1 {
					return fmt.Errorf("invalid capability: %q must not combine '*' with other operations", resource)
				}
				continue
			}
			if !knownOps[op] {
				return fmt.Errorf("invalid capability: unknown operation %q", op)
			}
		}
	}
	return nil
}

// IntersectCapability computes the capability granted to a token: the
// requested capability intersected with the key's (RSA6/TK2b semantics,
// pinned by ably-js rest/capability). An empty requested capability
// grants the key's capability verbatim. The result is canonical JSON
// (sorted resource keys via encoding/json map ordering, sorted ops);
// ok=false reports an empty intersection.
func IntersectCapability(keyCapabilityJSON, requestedJSON string) (string, bool, error) {
	if requestedJSON == "" {
		if keyCapabilityJSON == "" {
			return `{"*":["*"]}`, true, nil
		}
		return keyCapabilityJSON, true, nil
	}
	var requested map[string][]string
	if err := json.Unmarshal([]byte(requestedJSON), &requested); err != nil {
		return "", false, fmt.Errorf("invalid capability: %w", err)
	}
	// NB: Unmarshal into a pre-seeded map MERGES entries, so the full-
	// capability default is only used when the key carries no capability.
	keyCap := map[string][]string{}
	if keyCapabilityJSON == "" {
		keyCap["*"] = []string{"*"}
	} else if err := json.Unmarshal([]byte(keyCapabilityJSON), &keyCap); err != nil {
		return "", false, fmt.Errorf("invalid key capability: %w", err)
	}

	result := make(map[string][]string)
	for reqResource, reqOps := range requested {
		// Union the ops of every key resource whose grant covers the
		// requested resource (the requested resource may itself be a
		// pattern: a key grant of "*" or an identical pattern covers it).
		grantedOps := map[string]bool{}
		grantedStar := false
		for keyResource, keyOps := range keyCap {
			if keyResource != reqResource && !resourceMatches(keyResource, reqResource) {
				continue
			}
			for _, op := range keyOps {
				if op == "*" {
					grantedStar = true
				} else {
					grantedOps[op] = true
				}
			}
		}
		if !grantedStar && len(grantedOps) == 0 {
			continue // no path intersection for this resource
		}
		var ops []string
		wantStar := len(reqOps) == 1 && reqOps[0] == "*"
		switch {
		case wantStar && grantedStar:
			ops = []string{"*"}
		case wantStar:
			for op := range grantedOps {
				ops = append(ops, op)
			}
		default:
			for _, op := range reqOps {
				if grantedStar || grantedOps[op] {
					ops = append(ops, op)
				}
			}
		}
		if len(ops) == 0 {
			continue // ops intersection empty for this resource
		}
		sort.Strings(ops)
		result[reqResource] = ops
	}
	if len(result) == 0 {
		return "", false, nil
	}
	out, err := json.Marshal(result) // map keys marshal sorted: canonical
	if err != nil {
		return "", false, err
	}
	return string(out), true, nil
}

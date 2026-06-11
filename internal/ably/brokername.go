package ably

// Broker channel-name mapping. Ably channel names may contain ':'
// anywhere (the first segment is a namespace, but arbitrary colons are
// legal — ably-js's own tests publish on names like
// `publish {"transports":["web_socket"]}`), while centrifuge treats the
// text before the first ':' as a NAMESPACE and rejects subscriptions to
// unconfigured ones (102 unknown channel — the divergence #2 family).
//
// Every broker-facing channel name is therefore escaped to a
// namespace-free form: '~' → "~0", ':' → "~1" (RFC6901-style, bijective:
// decode reverses unambiguously, and a literal "~1" in an Ably name
// escapes to "~01"). Names without ':' or '~' map to themselves, so the
// common case is allocation-free. Delivery decodes Push.Channel back.
//
// Consequences:
//   - Centrifugo NAMESPACE configs no longer apply to Ably channels —
//     every Ably channel resolves to without_namespace options, which
//     carry everything the adapter needs (presence, allow_recovery,
//     allow_tags_filter, allow_subscribe_for_client). The adapter's own
//     conventions (persisted: retention tier, mutable:/ai: mutability)
//     key off the ABLY name and are unaffected.
//   - Native centrifugo clients interop under BROKER names: a native
//     channel literally named "with~1tilde" is the same channel as Ably
//     "with:tilde" (inherent to any bijective escape; no native clients
//     exist in the PoC configs).
//   - The presence shadow channel ":presence:<ch>" escapes like
//     everything else; it stays client-unreachable because client names
//     escape through the same function (no client name can collide with
//     an adapter-constructed shadow form).

import "strings"

// brokerChannel maps an Ably channel name to its centrifuge form.
func brokerChannel(ably string) string {
	if !strings.ContainsAny(ably, ":~") {
		return ably
	}
	var b strings.Builder
	b.Grow(len(ably) + 4)
	for i := 0; i < len(ably); i++ {
		switch ably[i] {
		case '~':
			b.WriteString("~0")
		case ':':
			b.WriteString("~1")
		default:
			b.WriteByte(ably[i])
		}
	}
	return b.String()
}

// ablyChannel reverses brokerChannel.
func ablyChannel(broker string) string {
	if !strings.Contains(broker, "~") {
		return broker
	}
	var b strings.Builder
	b.Grow(len(broker))
	for i := 0; i < len(broker); i++ {
		c := broker[i]
		if c == '~' && i+1 < len(broker) {
			switch broker[i+1] {
			case '0':
				b.WriteByte('~')
				i++
				continue
			case '1':
				b.WriteByte(':')
				i++
				continue
			}
		}
		b.WriteByte(c)
	}
	return b.String()
}

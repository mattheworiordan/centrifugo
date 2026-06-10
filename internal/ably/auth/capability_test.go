package auth

import "testing"

func TestCapabilityAllows(t *testing.T) {
	cases := []struct {
		name       string
		capability string
		op         string
		channel    string
		want       bool
	}{
		{"full capability allows all", `{"*":["*"]}`, OpPublish, "any:channel", true},
		{"empty string is full capability", ``, OpSubscribe, "x", true},
		{"exact resource exact op", `{"chan":["publish"]}`, OpPublish, "chan", true},
		{"exact resource wrong op", `{"chan":["publish"]}`, OpSubscribe, "chan", false},
		{"wrong resource", `{"chan":["publish"]}`, OpPublish, "other", false},
		{"op wildcard", `{"chan":["*"]}`, OpHistory, "chan", true},
		{"namespace star prefix", `{"persisted:*":["history"]}`, OpHistory, "persisted:thing", true},
		{"namespace star non-match", `{"persisted:*":["history"]}`, OpHistory, "other:thing", false},
		{"fixture cansubscribe shape", `{"cansubscribe:*":["subscribe"],"canpublish:*":["publish"]}`, OpSubscribe, "cansubscribe:x", true},
		{"fixture cansubscribe denies publish", `{"cansubscribe:*":["subscribe"]}`, OpPublish, "cansubscribe:x", false},
		{"star resource limited ops", `{"*":["subscribe"]}`, OpPublish, "chan", false},
		{"qualified wildcard matches plain channel", `{"[*]*":["*"]}`, OpPublish, "chan", true},
		{"qualified wildcard matches qualified channel", `{"[*]*":["*"]}`, OpSubscribe, "[meta]log", true},
		{"qualified name", `{"[*]stats":["subscribe"]}`, OpSubscribe, "[weekly]stats", true},
		{"qualified name non-match", `{"[*]stats":["subscribe"]}`, OpSubscribe, "[weekly]other", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ParseCapability(tc.capability)
			if err != nil {
				t.Fatalf("ParseCapability: %v", err)
			}
			if got := c.Allows(tc.op, tc.channel); got != tc.want {
				t.Errorf("Allows(%q, %q) with %q = %v, want %v", tc.op, tc.channel, tc.capability, got, tc.want)
			}
		})
	}

	t.Run("malformed capability rejected", func(t *testing.T) {
		if _, err := ParseCapability(`{broken`); err == nil {
			t.Fatal("want error")
		}
	})
}

// RSA6/TK2b intersection semantics, pinned by ably-js rest/capability.
func TestIntersectCapability(t *testing.T) {
	key2 := `{"channel0":["publish"],"channel2":["publish","subscribe"],"channel5":["presence"],"channel6":["*"]}`
	key1 := `{"cansubscribe:*":["subscribe"],"canpublish:*":["publish"]}`
	cases := []struct {
		name      string
		key, req  string
		want      string
		wantEmpty bool
	}{
		{"no request inherits key capability", key2, "", key2, false},
		{"no request, no key capability is full", "", "", `{"*":["*"]}`, false},
		{"ops intersect", key2, `{"channel2":["presence","subscribe"]}`, `{"channel2":["subscribe"]}`, false},
		{"paths drop unmatched", key2, `{"channel2":["presence","subscribe"],"channelx":["subscribe"]}`, `{"channel2":["subscribe"]}`, false},
		{"requested star expands to key ops", key2, `{"channel2":["*"]}`, `{"channel2":["publish","subscribe"]}`, false},
		{"requested ops against key star", key2, `{"channel6":["publish","subscribe"]}`, `{"channel6":["publish","subscribe"]}`, false},
		{"empty ops intersection rejected", key1, `{"canpublish:test":["subscribe"]}`, "", true},
		{"empty paths intersection rejected", key2, `{"channelx":["publish"]}`, "", true},
		{"prefix wildcard key grant", key1, `{"canpublish:check":["publish"]}`, `{"canpublish:check":["publish"]}`, false},
		{"star key grants requested pattern", `{"*":["subscribe"]}`, `{"cansubscribe:*":["subscribe"]}`, `{"cansubscribe:*":["subscribe"]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := IntersectCapability(tc.key, tc.req)
			if err != nil {
				t.Fatalf("IntersectCapability: %v", err)
			}
			if tc.wantEmpty {
				if ok {
					t.Fatalf("want empty intersection, got %q", got)
				}
				return
			}
			if !ok || got != tc.want {
				t.Errorf("got %q ok=%v, want %q", got, ok, tc.want)
			}
		})
	}
}

// Invalid capability shapes are 400-class (pinned by rest/capability
// "Invalid capabilities 1-3").
func TestValidateCapabilityShape(t *testing.T) {
	for name, capability := range map[string]string{
		"unknown op":         `{"channel0":["publish_"]}`,
		"star mixed with op": `{"channel0":["*","publish"]}`,
		"empty ops":          `{"channel0":[]}`,
		"malformed":          `{broken`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateCapabilityShape(capability); err == nil {
				t.Fatal("want error")
			}
		})
	}
	if err := ValidateCapabilityShape(`{"a":["publish"],"b":["*"]}`); err != nil {
		t.Fatalf("valid shape rejected: %v", err)
	}
}

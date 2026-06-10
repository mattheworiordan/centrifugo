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

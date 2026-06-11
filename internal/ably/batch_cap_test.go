package ably

// C4: batch requests are bounded in fan-out — an unbounded channel/spec
// count would otherwise drive thousands of per-channel decodes + publish
// locks under one request. Over-cap requests are rejected 40000; within-cap
// requests still work.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func quotedChannels(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%q", fmt.Sprintf("%s-%d", prefix, i))
	}
	return out
}

func TestBatchPublishObjectChannelCap_C4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// Over the cap → 40000.
	over := `{"channels":[` + strings.Join(quotedChannels("c4o", maxBatchTotalChannels+1), ",") +
		`],"messages":{"name":"x","data":"y"}}`
	resp := restRequest(t, ts, http.MethodPost, "/messages", []byte(over),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))

	// Within the cap → still publishes (201).
	ok := `{"channels":["c4o-ok-0","c4o-ok-1"],"messages":{"name":"x","data":"y"}}`
	respOK := restRequest(t, ts, http.MethodPost, "/messages", []byte(ok),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, respOK.StatusCode)
}

func TestBatchPublishSpecsCap_C4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	specs := make([]string, maxBatchSpecs+1)
	for i := range specs {
		specs[i] = fmt.Sprintf(`{"channels":["c4s-%d"],"messages":{"name":"x","data":"y"}}`, i)
	}
	body := "[" + strings.Join(specs, ",") + "]"
	resp := restRequest(t, ts, http.MethodPost, "/messages", []byte(body),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

func TestBatchSpecsTotalChannelCap_C4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	// One spec whose channel list alone exceeds the total cap.
	body := `[{"channels":[` + strings.Join(quotedChannels("c4t", maxBatchTotalChannels+1), ",") +
		`],"messages":{"name":"x","data":"y"}}]`
	resp := restRequest(t, ts, http.MethodPost, "/messages", []byte(body),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

func TestBatchPresenceChannelCap_C4(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)
	names := make([]string, maxBatchTotalChannels+1)
	for i := range names {
		names[i] = fmt.Sprintf("c4p-%d", i)
	}
	resp := restRequest(t, ts, http.MethodGet, "/presence?channels="+strings.Join(names, ","), nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "40000", resp.Header.Get("X-Ably-Errorcode"))
}

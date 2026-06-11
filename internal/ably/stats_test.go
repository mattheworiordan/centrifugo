package ably

// App statistics (RSC6/TS12): fixture injection via POST /stats and the
// query surface the pinned ably-js rest/stats suite drives — intervalId
// and epoch bounds, hour aggregation, limit, and Link pagination.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// statsFixtureDoc mirrors the harness's createStatsFixtureData records:
// minute intervalIds with legacy nested counters.
func statsFixtureDoc(intervalID string, in, inData, out, outData int) map[string]any {
	return map[string]any{
		"intervalId": intervalID,
		"inbound":    map[string]any{"realtime": map[string]any{"messages": map[string]any{"count": in, "data": inData}}},
		"outbound":   map[string]any{"realtime": map[string]any{"messages": map[string]any{"count": out, "data": outData}}},
	}
}

func postStatsFixtures(t *testing.T, ts *realtimeTestServer, docs []map[string]any) {
	t.Helper()
	body, err := json.Marshal(docs)
	require.NoError(t, err)
	resp := restRequest(t, ts, "POST", "/stats", body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

type statsItem struct {
	IntervalID string             `json:"intervalId"`
	Unit       string             `json:"unit"`
	Schema     string             `json:"schema"`
	AppID      string             `json:"appId"`
	InProgress string             `json:"inProgress"`
	Entries    map[string]float64 `json:"entries"`
}

func getStats(t *testing.T, ts *realtimeTestServer, query string) ([]statsItem, *http.Response) {
	t.Helper()
	resp := restRequest(t, ts, "GET", "/stats?"+query, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var items []statsItem
	require.NoError(t, json.Unmarshal(data, &items))
	return items, resp
}

// statsNextQuery extracts the rel="next" Link query ("" when absent).
func statsNextQuery(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, l := range resp.Header.Values("Link") {
		if !strings.Contains(l, `rel="next"`) {
			continue
		}
		start, end := strings.Index(l, "<"), strings.Index(l, ">")
		require.True(t, start >= 0 && end > start)
		link := l[start+1 : end]
		require.True(t, strings.HasPrefix(link, "./stats?"), "stats links use the ./stats?query form: %s", link)
		return strings.TrimPrefix(link, "./stats?")
	}
	return ""
}

func TestStatsQueryAggregationAndPagination(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	postStatsFixtures(t, ts, []map[string]any{
		statsFixtureDoc("2025-02-03:15:03", 50, 5000, 20, 2000),
		statsFixtureDoc("2025-02-03:15:04", 60, 6000, 10, 1000),
		statsFixtureDoc("2025-02-03:15:05", 70, 7000, 40, 4000),
		statsFixtureDoc("2025-03-03:15:07", 15, 4000, 33, 3000),
	})

	// intervalId bounds, forwards: the three February minutes, schema
	// fields populated, legacy counters rolled up under .all.
	items, _ := getStats(t, ts, "start=2025-02-03:15:03&end=2025-02-03:15:05&direction=forwards")
	require.Len(t, items, 3)
	var inbound, outbound float64
	for _, it := range items {
		require.Equal(t, "minute", it.Unit)
		require.NotEmpty(t, it.Schema)
		require.NotEmpty(t, it.AppID)
		require.NotEmpty(t, it.InProgress)
		inbound += it.Entries["messages.inbound.all.messages.count"]
		outbound += it.Entries["messages.outbound.all.messages.count"]
	}
	require.Equal(t, float64(50+60+70), inbound)
	require.Equal(t, float64(20+10+40), outbound)
	require.Equal(t, "2025-02-03:15:03", items[0].IntervalID)
	require.Equal(t, float64(50), items[0].Entries["messages.inbound.realtime.messages.count"], "per-protocol counter preserved")

	// Epoch-millisecond bounds select the same range.
	startMS := time.Date(2025, 2, 3, 15, 3, 0, 0, time.UTC).UnixMilli()
	endMS := time.Date(2025, 3, 3, 15, 6, 0, 0, time.UTC).UnixMilli()
	items, _ = getStats(t, ts, fmt.Sprintf("start=%d&end=%d&direction=forwards", startMS, endMS))
	require.Len(t, items, 3)

	// Hour aggregation folds the three minutes into one bucket.
	items, _ = getStats(t, ts, "start=2025-02-03:15&end=2025-02-03:18&direction=forwards&by=hour")
	require.Len(t, items, 1)
	require.Equal(t, "hour", items[0].Unit)
	require.Equal(t, "2025-02-03:15", items[0].IntervalID)
	require.Equal(t, float64(180), items[0].Entries["messages.inbound.all.messages.count"])

	// Backwards + limit: an inclusive end of 15:04 serves that minute first.
	items, _ = getStats(t, ts, "end=2025-02-03:15:04&direction=backwards&limit=1")
	require.Len(t, items, 1)
	require.Equal(t, "2025-02-03:15:04", items[0].IntervalID)

	// Backwards pagination walks 15:05 → 15:04 → 15:03 via Link next.
	items, resp := getStats(t, ts, "end=2025-02-03:15:05&direction=backwards&limit=1")
	require.Len(t, items, 1)
	require.Equal(t, float64(7000), items[0].Entries["messages.inbound.all.messages.data"])
	next := statsNextQuery(t, resp)
	require.NotEmpty(t, next)
	items, resp = getStats(t, ts, next)
	require.Len(t, items, 1)
	require.Equal(t, float64(6000), items[0].Entries["messages.inbound.all.messages.data"])
	next = statsNextQuery(t, resp)
	require.NotEmpty(t, next)
	items, resp = getStats(t, ts, next)
	require.Len(t, items, 1)
	require.Equal(t, float64(5000), items[0].Entries["messages.inbound.all.messages.data"])
	require.Empty(t, statsNextQuery(t, resp), "last page carries no next link")

	// Forwards pagination from the same bound starts at 15:03.
	items, resp = getStats(t, ts, "end=2025-02-03:15:05&direction=forwards&limit=1")
	require.Len(t, items, 1)
	require.Equal(t, float64(5000), items[0].Entries["messages.inbound.all.messages.data"])
	next = statsNextQuery(t, resp)
	require.NotEmpty(t, next)
	items, _ = getStats(t, ts, next)
	require.Equal(t, float64(6000), items[0].Entries["messages.inbound.all.messages.data"])
}

func TestStatsValidationAndAuth(t *testing.T) {
	t.Parallel()
	ts := newRealtimeServer(t)

	// Unauthenticated requests are refused.
	req, err := http.NewRequest("GET", ts.srv.URL+"/stats", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Bad fixture and parameter input is a 400, not a panic.
	resp = restRequest(t, ts, "POST", "/stats", []byte(`{"not":"an array"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp = restRequest(t, ts, "POST", "/stats", []byte(`[{"intervalId":"bogus"}]`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp = restRequest(t, ts, "GET", "/stats?start=notatime", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp = restRequest(t, ts, "GET", "/stats?by=fortnight", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp = restRequest(t, ts, "GET", "/stats?limit=0", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// An empty store yields an empty page (fresh-app behaviour).
	items, _ := getStats(t, ts, "")
	require.Empty(t, items)
}

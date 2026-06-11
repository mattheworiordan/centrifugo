package ably

// App statistics (RSC6, TS12) — the surface the pinned ably-js
// rest/stats suite exercises. Two endpoints:
//
//   POST /stats — sandbox-style fixture injection (the ably-js harness's
//     createStatsFixtureData): an array of records keyed by intervalId
//     ("YYYY-MM-DD:HH:MM", minute granularity) carrying nested counters
//     in the legacy shape (inbound.realtime.messages.count, ...). The
//     real sandbox accepts the same document; production Ably has no
//     such endpoint (stats come from metering).
//   GET /stats — RSC6b query: start/end accept epoch milliseconds or
//     intervalId strings at month/day/hour/minute granularity (both
//     bounds inclusive of the named interval), direction, limit, and
//     unit aggregation ("by" or "unit" param): stored minute records are
//     bucketed per unit interval and their entries summed numerically.
//
// Records are served in the schema-style stats form the SDK exposes
// (TS12a/TS12r-t): a flat "entries" map of dotted counters, with
// inbound/outbound counters mirrored under messages.<dir>.<proto>.* and
// rolled up into messages.<dir>.all.* — the keys the suite sums.
//
// The store is in-memory and per-node like every other PoC store.

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
)

// statsSchema is advisory (TS12r asserts it is a string); the value
// mirrors the real service's app-stats schema URL.
const statsSchema = "https://schemas.ably.com/json/app-stats-0.0.1.json"

// statsRecord is one stored fixture record at minute granularity.
type statsRecord struct {
	start   int64 // interval start, ms since epoch UTC
	entries map[string]float64
}

type statsStore struct {
	mu      sync.Mutex
	records []statsRecord
}

func newStatsStore() *statsStore { return &statsStore{} }

func (s *statsStore) add(rec statsRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
}

func (s *statsStore) snapshot() []statsRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]statsRecord, len(s.records))
	copy(out, s.records)
	return out
}

// --- interval ids ---

// intervalLayouts maps a stats unit to its intervalId layout (TS12c
// granularities; all UTC).
var intervalLayouts = map[string]string{
	"minute": "2006-01-02:15:04",
	"hour":   "2006-01-02:15",
	"day":    "2006-01-02",
	"month":  "2006-01",
}

// parseIntervalID parses an intervalId string at any supported
// granularity, returning the interval's start.
func parseIntervalID(s string) (time.Time, string, bool) {
	for _, unit := range []string{"minute", "hour", "day", "month"} {
		if t, err := time.ParseInLocation(intervalLayouts[unit], s, time.UTC); err == nil {
			return t, unit, true
		}
	}
	return time.Time{}, "", false
}

// bucketStart truncates an instant to the start of its unit interval.
func bucketStart(ms int64, unit string) time.Time {
	t := time.UnixMilli(ms).UTC()
	switch unit {
	case "hour":
		return t.Truncate(time.Hour)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default: // minute
		return t.Truncate(time.Minute)
	}
}

// bucketEnd returns the exclusive end of the unit interval starting at t.
func bucketEnd(t time.Time, unit string) time.Time {
	switch unit {
	case "hour":
		return t.Add(time.Hour)
	case "day":
		return t.AddDate(0, 0, 1)
	case "month":
		return t.AddDate(0, 1, 0)
	default:
		return t.Add(time.Minute)
	}
}

// parseStatsBound parses a start/end query value: epoch milliseconds or
// an intervalId string. For an intervalId the bound is inclusive of the
// whole named interval, so an end bound resolves to the interval's last
// millisecond while a start bound resolves to its first.
func parseStatsBound(raw string, isEnd bool) (int64, bool) {
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return ms, true
	}
	t, unit, ok := parseIntervalID(raw)
	if !ok {
		return 0, false
	}
	if isEnd {
		return bucketEnd(t, unit).UnixMilli() - 1, true
	}
	return t.UnixMilli(), true
}

// --- fixture flattening ---

func statNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

// flattenStats walks a fixture document into dotted numeric leaves
// ("inbound.realtime.messages.count": 50).
func flattenStats(prefix string, v any, out map[string]float64) {
	if n, ok := statNumber(v); ok {
		out[prefix] += n
		return
	}
	m, ok := v.(map[string]any)
	if !ok {
		return // non-numeric scalars (and the intervalId we never pass) are ignored
	}
	for k, child := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		flattenStats(key, child, out)
	}
}

// entriesFromFixture converts legacy-shape dotted leaves to the schema
// entry keys the SDK exposes: "inbound.<proto>.<rest>" becomes
// "messages.inbound.<proto>.<rest>" and accumulates into
// "messages.inbound.all.<rest>" (same for outbound); every other leaf
// keeps its dotted path.
func entriesFromFixture(flat map[string]float64) map[string]float64 {
	entries := make(map[string]float64, len(flat)*2)
	for key, n := range flat {
		dir, rest, found := strings.Cut(key, ".")
		if !found || (dir != "inbound" && dir != "outbound") {
			entries[key] += n
			continue
		}
		entries["messages."+key] += n
		if proto, tail, ok := strings.Cut(rest, "."); ok && proto != "all" {
			entries["messages."+dir+".all."+tail] += n
		}
	}
	return entries
}

// --- HTTP surface ---

// serveStatsFixtures implements POST /stats: sandbox-style fixture
// injection used by the ably-js test harness before rest/stats runs.
func (h *Handler) serveStatsFixtures(rw http.ResponseWriter, r *http.Request) {
	if _, authErr := h.authenticate(r); authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxMessageSize+1))
	if err != nil || len(body) > maxMessageSize {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid request body")
		return
	}
	var docs []map[string]any
	if err := protocol.UnmarshalAny(body, requestBodyFormat(r), &docs); err != nil || len(docs) == 0 {
		h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid stats fixture body: expected a non-empty array of records")
		return
	}
	records := make([]statsRecord, 0, len(docs))
	for _, doc := range docs {
		id, _ := doc["intervalId"].(string)
		start, _, ok := parseIntervalID(id)
		if !ok {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid stats record intervalId")
			return
		}
		flat := make(map[string]float64)
		for k, v := range doc {
			if k == "intervalId" {
				continue
			}
			flattenStats(k, v, flat)
		}
		records = append(records, statsRecord{
			start:   start.UnixMilli(),
			entries: entriesFromFixture(flat),
		})
	}
	for _, rec := range records {
		h.stats.add(rec)
	}
	h.writeDocument(rw, r, http.StatusCreated, []any{})
}

// serveStats implements GET /stats (RSC6): unit-bucketed records in a
// paginated page, newest first unless direction=forwards.
func (h *Handler) serveStats(rw http.ResponseWriter, r *http.Request) {
	if _, authErr := h.authenticate(r); authErr != nil {
		h.writeError(rw, r, authErr.statusCode, authErr.code, authErr.message)
		return
	}
	q := r.URL.Query()

	unit := q.Get("by")
	if unit == "" {
		unit = q.Get("unit")
	}
	if _, ok := intervalLayouts[unit]; !ok {
		if unit != "" {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid stats unit")
			return
		}
		unit = "minute"
	}

	now := time.Now().UnixMilli()
	start, end := int64(0), now
	if raw := q.Get("start"); raw != "" {
		ms, ok := parseStatsBound(raw, false)
		if !ok {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid start parameter")
			return
		}
		start = ms
	}
	if raw := q.Get("end"); raw != "" {
		ms, ok := parseStatsBound(raw, true)
		if !ok {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid end parameter")
			return
		}
		end = ms
	}
	backwards := q.Get("direction") != "forwards"
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 1000 {
			h.writeError(rw, r, http.StatusBadRequest, errCodeBadRequest, "invalid limit parameter")
			return
		}
		limit = n
	}

	// Aggregate stored minute records into unit buckets, then keep the
	// buckets whose interval overlaps [start, end].
	type bucket struct {
		start   time.Time
		entries map[string]float64
	}
	buckets := map[int64]*bucket{}
	for _, rec := range h.stats.snapshot() {
		bs := bucketStart(rec.start, unit)
		be := bucketEnd(bs, unit).UnixMilli() - 1
		if bs.UnixMilli() > end || be < start {
			continue
		}
		b := buckets[bs.UnixMilli()]
		if b == nil {
			b = &bucket{start: bs, entries: map[string]float64{}}
			buckets[bs.UnixMilli()] = b
		}
		for k, n := range rec.entries {
			b.entries[k] += n
		}
	}
	ordered := make([]*bucket, 0, len(buckets))
	for _, b := range buckets {
		ordered = append(ordered, b)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if backwards {
			return ordered[i].start.After(ordered[j].start)
		}
		return ordered[i].start.Before(ordered[j].start)
	})

	page := ordered
	hasNext := len(ordered) > limit
	if hasNext {
		page = ordered[:limit]
	}

	items := make([]any, 0, len(page))
	for _, b := range page {
		items = append(items, map[string]any{
			"intervalId": b.start.Format(intervalLayouts[unit]),
			"unit":       unit,
			"schema":     statsSchema,
			"appId":      "poc",
			// inProgress marks the last sub-interval folded into a still-
			// open bucket; fixture data is historical, so the bucket start
			// stands in (TS12s only requires a string).
			"inProgress": b.start.Format(intervalLayouts["minute"]),
			"entries":    b.entries,
		})
	}

	// Link pagination, same contract as writeHistoryPage: ably-js only
	// accepts `./<word>?<query>` and reuses just the QUERY on the
	// resource's own path. Page state lives in the start/end bounds: the
	// next page tightens the bound past the last bucket served.
	direction := "forwards"
	if backwards {
		direction = "backwards"
	}
	first := fmt.Sprintf("./stats?limit=%d&direction=%s&by=%s&start=%d&end=%d",
		limit, direction, url.QueryEscape(unit), start, end)
	links := []string{fmt.Sprintf("<%s>; rel=\"first\"", first)}
	if hasNext {
		last := page[len(page)-1]
		nextStart, nextEnd := start, end
		if backwards {
			nextEnd = last.start.UnixMilli() - 1
		} else {
			nextStart = bucketEnd(last.start, unit).UnixMilli()
		}
		next := fmt.Sprintf("./stats?limit=%d&direction=%s&by=%s&start=%d&end=%d",
			limit, direction, url.QueryEscape(unit), nextStart, nextEnd)
		links = append(links, fmt.Sprintf("<%s>; rel=\"next\"", next))
	}
	for _, l := range links {
		rw.Header().Add("Link", l)
	}
	h.writeDocument(rw, r, http.StatusOK, items)
}

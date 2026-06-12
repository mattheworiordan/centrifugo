package ably

// P6.4 — cross-node comet forwarding (research/14-multinode-comet.md).
//
// Comet per-key state (the cometConn buffer/waiter, the live session, the
// key→session index) is in-process on the node that served /comet/connect.
// Multi-node, a per-key request can land on ANY node; rather than sharing
// the state in Redis (impossible for the live session, and the shareable
// half still needs an inbound path) or pinning at the LB (inexpressible on
// fly.io), the wrong node FORWARDS the request to the owning node over the
// broker's control channel — the exact mechanism centrifuge's own emulation
// layer uses to make its unidirectional HTTP transports multi-node
// (emulation.go: a targeted node.Survey carrying the client command).
//
// The owning node id rides the connectionKey:
// "<connectionId>!<centrifugeId>.<nodeId>". The suffix is invisible to
// every other consumer — TM2h attribution and the recover param read only
// up to the first '!', the registry treats keys as opaque strings, and the
// SDK echoes the key verbatim into per-key paths. A lookupKey miss whose
// key names a LIVE foreign node becomes a survey; a dead, unknown, or
// self-referencing owner keeps today's 410 contract (the SDK reconnects
// fresh and resumes — cross-node resume is pinned by
// TestMultiNodeResumeCrossNode_P6_5). Node restarts therefore self-heal:
// the restarted node has a new id, the stale key's owner is no longer a
// cluster member, and the very first per-key request is an immediate 410.
//
// A1 is preserved cross-node: the receiving node authenticates the HTTP
// request (the keys config is shared cluster-wide) and forwards the
// RESOLVED identity; the owning node runs the same ownsSession bind against
// its sessionRecord. The control channel is intra-cluster — the same trust
// boundary that already carries centrifuge emulation frames.
//
// Wiring: OnSurvey is a single slot per node. The test harness (and any
// standalone embedding) claims it via RegisterCometSurvey. The production
// app's slot is owned by survey.NewCaller (app/run.go) — muxing these ops
// into it is the documented follow-up (research/14-multinode-comet.md §8);
// until then a deployment behaves exactly as before this change (the
// survey fails → 410, the pre-existing churn, no regression).

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"

	"github.com/centrifugal/centrifuge"
)

// Survey ops for the three forwarded per-key request families. /connect is
// never forwarded — it creates the session on whichever node receives it.
const (
	cometSurveyRecvOp  = "ably_comet_recv"
	cometSurveySendOp  = "ably_comet_send"
	cometSurveyCloseOp = "ably_comet_close"
)

// Survey reply codes for the comet forward ops.
const (
	cometForwardOK         uint32 = 0
	cometForwardGone       uint32 = 1 // no live comet session for the key, or caller is not its owner
	cometForwardBadRequest uint32 = 2 // malformed payload (send body that is not a frame array)
)

const (
	// defaultForwardRecvTimeout bounds a forwarded recv end to end; it must
	// exceed forwardRecvPark so the survey outlives the remote park.
	defaultForwardRecvTimeout = 15 * time.Second
	// defaultForwardOpTimeout bounds forwarded send/close surveys — quick
	// feed-and-ack operations.
	defaultForwardOpTimeout = 5 * time.Second
	// forwardRecvPark is the remote park window: above heartbeatInterval so
	// an idle park always returns by the next heartbeat frame (never an
	// empty-poll busy loop), under the survey timeout so the reply makes it
	// back.
	forwardRecvPark = 12 * time.Second
)

// cometForwardReq is the survey payload for all three ops.
type cometForwardReq struct {
	Key      string `json:"k"`
	KeyName  string `json:"kn"`          // resolved caller identity (A1) …
	ClientID string `json:"c,omitempty"` // … enforced by the owning node
	WaitMS   int64  `json:"w,omitempty"` // recv: park window
	Body     []byte `json:"b,omitempty"` // send: the raw frame-array body
	Action   int    `json:"a,omitempty"` // close: ActionClose|ActionDisconnect
}

// cometKeyNode extracts the owning node id from a connectionKey of the
// shape "<connectionId>!<centrifugeId>.<nodeId>". Empty when the key
// carries no node suffix (malformed or pre-suffix key).
func cometKeyNode(key string) string {
	_, token, ok := strings.Cut(key, "!")
	if !ok {
		return ""
	}
	_, nodeID, ok := strings.Cut(token, ".")
	if !ok {
		return ""
	}
	return nodeID
}

// cometForwardTarget resolves the live FOREIGN node owning key. ok is false
// when forwarding does not apply — the key is self-owned (a genuine local
// miss → dead session), malformed, or its owner is not a live cluster
// member (restarted/stopped node) — and the caller keeps today's 410/204
// contract.
func (h *Handler) cometForwardTarget(key string) (string, bool) {
	nodeID := cometKeyNode(key)
	if nodeID == "" || nodeID == h.node.ID() {
		return "", false
	}
	info, err := h.node.Info()
	if err != nil {
		return "", false
	}
	for _, n := range info.Nodes {
		if n.UID == nodeID {
			return nodeID, true
		}
	}
	return "", false
}

// forwardComet sends one comet survey to the owning node and returns its
// reply. Any survey-level failure (timeout, node vanished, no reply, the
// peer's survey slot unwired) reports GONE: this node genuinely cannot
// reach the session, and 410 is the contract the SDK recovers from
// (reconnect + resume).
func (h *Handler) forwardComet(ctx context.Context, op, nodeID string, req cometForwardReq, timeout time.Duration) (uint32, []byte) {
	data, err := json.Marshal(req)
	if err != nil {
		return cometForwardGone, nil
	}
	sctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	results, err := h.node.Survey(sctx, op, data, nodeID)
	if err != nil {
		return cometForwardGone, nil
	}
	res, ok := results[nodeID]
	if !ok {
		return cometForwardGone, nil
	}
	return res.Code, res.Data
}

// RegisterCometSurvey claims the node's OnSurvey slot with the comet
// forwarding dispatcher. For the test harness and standalone embeddings
// ONLY: the production app's slot is owned by survey.NewCaller — these ops
// must be muxed into that handler instead (the documented follow-up).
func (h *Handler) RegisterCometSurvey() {
	h.node.OnSurvey(func(e centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		if !h.HandleCometSurvey(e, cb) {
			cb(centrifuge.SurveyReply{Code: cometForwardBadRequest})
		}
	})
}

// HandleCometSurvey dispatches one comet forward op, reporting false for
// ops it does not own so a wrapping survey handler can fall through. The
// work runs on a fresh goroutine — survey handlers execute on the
// control-plane goroutine and a forwarded recv parks for up to WaitMS;
// blocking there would stall every other cross-node operation (the same
// discipline as centrifuge's emulationSurveyHandler).
func (h *Handler) HandleCometSurvey(e centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) bool {
	switch e.Op {
	case cometSurveyRecvOp, cometSurveySendOp, cometSurveyCloseOp:
	default:
		return false
	}
	var req cometForwardReq
	if err := json.Unmarshal(e.Data, &req); err != nil {
		cb(centrifuge.SurveyReply{Code: cometForwardBadRequest})
		return true
	}
	op := e.Op
	go func() {
		code, data := h.serveCometForward(op, req)
		cb(centrifuge.SurveyReply{Code: code, Data: data})
	}()
	return true
}

// serveCometForward executes a forwarded per-key op against the local
// registry — the owning-node half of the comet HTTP handlers, minus HTTP.
// The identity bind is the SAME ownsSession check the local paths run; a
// non-owner is indistinguishable from a dead key (no liveness oracle, A1).
func (h *Handler) serveCometForward(op string, req cometForwardReq) (uint32, []byte) {
	sess := h.registry.lookupKey(req.Key)
	if sess == nil {
		return cometForwardGone, nil
	}
	rec, ok := h.registry.recordFor(sess)
	if !ok || !ownsSession(authResult{keyName: req.KeyName, clientID: req.ClientID}, rec) {
		return cometForwardGone, nil
	}
	cc, ok := sess.conn.(*cometConn)
	if !ok {
		return cometForwardGone, nil
	}
	switch op {
	case cometSurveyRecvOp:
		wait := time.Duration(req.WaitMS) * time.Millisecond
		if wait <= 0 || wait > forwardRecvPark {
			wait = forwardRecvPark
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		return cometForwardOK, encodeCometBatch(cc.parkRecv(ctx))
	case cometSurveySendOp:
		var frames []*protocol.ProtocolMessage
		if err := json.Unmarshal(req.Body, &frames); err != nil {
			return cometForwardBadRequest, nil
		}
		// One second under the forwarder's survey timeout: a feed that
		// unblocks at the limit on a wedged session must not apply frames
		// AFTER the forwarder has already reported 410.
		ctx, cancel := context.WithTimeout(context.Background(), defaultForwardOpTimeout-time.Second)
		defer cancel()
		for _, m := range frames {
			if m == nil {
				continue
			}
			if err := cc.feed(ctx, m); err != nil {
				return cometForwardGone, nil
			}
		}
		return cometForwardOK, nil
	case cometSurveyCloseOp:
		action := protocol.Action(req.Action)
		if action != protocol.ActionClose && action != protocol.ActionDisconnect {
			return cometForwardBadRequest, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), cometCloseInjectTimeout)
		defer cancel()
		_ = cc.feed(ctx, &protocol.ProtocolMessage{Action: action})
		return cometForwardOK, nil
	}
	return cometForwardBadRequest, nil
}

// encodeCometBatch renders a frame batch exactly as writeCometBatch puts it
// on the wire ("[" frames "]\n"); nil for an empty batch so the forwarding
// node answers 204, preserving the empty-poll contract.
func encodeCometBatch(frames [][]byte) []byte {
	if len(frames) == 0 {
		return nil
	}
	var buf bytes.Buffer
	buf.Grow(2 + len(frames))
	buf.WriteByte('[')
	buf.Write(bytes.Join(frames, []byte(",")))
	buf.WriteString("]\n")
	return buf.Bytes()
}

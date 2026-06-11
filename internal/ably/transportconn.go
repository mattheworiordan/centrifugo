package ably

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/centrifugal/centrifugo/v6/internal/websocket"
)

// frameConn is the session's transport: a connection that carries Ably
// ProtocolMessage frames. The session is transport-agnostic — every
// frame handler, the centrifuge bridge, delivery, token expiry and the
// close machinery work against this interface; WebSocket (wsConn) is
// one implementation, the comet/HTTP-fallback front is another.
//
// Contract:
//   - readFrame blocks for the next inbound frame; any error ends the
//     session (the run loop returns and teardown runs). Implementations
//     own wire decoding — readFrame returns decoded frames so a comet
//     /send carrying an ARRAY of messages can feed them one at a time.
//   - writeEncoded takes one pre-encoded frame (the session encodes;
//     formats differ per frame shape — see writeWire). It is always
//     called under the session's writeMu, so implementations need no
//     additional ordering; they own their deadline/buffering policy.
//     binary reports msgpack framing (WS picks the frame type from it).
//   - close is idempotent and unblocks a pending readFrame.
type frameConn interface {
	readFrame() (*protocol.ProtocolMessage, error)
	writeEncoded(encoded []byte, binary bool) error
	close() error
}

// wsConn adapts *websocket.Conn to frameConn with the adapter's
// long-standing WS discipline: reads are format-tolerant (decode by the
// session's negotiated format regardless of the WS frame-type flag),
// writes carry the matching frame type under a write deadline.
type wsConn struct {
	conn   *websocket.Conn
	format protocol.Format
}

func (w *wsConn) readFrame() (*protocol.ProtocolMessage, error) {
	for {
		_, data, err := w.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		var m protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, w.format, &m); err != nil {
			log.Warn().Err(err).Str("transport", transportName).Str("format", w.format.String()).Msg("bad inbound frame")
			continue
		}
		return &m, nil
	}
}

func (w *wsConn) writeEncoded(encoded []byte, binary bool) error {
	messageType := websocket.TextMessage
	if binary {
		messageType = websocket.BinaryMessage
	}
	_ = w.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return w.conn.WriteMessage(messageType, encoded)
}

func (w *wsConn) close() error { return w.conn.Close() }

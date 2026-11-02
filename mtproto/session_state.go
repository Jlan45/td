package mtproto

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"github.com/gotd/log"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
)

// SessionState is MTProto session state (session_id and content message
// counter) shared by successive connections of one logical client session.
//
// Quote: "The session in conjunction with the key identifier corresponds to an
// application instance."
// Link: https://core.telegram.org/mtproto/description#session
//
// Official clients (TDLib Session, Telegram Desktop SessionPrivate, Telegram
// Android Connection) keep one session_id per auth key for the lifetime of the
// session and only move it to a new TCP connection on reconnect or proxy change.
// Starting a new session_id on every reconnect makes the server see a second
// main session on the same authorization while the previous TCP connection may
// still be alive on its side (up to ping_delay_disconnect), which triggers
// AUTH_KEY_DUPLICATED and permanently revokes the key.
// Link: https://core.telegram.org/api/errors#406-not-acceptable
//
// A new session is started only when the auth key changes or the server
// reports inconsistent msg_seqno (bad_msg_notification 32/33), like Telegram
// Desktop's resetSession.
//
// SessionState must be shared only between connections that are never used
// concurrently (successive primary connections). Zero value is ready to use.
type SessionState struct {
	mux   sync.Mutex
	keyID [8]byte
	id    int64
	// sentContentMessages is count of content messages sent in this session,
	// used to compute msg_seqno.
	sentContentMessages int32
	// received stores msg_id of last messages received in this session (replay
	// protection). On resume the server may resend messages that were not
	// acknowledged on the previous connection; they must not be handled twice.
	received *proto.MessageIDBuf
}

// receivedBufSize is the number of last received msg_id values to store.
const receivedBufSize = 100

// NewSessionState returns empty session state.
func NewSessionState() *SessionState {
	return &SessionState{}
}

// resume returns session id of the session started for the given key, if any.
func (s *SessionState) resume(key crypto.AuthKey) (int64, bool) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.id == 0 || s.keyID != key.ID {
		return 0, false
	}
	return s.id, true
}

// start begins new session with given id for the given key.
func (s *SessionState) start(key crypto.AuthKey, id int64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.keyID = key.ID
	s.id = id
	s.sentContentMessages = 0
	s.received = proto.NewMessageIDBuf(receivedBufSize)
}

// receivedBuf returns received msg_id buffer of the current session, nil if
// there is no session.
func (s *SessionState) receivedBuf() *proto.MessageIDBuf {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.received
}

// reset forgets current session, so the next connection starts a new one.
func (s *SessionState) reset() {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.id = 0
	s.sentContentMessages = 0
	s.received = nil
}

// ID returns current session id or zero if there is no session.
func (s *SessionState) ID() int64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.id
}

// next generates message id and sequence number. Both are computed under one
// lock: msg_id1 < msg_id2 must imply seqno1 <= seqno2 within a session.
func (s *SessionState) next(newID func(t proto.MessageType) int64, content bool) (msgID int64, seqNo int32) {
	s.mux.Lock()
	defer s.mux.Unlock()

	msgID = newID(proto.MessageFromClient)
	seqNo = s.sentContentMessages * 2
	if content {
		seqNo++
		s.sentContentMessages++
	}
	return msgID, seqNo
}

// ErrSessionReset is returned by Conn.Run when the server reported that
// msg_seqno of the current session is inconsistent. The session is dropped, so
// the next connection starts a new one.
var ErrSessionReset = errors.New("mtproto session reset")

// state returns session state of the connection.
func (c *Conn) state() *SessionState {
	c.stateOnce.Do(func() {
		if c.sessState == nil {
			c.sessState = NewSessionState()
		}
	})
	return c.sessState
}

// receivedIDs returns replay protection buffer: the one of the current session
// (kept across resumed connections) or the connection's own as fallback.
func (c *Conn) receivedIDs() MessageBuf {
	if b := c.state().receivedBuf(); b != nil {
		return b
	}
	return c.messageIDBuf
}

// requestSessionReset forgets the current session and asks Run to restart the
// connection.
func (c *Conn) requestSessionReset(code int) {
	c.state().reset()
	c.log.Warn(context.Background(), "Resetting session after bad msg_seqno", log.Int("code", code))
	select {
	case c.sessionResetCh <- struct{}{}:
	default:
	}
}

func (c *Conn) sessionResetLoop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.sessionResetCh:
		return ErrSessionReset
	}
}

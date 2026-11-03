package mtproto

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/rpc"
)

// Requests of a shared session (Options.Session) outlive the connection they
// were sent on, like in official clients (TDLib Session::sent_queries_,
// Telegram Desktop SessionData::haveSent, Telegram Android runningRequests):
//
//   - request acknowledged by server before the connection was closed keeps
//     waiting for the result, which the server sends to the resumed session;
//   - request that was not acknowledged keeps waiting until its state is
//     queried with msgs_state_req on the next connection of the session, and
//     is resent only if the server did not receive it;
//   - when the session ends (new auth key, session reset, close), requests
//     that may have been processed fail and others are resent by caller.
//
// Such requests are called orphans below: rpc.Engine of the closed connection
// no longer tracks them. Invoke does not block on them (the call may come from
// the connection callback itself, which must return before the client can
// reconnect): it returns an error that AwaitSession waits on. The result is
// stored as raw bytes and decoded by the waiter, so a caller that does not
// wait never has its output written concurrently.

var (
	// errNotProcessed means that the request was not processed by server and
	// is safe to resend with a new msg_id, possibly on a new connection
	// (rpc.ErrEngineClosed).
	errNotProcessed = errors.Wrap(rpc.ErrEngineClosed, "request was not processed by server")
	// errAnswerLost means that the request was acknowledged by server but the
	// session ended before the answer: it may have been processed, so it must
	// not be resent transparently.
	errAnswerLost = errors.Wrap(rpc.ErrClosedAfterAck, "session ended before answer")
	// errResendInSession means that the request sent on the current connection
	// was not processed by server and must be resent with a new msg_id, e.g.
	// it was sent before first_msg_id of new_session_created.
	errResendInSession = errors.New("request must be resent")
)

// pendingRequest is a request of shared session.
type pendingRequest struct {
	msgID int64
	// gen is generation of the session the request was sent in.
	gen uint64
	// acked is set when server acknowledged the request.
	acked bool
	// orphan is set when connection of the request was closed.
	orphan bool
	// queriedBy is connection that sent msgs_state_req for the request.
	queriedBy *Conn

	done chan struct{}
	// raw is the result, valid if err is nil after done is closed.
	raw []byte
	err error
}

// orphanError is returned by Invoke for request whose connection was closed
// before the result was received but which may still complete in the
// resumed session, see AwaitSession.
type orphanError struct {
	cause  error
	s      *SessionState
	p      *pendingRequest
	output bin.Decoder
}

func (e *orphanError) Error() string { return e.cause.Error() }
func (e *orphanError) Unwrap() error { return e.cause }

// AwaitSession waits for the result of a request whose connection was closed,
// if the request can still complete in the resumed MTProto session (see
// Options.Session). Otherwise err is returned as is.
//
// The returned error is:
//   - nil, if the result was received and decoded into the request output;
//   - error wrapping rpc.ErrEngineClosed, if the server did not process the
//     request, so it is safe to resend it on a new connection;
//   - error wrapping rpc.ErrClosedAfterAck, if the session ended after the
//     server acknowledged the request: it may have been processed;
//   - RPC error or context error.
func AwaitSession(ctx context.Context, err error) error {
	var orphan *orphanError
	if !errors.As(err, &orphan) {
		return err
	}
	p := orphan.p
	select {
	case <-p.done:
		if p.err != nil {
			return p.err
		}
		return orphan.output.Decode(&bin.Buffer{Buf: p.raw})
	case <-ctx.Done():
		orphan.s.untrack(p)
		return ctx.Err()
	}
}

func (s *SessionState) initRequests() {
	if s.requests == nil {
		s.requests = map[int64]*pendingRequest{}
		s.stateQueries = map[int64][]int64{}
		s.orphaned = make(chan struct{}, 1)
	}
}

// track registers request of the current session before it is sent.
func (s *SessionState) track(msgID int64) *pendingRequest {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.initRequests()
	p := &pendingRequest{
		msgID: msgID,
		gen:   s.gen,
		done:  make(chan struct{}),
	}
	s.requests[msgID] = p
	return p
}

// untrack removes request.
func (s *SessionState) untrack(p *pendingRequest) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.requests[p.msgID] == p {
		delete(s.requests, p.msgID)
	}
}

// resolve completes the request. Caller must hold mux.
func (s *SessionState) resolve(p *pendingRequest, err error) {
	select {
	case <-p.done:
		return
	default:
	}
	p.err = err
	close(p.done)
	if s.requests[p.msgID] == p {
		delete(s.requests, p.msgID)
	}
}

// orphan marks request as orphan after its connection was closed. If the
// session ended, the request is completed with the final error.
func (s *SessionState) orphan(p *pendingRequest, acked bool) {
	s.mux.Lock()
	defer s.mux.Unlock()
	select {
	case <-p.done:
		return
	default:
	}
	p.acked = p.acked || acked
	if s.closed || p.gen != s.gen || s.id == 0 {
		// Session ended: it will never be resumed.
		s.resolve(p, endErr(p))
		return
	}
	p.orphan = true
	select {
	case s.orphaned <- struct{}{}:
	default:
	}
}

func endErr(p *pendingRequest) error {
	if p.acked {
		return errAnswerLost
	}
	return errNotProcessed
}

// endRequests resolves orphans of the ended session. Caller must hold mux.
func (s *SessionState) endRequests() {
	s.gen++
	for _, p := range s.requests {
		if p.orphan {
			s.resolve(p, endErr(p))
		}
	}
	for id := range s.stateQueries {
		delete(s.stateQueries, id)
	}
}

// Close ends the session: it can not be resumed anymore and requests waiting
// for it fail. Should be called when the client is closed.
func (s *SessionState) Close() {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	s.endRequests()
}

// orphanFor returns orphan request with given msg_id. Caller must hold mux.
func (s *SessionState) orphanFor(msgID int64) (*pendingRequest, bool) {
	p, ok := s.requests[msgID]
	if !ok || !p.orphan {
		return nil, false
	}
	return p, true
}

// deliverResult decodes result of orphan request. Returns false if there is
// no such request.
func (s *SessionState) deliverResult(msgID int64, b *bin.Buffer) bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	p, ok := s.orphanFor(msgID)
	if !ok {
		return false
	}
	p.raw = append([]byte(nil), b.Buf...)
	s.resolve(p, nil)
	return true
}

// deliverError completes orphan request with error. Returns false if there is
// no such request.
func (s *SessionState) deliverError(msgID int64, err error) bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	p, ok := s.orphanFor(msgID)
	if !ok {
		return false
	}
	s.resolve(p, err)
	return true
}

// notProcessed completes orphan request that was not processed by server.
func (s *SessionState) notProcessed(msgID int64) bool {
	return s.deliverError(msgID, errNotProcessed)
}

// notProcessedBefore completes orphan requests sent before firstMsgID of the
// new session created by server: server never processed them.
func (s *SessionState) notProcessedBefore(firstMsgID int64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for id, p := range s.requests {
		if p.orphan && id < firstMsgID {
			s.resolve(p, errNotProcessed)
		}
	}
}

// ack marks requests as acknowledged by server.
func (s *SessionState) ack(ids []int64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for _, id := range ids {
		if p, ok := s.requests[id]; ok {
			p.acked = true
		}
	}
}

// unqueried returns not acknowledged orphans whose state was not queried on
// connection c, marking them as queried.
func (s *SessionState) unqueried(c *Conn) []int64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	var ids []int64
	for id, p := range s.requests {
		if p.orphan && !p.acked && p.queriedBy != c {
			p.queriedBy = c
			ids = append(ids, id)
		}
	}
	return ids
}

// orphanedCh is signaled when a new orphan appears.
func (s *SessionState) orphanedCh() <-chan struct{} {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.initRequests()
	return s.orphaned
}

// addStateQuery stores msg_ids queried by msgs_state_req with reqID.
func (s *SessionState) addStateQuery(reqID int64, ids []int64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.initRequests()
	s.stateQueries[reqID] = ids
}

// Statuses of msgs_state_info, lower 3 bits.
//
// See https://core.telegram.org/mtproto/service_messages_about_messages#request-for-message-status-information
const (
	msgStateUnknown        = 1
	msgStateNotReceived    = 2
	msgStateNotReceivedLow = 3
	msgStateReceived       = 4
)

// stateInfo applies msgs_state_info for query reqID.
func (s *SessionState) stateInfo(reqID int64, info []byte) {
	s.mux.Lock()
	defer s.mux.Unlock()
	ids, ok := s.stateQueries[reqID]
	if !ok {
		return
	}
	delete(s.stateQueries, reqID)
	for i, id := range ids {
		if i >= len(info) {
			break
		}
		p, ok := s.orphanFor(id)
		if !ok {
			continue
		}
		switch info[i] & 7 {
		case msgStateUnknown, msgStateNotReceived, msgStateNotReceivedLow:
			s.resolve(p, errNotProcessed)
		case msgStateReceived:
			// Server has the request: the result will be sent to the session.
			p.acked = true
		}
	}
}

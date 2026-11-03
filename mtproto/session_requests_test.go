package mtproto

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/log"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/rpc"
	"github.com/gotd/td/transport"
)

// sinkConn is transport that records sent messages.
type sinkConn struct {
	mux  sync.Mutex
	sent []*bin.Buffer
	cond chan struct{}
}

func newSinkConn() *sinkConn { return &sinkConn{cond: make(chan struct{}, 64)} }

func (c *sinkConn) Send(_ context.Context, b *bin.Buffer) error {
	c.mux.Lock()
	c.sent = append(c.sent, &bin.Buffer{Buf: b.Copy()})
	c.mux.Unlock()
	select {
	case c.cond <- struct{}{}:
	default:
	}
	return nil
}

func (c *sinkConn) Recv(ctx context.Context, _ *bin.Buffer) error {
	<-ctx.Done()
	return ctx.Err()
}

func (c *sinkConn) Close() error { return nil }

// sentMessage is decrypted client message.
type sentMessage struct {
	msgID int64
	seqNo int32
	body  *bin.Buffer
}

// next waits for n-th (0-based) sent message and decrypts it.
func (c *sinkConn) next(t *testing.T, key crypto.AuthKey, n int) sentMessage {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		c.mux.Lock()
		if len(c.sent) > n {
			buf := &bin.Buffer{Buf: append([]byte(nil), c.sent[n].Buf...)}
			c.mux.Unlock()
			msg, err := crypto.NewServerCipher(rand.Reader).DecryptFromBuffer(key, buf)
			require.NoError(t, err)
			return sentMessage{msgID: msg.MessageID, seqNo: msg.SeqNo, body: &bin.Buffer{Buf: msg.Data()}}
		}
		c.mux.Unlock()
		select {
		case <-c.cond:
		case <-deadline:
			t.Fatalf("message %d was not sent", n)
		}
	}
}

func newSessionTestConn(t *testing.T, state *SessionState, key crypto.AuthKey, sink *sinkConn) *Conn {
	t.Helper()
	c := New(func(ctx context.Context) (transport.Conn, error) {
		return sink, nil
	}, Options{
		Logger:        log.Nop,
		Random:        rand.Reader,
		Key:           key,
		Session:       state,
		Clock:         clock.System,
		RetryInterval: time.Hour,
	})
	c.ackSendChan = make(chan int64, 16)
	require.NoError(t, c.connect(context.Background()))
	return c
}

func encodeResult(t *testing.T, msgID int64, result bin.Encoder) *proto.Result {
	t.Helper()
	var b bin.Buffer
	require.NoError(t, b.Encode(result))
	return &proto.Result{RequestMessageID: msgID, Result: b.Copy()}
}

// invokeAsync starts get_future_salts invoke and returns its first sent message id.
func invokeAsync(
	t *testing.T, c *Conn, key crypto.AuthKey, sink *sinkConn, out *mt.FutureSalts,
) (msgID int64, result <-chan error) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Invoke(context.Background(), &mt.GetFutureSaltsRequest{Num: 1}, out)
	}()
	return sink.next(t, key, 0).msgID, errCh
}

func awaitAsync(err error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- AwaitSession(context.Background(), err) }()
	return ch
}

func requireWaiting(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("unexpected result: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func receive(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
		return nil
	}
}

// Result of a request acknowledged before the connection was closed is
// delivered by the next connection of the session (H4).
func TestAckedRequestCompletesInResumedSession(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	state := NewSessionState()

	sink1 := newSinkConn()
	c1 := newSessionTestConn(t, state, key, sink1)
	var out mt.FutureSalts
	msgID, errCh := invokeAsync(t, c1, key, sink1, &out)
	c1.rpc.NotifyAcks([]int64{msgID})
	c1.rpc.ForceClose()

	err := receive(t, errCh)
	a.ErrorIs(err, rpc.ErrClosedAfterAck, "legacy callers see acknowledged close")
	awaited := awaitAsync(err)
	requireWaiting(t, awaited)

	c2 := newSessionTestConn(t, state, key, newSinkConn())
	a.True(c2.sessionResumed)
	sessionID := c2.session().ID
	res := encodeResult(t, msgID, &mt.FutureSalts{ReqMsgID: msgID, Now: 7})
	a.NoError(c2.consumeMessage(context.Background(),
		encryptPayloadFromServer(t, key, sessionID, serverIDs(1)[0], 1, res)))

	a.NoError(receive(t, awaited))
	a.Equal(7, out.Now)
}

// State of a request not acknowledged before the connection was closed is
// queried with msgs_state_req before it is resent (M3).
func TestUnackedRequestStateQueried(t *testing.T) {
	for _, tt := range []struct {
		name     string
		state    byte
		resend   bool
		received bool
	}{
		{name: "NotReceived", state: msgStateNotReceived, resend: true},
		{name: "Unknown", state: msgStateUnknown, resend: true},
		{name: "Received", state: msgStateReceived | 8, received: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)
			key := crypto.Key{1}.WithID()
			state := NewSessionState()

			sink1 := newSinkConn()
			c1 := newSessionTestConn(t, state, key, sink1)
			var out mt.FutureSalts
			msgID, errCh := invokeAsync(t, c1, key, sink1, &out)
			c1.rpc.ForceClose()
			err := receive(t, errCh)
			a.ErrorIs(err, rpc.ErrEngineClosed)
			awaited := awaitAsync(err)

			sink2 := newSinkConn()
			c2 := newSessionTestConn(t, state, key, sink2)
			ids := state.unqueried(c2)
			a.Equal([]int64{msgID}, ids)
			a.Empty(state.unqueried(c2), "queried once per connection")
			a.NoError(c2.sendStateRequest(context.Background(), ids))

			sent := sink2.next(t, key, 0)
			a.Equal(int32(1), sent.seqNo&1, "msgs_state_req is content-related")
			var req mt.MsgsStateReq
			a.NoError(req.Decode(sent.body))
			a.Equal([]int64{msgID}, req.MsgIDs)

			var info bin.Buffer
			a.NoError(info.Encode(&mt.MsgsStateInfo{ReqMsgID: sent.msgID, Info: []byte{tt.state}}))
			a.NoError(c2.handleStateInfo(&info))

			if tt.resend {
				a.ErrorIs(receive(t, awaited), rpc.ErrEngineClosed, "not received request is resent by caller")
				return
			}
			requireWaiting(t, awaited)
			res := encodeResult(t, msgID, &mt.FutureSalts{ReqMsgID: msgID, Now: 9})
			a.NoError(c2.consumeMessage(context.Background(),
				encryptPayloadFromServer(t, key, c2.session().ID, serverIDs(1)[0], 1, res)))
			a.NoError(receive(t, awaited))
			a.Equal(9, out.Now)
		})
	}
}

// When the session ends, acknowledged requests fail (they may have been
// processed), others are safe to resend.
func TestOrphansFailWhenSessionEnds(t *testing.T) {
	for _, tt := range []struct {
		name  string
		acked bool
		end   func(s *SessionState)
	}{
		{name: "ResetAcked", acked: true, end: (*SessionState).reset},
		{name: "ResetUnacked", end: (*SessionState).reset},
		{name: "CloseAcked", acked: true, end: (*SessionState).Close},
		{name: "NewKey", end: func(s *SessionState) { s.start(crypto.Key{2}.WithID(), 42) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)
			key := crypto.Key{1}.WithID()
			state := NewSessionState()
			sink := newSinkConn()
			c := newSessionTestConn(t, state, key, sink)
			var out mt.FutureSalts
			msgID, errCh := invokeAsync(t, c, key, sink, &out)
			if tt.acked {
				c.rpc.NotifyAcks([]int64{msgID})
			}
			c.rpc.ForceClose()
			awaited := awaitAsync(receive(t, errCh))
			requireWaiting(t, awaited)

			tt.end(state)
			err := receive(t, awaited)
			if tt.acked {
				a.ErrorIs(err, rpc.ErrClosedAfterAck)
				a.ErrorIs(err, context.Canceled, "compatible with previous acknowledged close")
				a.NotErrorIs(err, rpc.ErrEngineClosed)
			} else {
				a.ErrorIs(err, rpc.ErrEngineClosed)
			}
			a.Empty(state.requests)
		})
	}
}

// Request of connection without shared session fails as before.
func TestNotSharedSessionRequestFails(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	sink := newSinkConn()
	c := New(func(ctx context.Context) (transport.Conn, error) { return sink, nil }, Options{
		Logger: log.Nop, Random: rand.Reader, Key: key, Clock: clock.System, RetryInterval: time.Hour,
	})
	a.NoError(c.connect(context.Background()))
	var out mt.FutureSalts
	_, errCh := invokeAsync(t, c, key, sink, &out)
	c.rpc.ForceClose()
	err := receive(t, errCh)
	a.ErrorIs(err, rpc.ErrEngineClosed)
	var orphan *orphanError
	a.False(errors.As(err, &orphan))
	a.Equal(err, AwaitSession(context.Background(), err))
}

// Requests sent before first_msg_id of new_session_created were not processed
// by server and are resent with a new msg_id (M4).
func TestRequestsBeforeNewSessionResent(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	sink := newSinkConn()
	c := newSessionTestConn(t, NewSessionState(), key, sink)
	var out mt.FutureSalts
	first, errCh := invokeAsync(t, c, key, sink, &out)

	var created bin.Buffer
	a.NoError(created.Encode(&mt.NewSessionCreated{FirstMsgID: first + 4, ServerSalt: 1}))
	a.NoError(c.handleSessionCreated(&created))

	resent := sink.next(t, key, 1)
	a.Greater(resent.msgID, first)
	var req mt.GetFutureSaltsRequest
	a.NoError(req.Decode(resent.body))

	res := encodeResult(t, resent.msgID, &mt.FutureSalts{ReqMsgID: resent.msgID, Now: 3})
	a.NoError(c.consumeMessage(context.Background(),
		encryptPayloadFromServer(t, key, c.session().ID, serverIDs(1)[0], 1, res)))
	a.NoError(receive(t, errCh))
	a.Equal(3, out.Now)
}

// msg_detailed_info: answer is acknowledged if received, otherwise requested
// with msg_resend_req (M2).
func TestDetailedInfo(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	sink := newSinkConn()
	c := newSessionTestConn(t, NewSessionState(), key, sink)

	ids := serverIDs(2)
	received, missing := ids[0], ids[1]
	a.Equal(proto.MessageIDAccepted, checkReceived(c.receivedIDs(), received))

	var b bin.Buffer
	a.NoError(b.Encode(&mt.MsgDetailedInfo{MsgID: 1, AnswerMsgID: received}))
	a.NoError(c.handleDetailedInfo(&b))
	ack := sink.next(t, key, 0)
	a.Zero(ack.seqNo & 1)
	var msgsAck mt.MsgsAck
	a.NoError(msgsAck.Decode(ack.body))
	a.Equal([]int64{received}, msgsAck.MsgIDs)

	b.Reset()
	a.NoError(b.Encode(&mt.MsgNewDetailedInfo{AnswerMsgID: missing}))
	a.NoError(c.handleDetailedInfo(&b))
	resend := sink.next(t, key, 1)
	a.Equal(int32(1), resend.seqNo&1, "msg_resend_req is content-related")
	var req mt.MsgResendReq
	a.NoError(req.Decode(resend.body))
	a.Equal([]int64{missing}, req.MsgIDs)
}

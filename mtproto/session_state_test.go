package mtproto

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/log"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/transport"
)

// newResumeTestConn returns connection with existing key that dials a transport
// which accepts the connection but never returns data.
func newResumeTestConn(state *SessionState, key crypto.AuthKey) *Conn {
	return New(func(ctx context.Context) (transport.Conn, error) {
		return &closeConn{}, nil
	}, Options{
		Logger:  log.Nop,
		Random:  rand.Reader,
		Key:     key,
		Session: state,
		Clock:   clock.System,
	})
}

// Successive connections sharing SessionState must move the session to the new
// TCP connection (same session_id, continued msg_seqno), as official clients do.
// A new session_id on reconnect is a second main session on the same
// authorization and gets the key revoked with AUTH_KEY_DUPLICATED.
func TestSessionResumedAcrossConnections(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	state := NewSessionState()

	first := newResumeTestConn(state, key)
	a.NoError(first.connect(context.Background()))
	a.False(first.sessionResumed)
	id := first.session().ID
	a.NotZero(id)
	a.Equal(id, state.ID())

	_, seq1 := first.nextMsgSeq(true)
	_, seq2 := first.nextMsgSeq(true)
	a.Equal(int32(1), seq1)
	a.Equal(int32(3), seq2)

	second := newResumeTestConn(state, key)
	a.NoError(second.connect(context.Background()))
	a.True(second.sessionResumed)
	a.Equal(id, second.session().ID, "reconnect must keep session_id")

	_, seq3 := second.nextMsgSeq(false)
	a.Equal(int32(4), seq3, "msg_seqno must continue after reconnect")
	_, seq4 := second.nextMsgSeq(true)
	a.Equal(int32(5), seq4)
}

// Without shared state every connection starts its own session (previous behavior).
func TestSessionNotSharedByDefault(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()

	first := newResumeTestConn(nil, key)
	a.NoError(first.connect(context.Background()))
	second := newResumeTestConn(nil, key)
	a.NoError(second.connect(context.Background()))

	a.False(second.sessionResumed)
	a.NotEqual(first.session().ID, second.session().ID)
	_, seq := second.nextMsgSeq(true)
	a.Equal(int32(1), seq)
}

// A session belongs to one auth key: a connection with another key starts a new one.
func TestSessionNotResumedForOtherKey(t *testing.T) {
	a := require.New(t)
	state := NewSessionState()

	first := newResumeTestConn(state, crypto.Key{1}.WithID())
	a.NoError(first.connect(context.Background()))
	first.nextMsgSeq(true)

	second := newResumeTestConn(state, crypto.Key{2}.WithID())
	a.NoError(second.connect(context.Background()))
	a.False(second.sessionResumed)
	a.NotEqual(first.session().ID, second.session().ID)
	_, seq := second.nextMsgSeq(true)
	a.Equal(int32(1), seq)
}

// bad_msg_notification 32/33 means msg_seqno is inconsistent: the session is
// dropped and the connection restarted, the next connection starts a new session.
func TestSessionResetOnBadSeqNo(t *testing.T) {
	for _, code := range []int{codeMessageSeqNoTooLow, codeMessageSeqNoTooHigh} {
		a := require.New(t)
		key := crypto.Key{1}.WithID()
		state := NewSessionState()

		first := newResumeTestConn(state, key)
		a.NoError(first.connect(context.Background()))
		id := first.session().ID

		var b bin.Buffer
		a.NoError(b.Encode(&mt.BadMsgNotification{BadMsgID: 1, ErrorCode: code}))
		a.NoError(first.handleBadMsg(&b))
		a.Zero(state.ID())
		a.ErrorIs(first.sessionResetLoop(context.Background()), ErrSessionReset)

		second := newResumeTestConn(state, key)
		a.NoError(second.connect(context.Background()))
		a.False(second.sessionResumed)
		a.NotEqual(id, second.session().ID)
	}
}

// Other bad_msg codes (msg_id time) are handled by resending, not by session reset.
func TestSessionKeptOnBadMsgID(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	state := NewSessionState()

	c := newResumeTestConn(state, key)
	a.NoError(c.connect(context.Background()))
	id := state.ID()

	var b bin.Buffer
	a.NoError(b.Encode(&mt.BadMsgNotification{BadMsgID: 1, ErrorCode: codeMessageIDTooLow}))
	a.NoError(c.handleBadMsg(&b))
	a.Equal(id, state.ID())
	select {
	case <-c.sessionResetCh:
		t.Fatal("unexpected session reset")
	default:
	}
}

// Messages the server resends to a resumed session (not acknowledged on the
// previous connection) must be dropped by the replay protection, not handled twice.
func TestSessionReplayProtectionKeptAcrossConnections(t *testing.T) {
	a := require.New(t)
	key := crypto.Key{1}.WithID()
	state := NewSessionState()

	first := newResumeTestConn(state, key)
	a.NoError(first.connect(context.Background()))
	msgID := int64(proto.NewMessageID(time.Now(), proto.MessageServerResponse))
	a.True(first.receivedIDs().Consume(msgID))

	second := newResumeTestConn(state, key)
	a.NoError(second.connect(context.Background()))
	a.False(second.receivedIDs().Consume(msgID), "message from previous connection must be rejected")
}

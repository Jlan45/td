package mtproto

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/rpc"
)

// encryptPayloadFromServer encrypts payload as a server message of the session.
func encryptPayloadFromServer(
	t *testing.T,
	key crypto.AuthKey,
	sessionID, msgID int64,
	seqNo int32,
	payload bin.Encoder,
) *bin.Buffer {
	t.Helper()
	var data bin.Buffer
	require.NoError(t, data.Encode(payload))
	var out bin.Buffer
	require.NoError(t, crypto.NewServerCipher(rand.Reader).Encrypt(key, crypto.EncryptedMessageData{
		SessionID:              sessionID,
		MessageID:              msgID,
		SeqNo:                  seqNo,
		MessageDataLen:         int32(data.Len()),
		MessageDataWithPadding: data.Copy(),
	}, &out))
	return &out
}

// innerMessage encodes pong as message of container.
func innerMessage(t *testing.T, msgID int64, seqNo int) proto.Message {
	t.Helper()
	var body bin.Buffer
	require.NoError(t, body.Encode(&mt.Pong{MsgID: 1, PingID: msgID}))
	return proto.Message{ID: msgID, SeqNo: seqNo, Bytes: body.Len(), Body: body.Copy()}
}

func newReceiveTestConn(t *testing.T) (*Conn, crypto.AuthKey, int64) {
	t.Helper()
	key := crypto.Key{1}.WithID()
	c := newResumeTestConn(NewSessionState(), key)
	c.cipher = crypto.NewClientCipher(rand.Reader)
	c.ackSendChan = make(chan int64, 16)
	require.NoError(t, c.connect(context.Background()))
	return c, key, c.session().ID
}

func drainAcks(c *Conn) []int64 {
	var acks []int64
	for {
		select {
		case id := <-c.ackSendChan:
			acks = append(acks, id)
		default:
			return acks
		}
	}
}

// serverIDs returns n increasing server message ids.
func serverIDs(n int) []int64 {
	gen := proto.NewMessageIDGen(time.Now)
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = gen.New(proto.MessageFromServer)
	}
	return ids
}

// Every content-related inner message of a container must be acknowledged
// (container itself has even seqno), and inner messages resent by the server
// in another container must not be handled twice.
func TestContainerInnerMessagesAckedAndDeduplicated(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	c, key, sessionID := newReceiveTestConn(t)

	ids := serverIDs(5)
	outer1, inner1, inner2, inner3, outer2 := ids[0], ids[1], ids[2], ids[3], ids[4]

	container := &proto.MessageContainer{Messages: []proto.Message{
		innerMessage(t, inner1, 1),
		innerMessage(t, inner2, 3),
		innerMessage(t, inner3, 4), // service message, no ack.
	}}
	a.NoError(c.consumeMessage(ctx, encryptPayloadFromServer(t, key, sessionID, outer1, 4, container)))
	a.Equal([]int64{inner1, inner2}, drainAcks(c))
	a.Equal(uint64(3), c.pongSeq.Load(), "all inner messages handled")

	// Server resends unacknowledged inner messages in a new container.
	resent := &proto.MessageContainer{Messages: []proto.Message{
		innerMessage(t, inner1, 1),
		innerMessage(t, inner2, 3),
	}}
	a.NoError(c.consumeMessage(ctx, encryptPayloadFromServer(t, key, sessionID, outer2, 6, resent)))
	a.Equal([]int64{inner1, inner2}, drainAcks(c), "duplicates are acknowledged")
	a.Equal(uint64(3), c.pongSeq.Load(), "duplicates are not handled")
}

// Replay protection must remember the whole window, not only the last message.
func TestDuplicateDetectedAfterOtherMessages(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	c, key, sessionID := newReceiveTestConn(t)

	ids := serverIDs(11)
	for _, id := range ids {
		a.NoError(c.consumeMessage(ctx, encryptFromServer(t, key, sessionID, id, 1)))
	}
	a.Equal(uint64(len(ids)), c.pongSeq.Load())
	drainAcks(c)

	a.NoError(c.consumeMessage(ctx, encryptFromServer(t, key, sessionID, ids[0], 1)))
	a.Equal(uint64(len(ids)), c.pongSeq.Load(), "resent message must not be handled")
	a.Equal([]int64{ids[0]}, drainAcks(c))
}

func requireSessionReset(t *testing.T, c *Conn) {
	t.Helper()
	a := require.New(t)
	a.Zero(c.state().ID(), "session must be dropped")
	a.True(c.sessionResetting.Load())
	a.ErrorIs(c.sessionResetLoop(context.Background()), ErrSessionReset)

	// Writes fail with retryable error until the connection is closed.
	err := c.write(context.Background(), 1, 1, &mt.PingRequest{PingID: 1})
	a.ErrorIs(err, rpc.ErrEngineClosed)
}

// fillReceived fills replay protection window with recent ids, returning an id
// that is older than all of them but still valid by time.
func fillReceived(t *testing.T, c *Conn) (old int64) {
	t.Helper()
	old = int64(proto.NewMessageID(time.Now().Add(-time.Minute), proto.MessageFromServer))
	for _, id := range serverIDs(receivedBufSize) {
		require.Equal(t, proto.MessageIDAccepted, checkReceived(c.receivedIDs(), id))
	}
	return old
}

// A message lower than all stored msg_id values may or may not have been
// processed: it must be neither handled nor acknowledged, and the session must
// be reset (TDLib, Telegram Desktop and Android do so).
func TestTooOldMessageResetsSession(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	c, key, sessionID := newReceiveTestConn(t)
	old := fillReceived(t, c)

	a.NoError(c.consumeMessage(ctx, encryptFromServer(t, key, sessionID, old, 1)))
	a.Empty(drainAcks(c), "too old message must not be acknowledged")
	a.Zero(c.pongSeq.Load(), "too old message must not be handled")
	requireSessionReset(t, c)

	// Messages arriving after reset belong to the dropped session.
	next := serverIDs(1)[0]
	a.NoError(c.consumeMessage(ctx, encryptFromServer(t, key, sessionID, next, 1)))
	a.Empty(drainAcks(c))
	a.Zero(c.pongSeq.Load())
}

func TestTooOldMessageInContainerResetsSession(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	c, key, sessionID := newReceiveTestConn(t)
	old := fillReceived(t, c)
	outer := serverIDs(1)[0]

	container := &proto.MessageContainer{Messages: []proto.Message{
		innerMessage(t, old, 1),
	}}
	a.NoError(c.consumeMessage(ctx, encryptPayloadFromServer(t, key, sessionID, outer, 2, container)))
	a.Empty(drainAcks(c))
	a.Zero(c.pongSeq.Load())
	requireSessionReset(t, c)
}

func TestSessionResetOnlyOnce(t *testing.T) {
	a := require.New(t)
	c, _, _ := newReceiveTestConn(t)
	c.requestSessionReset(sessionResetBadSeqNo)
	c.requestSessionReset(sessionResetTooOld)
	a.Len(c.sessionResetCh, 1)
	a.True(errors.Is(errWriteSessionReset, rpc.ErrEngineClosed))
}

// With local clock skewed far from server time, server messages must still be
// accepted (server time is measured before msg_id validation) and generated
// msg_id must follow server time, otherwise neither side accepts messages of
// the other one.
func TestSkewedLocalClockFollowsServerTime(t *testing.T) {
	for _, skew := range []time.Duration{10 * time.Minute, -10 * time.Minute} {
		t.Run(skew.String(), func(t *testing.T) {
			a := require.New(t)
			ctx := context.Background()
			c, key, sessionID := newReceiveTestConn(t)

			serverNow := time.Now().Add(skew)
			msgID := int64(proto.NewMessageID(serverNow, proto.MessageFromServer))
			a.NoError(c.consumeMessage(ctx, encryptFromServer(t, key, sessionID, msgID, 1)))
			a.Equal(uint64(1), c.pongSeq.Load(), "server message must be accepted")
			a.InDelta(skew.Seconds(), c.serverTime.Offset().Seconds(), 2)

			generated := proto.MessageID(c.newMessageID()).Time()
			a.WithinDuration(serverNow, generated, 2*time.Second)
		})
	}
}

// bad_msg_notification 17 (msg_id too high) resets server time difference to
// the time of the notification itself.
func TestBadMsgIDTooHighResetsServerTime(t *testing.T) {
	a := require.New(t)
	c, _, _ := newReceiveTestConn(t)
	c.serverTime.Set(time.Hour)

	notification := int64(proto.NewMessageID(time.Now(), proto.MessageFromServer))
	var b bin.Buffer
	a.NoError(b.Encode(&mt.BadMsgNotification{BadMsgID: 1, ErrorCode: codeMessageIDTooHigh}))
	a.NoError(c.handleBadMsg(notification, &b))
	a.InDelta(0, c.serverTime.Offset().Seconds(), 2)
}

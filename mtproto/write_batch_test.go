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
	"github.com/gotd/td/testutil"
)

// gatedSink blocks the first Send until released, so that other writes queue.
type gatedSink struct {
	*sinkConn
	entered chan struct{}
	release chan struct{}
	first   bool
}

func newGatedSink() *gatedSink {
	return &gatedSink{sinkConn: newSinkConn(), entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedSink) Send(ctx context.Context, b *bin.Buffer) error {
	g.mux.Lock()
	first := !g.first
	g.first = true
	g.mux.Unlock()
	if first {
		close(g.entered)
		<-g.release
	}
	return g.sinkConn.Send(ctx, b)
}

func newBatchTestConn(t *testing.T, sink *gatedSink) (*Conn, crypto.AuthKey) {
	t.Helper()
	key := crypto.Key{1}.WithID()
	c := newSessionTestConn(t, NewSessionState(), key, sink.sinkConn)
	c.conn = sink
	return c, key
}

func decodeContainer(t *testing.T, m sentMessage) []proto.Message {
	t.Helper()
	var container proto.MessageContainer
	require.NoError(t, container.Decode(m.body))
	return container.Messages
}

// Writes issued while another write is in progress are packed into a
// container: ordered by msg_id, with the container msg_id greater than all of
// them, even container seqno and original seqno of every message.
func TestWritesPackedIntoContainer(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	sink := newGatedSink()
	c, key := newBatchTestConn(t, sink)

	first := make(chan error, 1)
	go func() { first <- c.writeServiceMessage(ctx, &mt.PingRequest{PingID: 0}) }()
	<-sink.entered

	const n = 5
	type sent struct {
		msgID int64
		seqNo int32
	}
	ids := make([]sent, n)
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		msgID, seqNo := c.nextMsgSeq(true)
		ids[i] = sent{msgID, seqNo}
		go func(i int) {
			results <- c.write(ctx, ids[i].msgID, ids[i].seqNo, &mt.PingRequest{PingID: int64(i + 1)})
		}(i)
	}
	// Wait until all writes are queued.
	require.Eventually(t, func() bool {
		c.writes.mux.Lock()
		defer c.writes.mux.Unlock()
		return len(c.writes.queue) == n
	}, 5*time.Second, time.Millisecond)
	close(sink.release)

	a.NoError(<-first)
	for i := 0; i < n; i++ {
		a.NoError(<-results)
	}

	msg := sink.next(t, key, 1)
	a.Zero(msg.seqNo&1, "container is not content-related")
	inner := decodeContainer(t, msg)
	a.Len(inner, n)
	for i, m := range inner {
		a.Equal(ids[i].msgID, m.ID)
		a.Equal(int(ids[i].seqNo), m.SeqNo)
		a.Less(m.ID, msg.msgID, "container msg_id must be greater")
		var ping mt.PingRequest
		a.NoError(ping.Decode(&bin.Buffer{Buf: m.Body}))
		a.Equal(int64(i+1), ping.PingID)
	}
	a.Len(c.containerMessages(msg.msgID), n)
}

// Messages exceeding container size limit are split.
func TestContainerSizeLimit(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	sink := newGatedSink()
	c, key := newBatchTestConn(t, sink)

	go func() { _ = c.writeServiceMessage(ctx, &mt.PingRequest{}) }()
	<-sink.entered

	// Random data is not compressed.
	big := make([]byte, containerMaxSize/2+100)
	_, _ = rand.Read(big)
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		msgID, seqNo := c.nextMsgSeq(true)
		go func() { results <- c.write(ctx, msgID, seqNo, &testPayload{Data: big}) }()
	}
	require.Eventually(t, func() bool {
		c.writes.mux.Lock()
		defer c.writes.mux.Unlock()
		return len(c.writes.queue) == 3
	}, 5*time.Second, time.Millisecond)
	close(sink.release)
	for i := 0; i < 3; i++ {
		a.NoError(<-results)
	}
	// Each big message is sent on its own.
	for i := 1; i <= 3; i++ {
		msg := sink.next(t, key, i)
		id, err := msg.body.PeekID()
		a.NoError(err)
		a.NotEqual(uint32(proto.MessageContainerTypeID), id)
	}
}

func TestDisableContainers(t *testing.T) {
	a := require.New(t)
	ctx := context.Background()
	sink := newGatedSink()
	c, key := newBatchTestConn(t, sink)
	c.disableContainers = true

	go func() { _ = c.writeServiceMessage(ctx, &mt.PingRequest{}) }()
	<-sink.entered
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- c.writeServiceMessage(ctx, &mt.PingRequest{PingID: 1}) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(sink.release)
	a.NoError(<-results)
	a.NoError(<-results)
	for i := 1; i <= 2; i++ {
		var ping mt.PingRequest
		a.NoError(ping.Decode(sink.next(t, key, i).body))
	}
}

// Queued write canceled by caller is not sent.
func TestQueuedWriteCanceled(t *testing.T) {
	a := require.New(t)
	sink := newGatedSink()
	c, key := newBatchTestConn(t, sink)

	go func() { _ = c.writeServiceMessage(context.Background(), &mt.PingRequest{}) }()
	<-sink.entered

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() { canceled <- c.writeServiceMessage(ctx, &mt.PingRequest{PingID: 13}) }()
	require.Eventually(t, func() bool {
		c.writes.mux.Lock()
		defer c.writes.mux.Unlock()
		return len(c.writes.queue) == 1
	}, 5*time.Second, time.Millisecond)
	cancel()
	a.ErrorIs(<-canceled, context.Canceled)

	kept := make(chan error, 1)
	go func() { kept <- c.writeServiceMessage(context.Background(), &mt.PingRequest{PingID: 14}) }()
	require.Eventually(t, func() bool {
		c.writes.mux.Lock()
		defer c.writes.mux.Unlock()
		return len(c.writes.queue) == 2
	}, 5*time.Second, time.Millisecond)
	close(sink.release)
	a.NoError(<-kept)

	var ping mt.PingRequest
	a.NoError(ping.Decode(sink.next(t, key, 1).body))
	a.Equal(int64(14), ping.PingID, "canceled write must not be sent")
}

// bad_msg_notification 64 for a container makes its requests resend.
func TestInvalidContainerResent(t *testing.T) {
	a := require.New(t)
	sink := newSinkConn()
	key := crypto.Key{1}.WithID()
	c := newSessionTestConn(t, NewSessionState(), key, sink)

	var out mt.FutureSalts
	first, errCh := invokeAsync(t, c, key, sink, &out)
	containerID := first + 4
	c.rememberContainer(containerID, []int64{first})

	var b bin.Buffer
	a.NoError(b.Encode(&mt.BadMsgNotification{BadMsgID: containerID, ErrorCode: codeInvalidContainer}))
	a.NoError(c.handleBadMsg(serverIDs(1)[0], &b))

	resent := sink.next(t, key, 1)
	a.Greater(resent.msgID, first)
	res := encodeResult(t, resent.msgID, &mt.FutureSalts{ReqMsgID: resent.msgID, Now: 5})
	a.NoError(c.consumeMessage(context.Background(),
		encryptPayloadFromServer(t, key, c.session().ID, serverIDs(1)[0], 1, res)))
	a.NoError(receive(t, errCh))
	a.Equal(5, out.Now)
}

// Uncontended write keeps the previous allocation profile.
func TestUncontendedWriteAllocs(t *testing.T) {
	if testutil.Race {
		t.Skip("Skipped (race detector conflicts with allocation tests)")
	}
	key := crypto.Key{1}.WithID()
	c := &Conn{
		conn:              &constantConn{},
		clock:             clock.System,
		rand:              testutil.ZeroRand{},
		cipher:            crypto.NewClientCipher(testutil.ZeroRand{}),
		log:               log.For(log.Nop),
		authKey:           key,
		compressThreshold: -1,
	}
	data := &testPayload{Data: make([]byte, 64)}
	before := testing.AllocsPerRun(100, func() {
		_ = c.writeOne(context.Background(), 1, 1, data)
	})
	after := testing.AllocsPerRun(100, func() {
		_ = c.write(context.Background(), 1, 1, data)
	})
	require.Equal(t, before, after)
}

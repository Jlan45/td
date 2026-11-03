package mtproto

import (
	"context"
	"sort"
	"sync"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
)

// Outgoing messages are packed into msg_container when they are written
// concurrently, like official clients pack requests, acks and pings
// (TDLib SessionConnection, Telegram Desktop tryToSend):
//
// A write that finds no write in progress is sent immediately, as a single
// message (no added latency, no allocations). Writes issued while another one
// is in progress are queued, and the writer in progress sends them after its
// own message, packed into containers. So packing happens only when messages
// would wait for the transport anyway.

const (
	// containerMaxSize limits the size of packed messages in one container,
	// larger messages are sent alone (Telegram Desktop kCutContainerOnSize).
	containerMaxSize = 16 * 1024
	// containerMaxMessages is the maximum number of messages in a container.
	containerMaxMessages = 1020
	// sentContainersLimit is the number of last sent containers remembered to
	// map bad_msg_notification for a container to its messages.
	sentContainersLimit = 128
)

// queuedWrite is a write waiting for the writer in progress.
type queuedWrite struct {
	msgID   int64
	seqNo   int32
	message bin.Encoder
	// canceled is set when caller does not wait for the write anymore.
	canceled bool
	err      error
	done     chan struct{}
}

// writeQueue stores state of concurrent writes. Zero value is ready to use.
type writeQueue struct {
	mux     sync.Mutex
	writing bool
	queue   []*queuedWrite

	// containers maps msg_id of sent containers to msg_id of their messages.
	containers     map[int64][]int64
	containerOrder []int64
}

// write sends message, packing concurrent writes into containers.
func (c *Conn) write(ctx context.Context, msgID int64, seqNo int32, message bin.Encoder) error {
	if c.disableContainers {
		return c.writeOne(ctx, msgID, seqNo, message)
	}

	q := &c.writes
	q.mux.Lock()
	if !q.writing {
		q.writing = true
		q.mux.Unlock()

		err := c.writeOne(ctx, msgID, seqNo, message)
		c.flushQueued(ctx)
		return err
	}
	w := &queuedWrite{
		msgID:   msgID,
		seqNo:   seqNo,
		message: message,
		done:    make(chan struct{}),
	}
	q.queue = append(q.queue, w)
	q.mux.Unlock()

	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		q.mux.Lock()
		w.canceled = true
		q.mux.Unlock()
		select {
		case <-w.done:
			// Already sent.
			return w.err
		default:
			return ctx.Err()
		}
	}
}

// flushQueued sends writes queued while the caller was writing, until the
// queue is empty.
func (c *Conn) flushQueued(ctx context.Context) {
	q := &c.writes
	for {
		q.mux.Lock()
		var batch []*queuedWrite
		for _, w := range q.queue {
			if w.canceled {
				close(w.done)
				continue
			}
			batch = append(batch, w)
		}
		q.queue = q.queue[:0]
		if len(batch) == 0 {
			q.writing = false
			q.mux.Unlock()
			return
		}
		q.mux.Unlock()

		c.sendBatch(ctx, batch)
	}
}

// sendBatch sends queued writes, packing them into containers.
func (c *Conn) sendBatch(ctx context.Context, batch []*queuedWrite) {
	complete := func(ws []*queuedWrite, err error) {
		for _, w := range ws {
			w.err = err
			close(w.done)
		}
	}

	// Messages in container must be ordered by msg_id.
	sort.Slice(batch, func(i, j int) bool { return batch[i].msgID < batch[j].msgID })

	var (
		messages []proto.Message
		pending  []*queuedWrite
		size     int
	)
	flush := func() {
		switch len(pending) {
		case 0:
			return
		case 1:
			w := pending[0]
			complete(pending, c.writeOne(ctx, w.msgID, w.seqNo, w.message))
		default:
			complete(pending, c.writeContainer(ctx, messages))
		}
		messages, pending, size = nil, nil, 0
	}

	for _, w := range batch {
		body := &bin.Buffer{}
		if err := c.encodePayload(w.message, body); err != nil {
			complete([]*queuedWrite{w}, errors.Wrap(err, "encode"))
			continue
		}
		if len(pending) > 0 && (size+body.Len() > containerMaxSize || len(pending) >= containerMaxMessages) {
			flush()
		}
		messages = append(messages, proto.Message{
			ID:    w.msgID,
			SeqNo: int(w.seqNo),
			Bytes: body.Len(),
			Body:  body.Buf,
		})
		pending = append(pending, w)
		size += body.Len()
	}
	flush()
}

// writeContainer sends messages packed into msg_container.
func (c *Conn) writeContainer(ctx context.Context, messages []proto.Message) error {
	// Container is not content-related, and its msg_id must be greater than
	// msg_id of contained messages: it is generated after them.
	containerID, seqNo := c.nextMsgSeq(false)
	container := &proto.MessageContainer{Messages: messages}

	raw := bufPool.Get()
	defer bufPool.Put(raw)
	if err := container.Encode(raw); err != nil {
		return errors.Wrap(err, "encode container")
	}

	ids := make([]int64, len(messages))
	for i, m := range messages {
		ids[i] = m.ID
	}
	c.rememberContainer(containerID, ids)
	c.log.Debug(ctx, "Sending container",
		log.Int64("container_msg_id", containerID),
		log.Any("msg_ids", ids),
	)
	return c.send(ctx, containerID, seqNo, container, raw)
}

func (c *Conn) rememberContainer(containerID int64, ids []int64) {
	q := &c.writes
	q.mux.Lock()
	defer q.mux.Unlock()
	if q.containers == nil {
		q.containers = map[int64][]int64{}
	}
	q.containers[containerID] = ids
	q.containerOrder = append(q.containerOrder, containerID)
	if len(q.containerOrder) > sentContainersLimit {
		delete(q.containers, q.containerOrder[0])
		q.containerOrder = q.containerOrder[1:]
	}
}

// containerMessages returns msg_id of messages of sent container, or nil if
// id is not a container.
func (c *Conn) containerMessages(id int64) []int64 {
	q := &c.writes
	q.mux.Lock()
	defer q.mux.Unlock()
	return q.containers[id]
}

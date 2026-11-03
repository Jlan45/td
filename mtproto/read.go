package mtproto

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
)

// https://core.telegram.org/mtproto/description#message-identifier-msg-id
// A message is rejected over 300 seconds after it is created or 30 seconds
// before it is created (this is needed to protect from replay attacks).
const (
	maxPast   = time.Second * 300
	maxFuture = time.Second * 30
)

// errRejected is returned on invalid message that should not be processed.
var errRejected = errors.New("message rejected")

// errDuplicate is returned on already processed message. It is not handled
// again but still acknowledged: after a reconnect the server resends messages
// whose ack was lost with the previous connection and keeps resending them
// until acknowledged (TDLib check_packet code 1 -> send_ack, Telegram Desktop
// and Android ack before deduplication).
var errDuplicate = errors.Wrap(errRejected, "already processed")

// errTooOld is returned on message with msg_id lower than all stored msg_id
// values: it is impossible to tell whether it was processed, so the session
// can not be trusted anymore. Such message is neither handled nor
// acknowledged and the session is reset, like TDLib (check_packet code 2 ->
// on_session_failed), Telegram Desktop (TooOld -> ResetSession) and Telegram
// Android (isMessageIdProcessed == 2 -> recreateSession) do.
var errTooOld = errors.Wrap(errRejected, "too old")

// checkReceived checks msg_id against replay protection buffer.
func checkReceived(buf MessageBuf, msgID int64) proto.MessageIDStatus {
	if b, ok := buf.(interface {
		Check(id int64) proto.MessageIDStatus
	}); ok {
		return b.Check(msgID)
	}
	if buf.Consume(msgID) {
		return proto.MessageIDAccepted
	}
	return proto.MessageIDDuplicate
}

// receivedErr maps msg_id check result to error.
func receivedErr(status proto.MessageIDStatus, msgID int64) error {
	switch status {
	case proto.MessageIDDuplicate:
		return errors.Wrapf(errDuplicate, "duplicate message id %d", msgID)
	case proto.MessageIDTooOld:
		return errors.Wrapf(errTooOld, "message id %d is lower than all stored", msgID)
	default:
		return nil
	}
}

func checkMessageID(now time.Time, rawID int64) error {
	id := proto.MessageID(rawID)

	// Check that message is from server.
	switch id.Type() {
	case proto.MessageFromServer, proto.MessageServerResponse:
		// Valid.
	default:
		return errors.Wrapf(errRejected, "unexpected type %s", id.Type())
	}

	created := id.Time()
	if created.Before(now) && now.Sub(created) > maxPast {
		return errors.Wrap(errRejected, "created too far in past")
	}
	if created.Sub(now) > maxFuture {
		return errors.Wrap(errRejected, "created too far in future")
	}

	return nil
}

func (c *Conn) decryptMessage(b *bin.Buffer) (*crypto.EncryptedMessageData, error) {
	session := c.session()
	msg, err := c.cipher.DecryptFromBuffer(session.Key, b)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	// Validating message. This protects from replay attacks.
	if msg.SessionID != session.ID {
		return nil, errors.Wrapf(errRejected, "invalid session (got %d, expected %d)", msg.SessionID, session.ID)
	}
	now := c.clock.Now()
	if t := proto.MessageID(msg.MessageID).Type(); t == proto.MessageFromServer || t == proto.MessageServerResponse {
		// Measure server time before validating msg_id time: with a skewed
		// local clock every server message would be rejected otherwise
		// (TDLib AuthData::check_packet). Replayed messages can not move
		// server time forward, and difference never decreases on Observe.
		c.serverTime.Observe(msg.MessageID, now)
	}
	if err := checkMessageID(c.serverTime.Now(now), msg.MessageID); err != nil {
		return nil, errors.Wrapf(err, "bad message id %d", msg.MessageID)
	}
	if err := receivedErr(checkReceived(c.receivedIDs(), msg.MessageID), msg.MessageID); err != nil {
		return msg, err
	}

	return msg, nil
}

func (c *Conn) consumeMessage(ctx context.Context, buf *bin.Buffer) error {
	if c.sessionResetting.Load() {
		// Session is being reset, connection is about to be closed: messages
		// of the dropped session must not be handled or acknowledged.
		return nil
	}
	msg, err := c.decryptMessage(buf)
	if errors.Is(err, errDuplicate) {
		c.log.Debug(ctx, "Ignoring already processed message", log.Error(err))
		if msg.SeqNo&0x01 != 0 {
			return c.enqueueAck(ctx, msg.MessageID)
		}
		return nil
	}
	if errors.Is(err, errTooOld) {
		c.requestSessionReset(sessionResetTooOld)
		return nil
	}
	if errors.Is(err, errRejected) {
		c.log.Warn(ctx, "Ignoring rejected message", log.Error(err))
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "consume message")
	}

	// Count the message before handleMessage. Handlers may block (for example a
	// slow UpdateHandler); liveness must not wait on them.
	c.recvMessages.Inc()

	body := &bin.Buffer{Buf: msg.Data()}
	if id, err := body.PeekID(); err == nil && id == proto.MessageContainerTypeID {
		// Container itself is not content-related (even seqno), but every inner
		// message must be acknowledged and deduplicated on its own.
		return c.consumeContainer(ctx, body)
	}

	if err := c.handleMessage(msg.MessageID, body); err != nil {
		// Probably we can return here, but this will shutdown whole
		// connection which can be unexpected.
		c.log.Warn(ctx, "Error while handling message", log.Error(err))
		// Sending acknowledge even on error. Client should restore
		// from missing updates via explicit pts check and getDiff call.
	}

	needAck := (msg.SeqNo & 0x01) != 0
	if needAck {
		return c.enqueueAck(ctx, msg.MessageID)
	}

	return nil
}

// consumeContainer handles inner messages of msg_container.
//
// Like the outer message, every inner message is acknowledged before
// deduplication if it is content-related (TDLib on_slice_packet, Telegram
// Desktop and Android addMessageToConfirm), and then checked against the
// replay protection buffer, so messages resent by server are not handled
// twice.
func (c *Conn) consumeContainer(ctx context.Context, b *bin.Buffer) error {
	var container proto.MessageContainer
	if err := container.Decode(b); err != nil {
		c.log.Warn(ctx, "Failed to decode container", log.Error(err))
		return nil
	}
	for _, inner := range container.Messages {
		if c.sessionResetting.Load() {
			return nil
		}
		status := checkReceived(c.receivedIDs(), inner.ID)
		if status == proto.MessageIDTooOld {
			c.log.Warn(ctx, "Too old message in container", log.Int64("msg_id", inner.ID))
			c.requestSessionReset(sessionResetTooOld)
			return nil
		}
		if inner.SeqNo&0x01 != 0 {
			if err := c.enqueueAck(ctx, inner.ID); err != nil {
				return err
			}
		}
		if status == proto.MessageIDDuplicate {
			c.log.Debug(ctx, "Ignoring already processed message in container", log.Int64("msg_id", inner.ID))
			continue
		}
		if err := c.handleMessage(inner.ID, &bin.Buffer{Buf: inner.Body}); err != nil {
			c.log.Warn(ctx, "Error while handling message in container",
				log.Int64("msg_id", inner.ID),
				log.Error(err),
			)
		}
	}
	return nil
}

// enqueueAck queues msg_id for acknowledgement.
func (c *Conn) enqueueAck(ctx context.Context, msgID int64) error {
	// Enqueue message id for acknowledge. This can block if the ack loop
	// is stuck (e.g. writing acks over a half-open connection), which would
	// stall message consumption. Trace slow enqueues to surface it.
	start := c.clock.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.ackSendChan <- msgID:
		if waited := c.clock.Now().Sub(start); waited > slowWriteThreshold {
			c.log.Warn(ctx, "Slow ack enqueue (ack loop may be stalled)",
				log.Int64("msg_id", msgID),
				log.Duration("waited", waited),
			)
		}
	}
	return nil
}

func (c *Conn) noUpdates(err error) bool {
	// Checking for read timeout.
	var syscall *net.OpError
	if errors.As(err, &syscall) && syscall.Timeout() {
		// We call SetReadDeadline so such error is expected.
		c.log.Debug(context.Background(), "No updates")
		return true
	}
	return false
}

func (c *Conn) handleAuthKeyNotFound(ctx context.Context) error {
	if c.session().ID == 0 {
		// The 404 error can also be caused by zero session id.
		// See https://github.com/gotd/td/issues/107
		c.log.Warn(ctx, "BUG: zero session id found")
	}
	if c.pfs {
		// In PFS mode 404 most likely means lost temporary key, so caller should
		// recreate transport and re-bind, not regenerate permanent key in-place.
		return errors.Wrap(ErrPFSReconnectRequired, "temporary auth key not found in pfs mode")
	}
	// Do not regenerate the permanent key in place: see ErrAuthKeyNotFound.
	c.log.Warn(ctx, "Server does not know auth key, it must be verified")
	return ErrAuthKeyNotFound
}

func (c *Conn) readLoop(ctx context.Context) (err error) {
	logger := c.log.Named("read")
	logger.Debug(ctx, "Read loop started")
	defer func() {
		l := logger
		if err != nil {
			l = logger.With(log.NamedError("reason", err))
		}
		l.Debug(ctx, "Read loop done")
	}()

	var (
		// Last error encountered by consumeMessage.
		lastErr atomic.Value
		// To wait all spawned goroutines
		handlers sync.WaitGroup
	)
	defer handlers.Wait()

	for {
		// We've tried multiple ways to reduce allocations via reusing buffer,
		// but naive implementation induces high idle memory waste.
		//
		// Proper optimization will probably require total rework of bin.Buffer
		// with sharded (by payload size?) pool that can be used after message
		// size read (after readLen).
		//
		// Such optimization can introduce additional complexity overhead and
		// is probably not worth it.
		buf := &bin.Buffer{}

		// Halting if consumeMessage encountered error.
		// Should be something critical with crypto.
		if err, ok := lastErr.Load().(error); ok && err != nil {
			return errors.Wrap(err, "halting")
		}

		if err := c.conn.Recv(ctx, buf); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if c.noUpdates(err) {
					continue
				}
			}

			var protoErr *codec.ProtocolErr
			if errors.As(err, &protoErr) && protoErr.Code == codec.CodeAuthKeyNotFound {
				if err := c.handleAuthKeyNotFound(ctx); err != nil {
					return errors.Wrap(err, "auth key not found")
				}

				continue
			}

			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "read loop")
			default:
				return errors.Wrap(err, "read")
			}
		}

		handlers.Add(1)
		go func() {
			defer handlers.Done()

			// Spawning goroutine per incoming message to utilize as much
			// resources as possible while keeping idle utilization low.
			//
			// The "worker" model was replaced by this due to idle utilization
			// overhead, especially on multi-CPU systems with multiple running
			// clients.
			if err := c.consumeMessage(ctx, buf); err != nil {
				logger.Error(ctx, "Failed to process message", log.Error(err))
				lastErr.Store(errors.Wrap(err, "consume"))
			}
		}()
	}
}

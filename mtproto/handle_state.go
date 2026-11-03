package mtproto

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
)

// stateQueryInterval is how often orphan requests are re-checked for state
// query, as a fallback for the orphan notification.
const stateQueryInterval = 10 * time.Second

// serviceWriteTimeout bounds writes of service messages sent from the read
// loop handlers.
const serviceWriteTimeout = 10 * time.Second

// stateQueryLoop queries state of not acknowledged orphan requests of the
// session with msgs_state_req (TDLib Session::mark_as_unknown and
// get_state_info, Telegram Desktop checkSentRequests).
func (c *Conn) stateQueryLoop(ctx context.Context) error {
	s := c.state()
	orphaned := s.orphanedCh()
	ticker := c.clock.Ticker(stateQueryInterval)
	defer ticker.Stop()
	for {
		if ids := s.unqueried(c); len(ids) > 0 {
			if err := c.sendStateRequest(ctx, ids); err != nil {
				c.log.Warn(ctx, "Failed to query state of requests", log.Error(err))
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-orphaned:
		case <-ticker.C():
		}
	}
}

func (c *Conn) sendStateRequest(ctx context.Context, ids []int64) error {
	// msgs_state_req is content-related (Telegram Desktop needAck).
	msgID, seqNo := c.nextMsgSeq(true)
	c.state().addStateQuery(msgID, ids)
	c.log.Debug(ctx, "Querying state of requests", log.Int64("msg_id", msgID), log.Any("msg_ids", ids))
	return c.write(ctx, msgID, seqNo, &mt.MsgsStateReq{MsgIDs: ids})
}

func (c *Conn) handleStateInfo(b *bin.Buffer) error {
	var info mt.MsgsStateInfo
	if err := info.Decode(b); err != nil {
		return errors.Wrap(err, "decode")
	}
	if c.sharedSession {
		c.state().stateInfo(info.ReqMsgID, info.Info)
	}
	return nil
}

// handleDetailedInfo handles msg_detailed_info and msg_new_detailed_info: the
// answer is acknowledged if it was received, otherwise it is requested with
// msg_resend_req (Telegram Desktop handleOneReceived).
func (c *Conn) handleDetailedInfo(b *bin.Buffer) error {
	var box mt.MsgDetailedInfoBox
	if err := box.Decode(b); err != nil {
		return errors.Wrap(err, "decode")
	}

	var answerID int64
	switch v := box.MsgDetailedInfo.(type) {
	case *mt.MsgDetailedInfo:
		// Server received the request.
		c.rpc.NotifyAcks([]int64{v.MsgID})
		if c.sharedSession {
			c.state().ack([]int64{v.MsgID})
		}
		answerID = v.AnswerMsgID
	case *mt.MsgNewDetailedInfo:
		answerID = v.AnswerMsgID
	default:
		return nil
	}

	if c.answerReceived(answerID) {
		c.sendServiceAsync(&mt.MsgsAck{MsgIDs: []int64{answerID}}, false)
	} else {
		// msg_resend_req is content-related (Telegram Desktop needAck).
		c.sendServiceAsync(&mt.MsgResendReq{MsgIDs: []int64{answerID}}, true)
	}
	return nil
}

// answerReceived reports whether message with id was received in the session.
func (c *Conn) answerReceived(id int64) bool {
	b, ok := c.receivedIDs().(interface{ Contains(id int64) bool })
	return ok && b.Contains(id)
}

// sendServiceAsync writes service message without blocking the read loop.
func (c *Conn) sendServiceAsync(msg bin.Encoder, content bool) {
	msgID, seqNo := c.nextMsgSeq(content)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), serviceWriteTimeout)
		defer cancel()
		if err := c.write(ctx, msgID, seqNo, msg); err != nil {
			c.log.Debug(ctx, "Failed to send service message", log.Error(err))
		}
	}()
}

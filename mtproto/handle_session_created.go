package mtproto

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
)

func (c *Conn) handleSessionCreated(b *bin.Buffer) error {
	ctx := context.Background()
	var s mt.NewSessionCreated
	if err := s.Decode(b); err != nil {
		return errors.Wrap(err, "decode")
	}
	c.gotSession.Signal()

	c.log.Debug(ctx, "Session created",
		log.Int64("unique_id", s.UniqueID),
		log.Int64("first_msg_id", s.FirstMsgID),
		log.Time("first_msg_time", proto.MessageID(s.FirstMsgID).Time().Local()),
	)

	if offset := c.serverTime.Offset(); offset > maxFuture || -offset > maxFuture {
		// Message ids are generated in server time (see proto.ServerTime),
		// but large difference usually means misconfigured local clock.
		c.log.Warn(ctx, "Local clock differs from server time, compensating",
			log.Duration("server_time_offset", offset),
		)
	}

	// Messages sent before first_msg_id were not processed by the new
	// session, so they must be resent (TDLib on_new_session_created).
	if c.rpc != nil {
		for _, id := range c.rpc.PendingIDs() {
			if id < s.FirstMsgID {
				c.rpc.NotifyError(id, errResendInSession)
			}
		}
	}
	if c.sharedSession {
		c.state().notProcessedBefore(s.FirstMsgID)
	}

	c.storeSalt(s.ServerSalt)
	if err := c.handler.OnSession(c.session()); err != nil {
		return errors.Wrap(err, "handler.OnSession")
	}
	if h, ok := c.handler.(SessionCreatedHandler); ok {
		h.OnSessionCreated()
	}
	return nil
}

package mtproto

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
)

type badMessageError struct {
	Code    int
	NewSalt int64
}

const (
	codeMessageIDTooLow     = 16
	codeMessageIDTooHigh    = 17
	codeMessageSeqNoTooLow  = 32
	codeMessageSeqNoTooHigh = 33
	codeIncorrectServerSalt = 48
	codeInvalidContainer    = 64
)

func (c badMessageError) Error() string {
	description := map[int]string{
		codeMessageIDTooLow:     "msg_id too low",
		codeMessageIDTooHigh:    "msg_id too high",
		codeIncorrectServerSalt: "incorrect server salt",

		18: "incorrect two lower order msg_id bits",
		19: "container msg_id is the same as msg_id of a previously received message",
		20: "message too old",
		32: "msg_seqno too low",
		33: "msg_seqno too high",
		34: "even msg_seqno expected, but odd received",
		35: "odd msg_seqno expected, but even received",
	}[c.Code]
	if description == "" {
		return fmt.Sprintf("bad msg error code %d", c.Code)
	}
	return description
}

// notifyBadMsg reports rejected message to its request. A rejected container
// rejects all of its messages (Telegram Desktop resends them).
func (c *Conn) notifyBadMsg(badMsgID int64, bad *badMessageError) {
	ids := c.containerMessages(badMsgID)
	if ids == nil {
		ids = []int64{badMsgID}
	}
	for _, id := range ids {
		if c.orphanRequest(id) && c.state().notProcessed(id) {
			// Orphan request was rejected without processing, its caller
			// resends it.
			continue
		}
		c.rpc.NotifyError(id, bad)
	}
}

func (c *Conn) handleBadMsg(msgID int64, b *bin.Buffer) error {
	id, err := b.PeekID()
	if err != nil {
		return err
	}
	switch id {
	case mt.BadMsgNotificationTypeID:
		var bad mt.BadMsgNotification
		if err := bad.Decode(b); err != nil {
			return err
		}

		if bad.ErrorCode == codeMessageIDTooHigh {
			// Our clock is ahead of server one: server time can not be
			// trusted to only grow anymore (TDLib reset_server_time_difference).
			// Too low msg_id is corrected by Observe on this message itself.
			c.serverTime.Reset(msgID, c.clock.Now())
		}
		if bad.ErrorCode == codeMessageIDTooLow || bad.ErrorCode == codeMessageIDTooHigh {
			c.log.Warn(context.Background(), "Bad msg_id time, server time corrected",
				log.Int("code", bad.ErrorCode),
				log.Duration("server_time_offset", c.serverTime.Offset()),
			)
		}
		if bad.ErrorCode == codeMessageSeqNoTooLow || bad.ErrorCode == codeMessageSeqNoTooHigh {
			// msg_id and msg_seqno are inconsistent within the session: it can
			// not be continued (Telegram Desktop resetSession).
			c.requestSessionReset(sessionResetBadSeqNo)
		}
		c.notifyBadMsg(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode})
		return nil
	case mt.BadServerSaltTypeID:
		var bad mt.BadServerSalt
		if err := bad.Decode(b); err != nil {
			return err
		}

		// Salt is applied before resending any rejected request.
		c.storeSalt(bad.NewServerSalt)
		c.salts.Reset()
		c.notifyBadMsg(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode, NewSalt: bad.NewServerSalt})
		return nil
	default:
		return errors.Errorf("unknown type id 0x%d", id)
	}
}

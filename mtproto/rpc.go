package mtproto

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/rpc"
)

// maxBadMsgRetries is the maximum number of resends of a request rejected by
// bad_msg_notification that can be fixed by the client.
const maxBadMsgRetries = 3

// Invoke sends input and decodes result into output.
//
// NOTE: Assuming that call contains content message (seqno increment).
func (c *Conn) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	for attempt := 0; ; attempt++ {
		msgID, seqNo := c.nextMsgSeq(true)
		req := rpc.Request{
			MsgID:  msgID,
			SeqNo:  seqNo,
			Input:  input,
			Output: output,
		}

		logger := c.log.With(log.Int64("msg_id", req.MsgID))
		logger.Debug(ctx, "Invoke start")
		err := c.do(ctx, req)
		logger.Debug(ctx, "Invoke end")
		if err == nil {
			return nil
		}

		if attempt >= maxBadMsgRetries {
			return errors.Wrap(err, "rpcDoRequest")
		}
		var badMsgErr *badMessageError
		switch {
		case errors.Is(err, errResendInSession):
		case errors.As(err, &badMsgErr) && c.fixBadMsg(ctx, badMsgErr):
		default:
			return errors.Wrap(err, "rpcDoRequest")
		}
		// The server did not process the message, so it is resent with a
		// new msg_id, like Telegram Desktop replaceMsgId.
		logger.Info(ctx, "Resending request not processed by server",
			log.Error(err),
			log.Int("attempt", attempt+1),
		)
	}
}

// do sends request and waits for result. With shared session the request
// outlives the connection, see session_requests.go and AwaitSession.
func (c *Conn) do(ctx context.Context, req rpc.Request) error {
	if !c.sharedSession {
		return c.rpc.Do(ctx, req)
	}

	s := c.state()
	p := s.track(req.MsgID)

	err := c.rpc.Do(ctx, req)
	acked := errors.Is(err, rpc.ErrClosedAfterAck)
	if !acked && !errors.Is(err, rpc.ErrEngineClosed) {
		s.untrack(p)
		return err
	}

	// Connection was closed before the result: the request may complete in
	// the next connection of the session, see AwaitSession.
	s.orphan(p, acked)
	c.log.Debug(ctx, "Request may complete in resumed session",
		log.Int64("msg_id", req.MsgID),
		log.Bool("acked", acked),
	)
	return &orphanError{cause: err, s: s, p: p, output: req.Output}
}

// fixBadMsg applies the fix for bad_msg_notification and reports whether the
// rejected request can be resent with a new msg_id.
func (c *Conn) fixBadMsg(ctx context.Context, bad *badMessageError) bool {
	switch bad.Code {
	case codeIncorrectServerSalt:
		// Salt was applied by handleBadMsg.
		return true
	case codeMessageIDTooLow, codeMessageIDTooHigh:
		// Server time was corrected by handleBadMsg.
		return true
	case codeInvalidContainer:
		// Messages of rejected container are resent on their own.
		return true
	default:
		return false
	}
}

func (c *Conn) dropRPC(req rpc.Request) error {
	ctx, cancel := context.WithTimeout(context.Background(),
		c.getTimeout(mt.RPCDropAnswerRequestTypeID),
	)
	defer cancel()

	var resp mt.RPCDropAnswerBox
	if err := c.Invoke(ctx, &mt.RPCDropAnswerRequest{
		ReqMsgID: req.MsgID,
	}, &resp); err != nil {
		return err
	}

	switch resp.RpcDropAnswer.(type) {
	case *mt.RPCAnswerDropped, *mt.RPCAnswerDroppedRunning:
		return nil
	case *mt.RPCAnswerUnknown:
		return errors.New("answer unknown")
	default:
		return errors.Errorf("unexpected response type: %T", resp.RpcDropAnswer)
	}
}

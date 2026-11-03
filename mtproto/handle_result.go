package mtproto

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tgerr"
)

func (c *Conn) handleResult(b *bin.Buffer) error {
	ctx := context.Background()
	// Response to an RPC query.
	var res proto.Result
	if err := res.Decode(b); err != nil {
		return errors.Wrap(err, "decode")
	}

	// Now b contains result message.
	b.ResetTo(res.Result)

	msgID := log.Int64("msg_id", res.RequestMessageID)
	c.logWithBuffer(b).Debug(ctx, "Handle result", msgID)

	// Handling gzipped results.
	id, err := b.PeekID()
	if err != nil {
		return err
	}
	if id == proto.GZIPTypeID {
		content, err := gzip(b)
		if err != nil {
			return errors.Wrap(err, "decompress")
		}

		// Replacing buffer so callback will deal with uncompressed data.
		b = content
		c.logWithBuffer(b).Debug(ctx, "Decompressed", msgID)

		// Replacing id with inner id if error is compressed for any reason.
		if id, err = b.PeekID(); err != nil {
			return errors.Wrap(err, "peek id")
		}
	}

	if id == mt.RPCErrorTypeID {
		var rpcErr mt.RPCError
		if err := rpcErr.Decode(b); err != nil {
			return errors.Wrap(err, "error decode")
		}

		c.log.Debug(ctx, "Got error", msgID,
			log.Int("err_code", rpcErr.ErrorCode),
			log.String("err_msg", rpcErr.ErrorMessage),
		)
		err := tgerr.New(rpcErr.ErrorCode, rpcErr.ErrorMessage)
		if c.orphanRequest(res.RequestMessageID) && c.state().deliverError(res.RequestMessageID, err) {
			return nil
		}
		c.rpc.NotifyError(res.RequestMessageID, err)

		return nil
	}
	if id == mt.PongTypeID {
		return c.handlePong(b)
	}

	if c.orphanRequest(res.RequestMessageID) && c.state().deliverResult(res.RequestMessageID, b) {
		c.log.Debug(ctx, "Result delivered to request of resumed session", msgID)
		return nil
	}
	return c.rpc.NotifyResult(res.RequestMessageID, b)
}

// orphanRequest reports whether request may be an orphan of shared session:
// it is not tracked by rpc engine of this connection.
func (c *Conn) orphanRequest(msgID int64) bool {
	return c.sharedSession && !c.rpc.Pending(msgID)
}

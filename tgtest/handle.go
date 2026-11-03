package tgtest

import (
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/gotd/td/transport"
)

func (s *Server) rpcHandle(ctx context.Context, c transport.Conn, b *bin.Buffer) error {
	m := &crypto.EncryptedMessage{}
	if err := m.DecodeWithoutCopy(b); err != nil {
		return errors.Wrap(err, "decode encrypted message")
	}

	key, ok := s.users.getSession(m.AuthKeyID)
	if !ok {
		return errors.New("invalid session")
	}

	msg, err := s.cipher.Decrypt(key, m)
	if err != nil {
		return errors.Wrap(err, "decrypt message")
	}

	session := Session{
		ID:      msg.SessionID,
		AuthKey: key,
	}
	if conn := s.users.createConnection(msg.SessionID, c); !conn.sentCreated() {
		s.log.Debug(ctx, "Send handleSessionCreated event", session.LogAttr())
		salt := int64(binary.LittleEndian.Uint64(key.ID[:]))
		if err := s.sendSessionCreated(ctx, session, msg.MessageID, salt); err != nil {
			return err
		}
	}

	// Buffer now contains plaintext message payload.
	b.ResetTo(msg.Data())

	if err := s.handle(&Request{
		DC:         s.dcID,
		Session:    session,
		MsgID:      msg.MessageID,
		Buf:        b,
		RequestCtx: ctx,
	}); err != nil {
		return errors.Wrap(err, "handle")
	}

	return nil
}

func (s *Server) handle(req *Request) error {
	in := req.Buf
	id, err := in.PeekID()
	if err != nil {
		return errors.Wrap(err, "peek id")
	}

	s.log.Debug(req.RequestCtx, "Got request",
		req.Session.LogAttr(),
		log.Int64("msg_id", req.MsgID),
		log.String("type", s.types.Get(id)),
	)

	conn, hasConn := s.users.getConnection(req.Session.ID)
	if hasConn {
		conn.markSeen(req.MsgID)
	}

	// TODO(tdakkota): unpack all containers
	switch id {
	case mt.MsgsStateReqTypeID:
		var stateReq mt.MsgsStateReq
		if err := stateReq.Decode(in); err != nil {
			return err
		}
		// 4: message received, 2: message not received.
		info := make([]byte, len(stateReq.MsgIDs))
		for i, msgID := range stateReq.MsgIDs {
			info[i] = 2
			if hasConn && conn.isSeen(msgID) {
				info[i] = 4
			}
		}
		return s.Send(req.RequestCtx, req.Session, proto.MessageServerResponse, &mt.MsgsStateInfo{
			ReqMsgID: req.MsgID,
			Info:     info,
		})

	case mt.PingDelayDisconnectRequestTypeID:
		pingReq := mt.PingDelayDisconnectRequest{}
		if err := pingReq.Decode(in); err != nil {
			return err
		}

		return s.SendPong(req, pingReq.PingID)
	case mt.PingRequestTypeID:
		pingReq := mt.PingRequest{}
		if err := pingReq.Decode(in); err != nil {
			return err
		}

		return s.SendPong(req, pingReq.PingID)

	case mt.GetFutureSaltsRequestTypeID:
		saltsRequest := mt.GetFutureSaltsRequest{}
		if err := saltsRequest.Decode(in); err != nil {
			return err
		}

		return s.SendEternalSalt(req)

	case tg.AuthBindTempAuthKeyRequestTypeID:
		var bind tg.AuthBindTempAuthKeyRequest
		if err := bind.Decode(in); err != nil {
			return err
		}
		// The server can decrypt bind message only with a permanent key it
		// knows, otherwise it reports ENCRYPTED_MESSAGE_INVALID.
		var perm crypto.AuthKey
		perm.SetIntID(bind.PermAuthKeyID)
		if _, ok := s.users.getSession(perm.ID); !ok {
			return s.SendErr(req, tgerr.New(400, tg.ErrEncryptedMessageInvalid))
		}
		return s.SendBool(req, true)

	case mt.RPCDropAnswerRequestTypeID:
		drop := mt.RPCDropAnswerRequest{}
		if err := drop.Decode(in); err != nil {
			return err
		}

		return s.SendResult(req, &mt.RPCAnswerDroppedRunning{})

	case proto.GZIPTypeID:
		var content proto.GZIP
		if err := content.Decode(in); err != nil {
			return errors.Wrap(err, "gzip")
		}
		req.Buf = &bin.Buffer{Buf: content.Data}

	case proto.MessageContainerTypeID:
		var container proto.MessageContainer
		if err := container.Decode(in); err != nil {
			return errors.Wrap(err, "container")
		}

		var err error
		for _, msg := range container.Messages {
			err = multierr.Append(err, s.handle(&Request{
				DC:         req.DC,
				Session:    req.Session,
				MsgID:      msg.ID,
				Buf:        &bin.Buffer{Buf: msg.Body},
				RequestCtx: req.RequestCtx,
			}))
		}
		return err
	}

	if err := s.handler.OnMessage(s, req); err != nil {
		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) {
			return s.SendErr(req, rpcErr)
		}
		return err
	}

	return nil
}

package mtproto

import "github.com/gotd/td/proto"

func (c *Conn) newMessageID() int64 {
	return c.messageID.New(proto.MessageFromClient)
}

// serverTimeOf returns server time tracker of message id source, or a new one
// if source does not support it (then generated ids are not corrected).
func serverTimeOf(src MessageIDSource) *proto.ServerTime {
	if s, ok := src.(interface{ ServerTime() *proto.ServerTime }); ok {
		return s.ServerTime()
	}
	return &proto.ServerTime{}
}

package proto

import (
	"fmt"
	"sync"
	"time"
)

// Message identifiers are coupled to message creation time.
//
// https://core.telegram.org/mtproto/description#message-identifier-msg-id

const (
	yieldClient         = 0
	yieldServerResponse = 1
	yieldFromServer     = 3

	messageIDModulo = 4
)

func newMessageID(nowNano int64, yield int) int64 {
	const nano = 1e9
	// Must approximately equal unixtime*2^32.

	// Important: to counter replay-attacks the lower 32 bits of msg_id
	// passed by the client must not be empty and must present a
	// fractional part of the time point when the message was created.
	intPart := nowNano / nano
	fracPart := nowNano % nano

	// Ensure that fracPart % 4 == 0.
	fracPart &= -messageIDModulo
	// Adding modulo 4 yield to ensure message type.
	fracPart += int64(yield)

	return (intPart << 32) | fracPart
}

// MessageID represents 64-bit message id.
type MessageID int64

func (id MessageID) String() string {
	return fmt.Sprintf("%x (%s, %s)",
		int64(id), id.Type(), id.Time().Format(time.RFC3339),
	)
}

// MessageType is type of message determined by message id.
//
// A message is rejected over 300 seconds after it is created or
// 30 seconds before it is created (this is needed to protect from replay attacks).
//
// The identifier of a message container must be strictly greater than those of
// its nested messages.
type MessageType byte

const (
	// MessageUnknown reports that message id has unknown time and probably
	// should be ignored.
	MessageUnknown MessageType = iota
	// MessageFromClient is client message identifiers.
	MessageFromClient
	// MessageServerResponse is a response to a client message.
	MessageServerResponse
	// MessageFromServer is a message from the server.
	MessageFromServer
)

func (m MessageType) String() string {
	switch m {
	case MessageFromClient:
		return "FromClient"
	case MessageServerResponse:
		return "ServerResponse"
	case MessageFromServer:
		return "FromServer"
	default:
		return "Unknown"
	}
}

// Time returns approximate time when MessageID were generated.
func (id MessageID) Time() time.Time {
	intPart := int64(id) >> 32
	fracPart := int64(int32(id))
	return time.Unix(intPart, fracPart).UTC()
}

// Type returns message type.
func (id MessageID) Type() MessageType {
	switch id % messageIDModulo {
	case yieldClient:
		return MessageFromClient
	case yieldServerResponse:
		return MessageServerResponse
	case yieldFromServer:
		return MessageFromServer
	default:
		return MessageUnknown
	}
}

// NewMessageID returns new message id for provided time and type.
func NewMessageID(now time.Time, typ MessageType) MessageID {
	return NewMessageIDNano(now.UnixNano(), typ)
}

// NewMessageIDNano returns new message id for provided current unix
// nanoseconds and type.
func NewMessageIDNano(nano int64, typ MessageType) MessageID {
	var yield int
	switch typ {
	case MessageFromClient:
		yield = yieldClient
	case MessageFromServer:
		yield = yieldFromServer
	case MessageServerResponse:
		yield = yieldServerResponse
	default:
		yield = yieldClient
	}
	return MessageID(newMessageID(nano, yield))
}

// MessageIDGen is message id generator that provides collision prevention.
//
// The main reason of such structure is that now() can return same time during
// multiple calls and that leads to duplicate message id.
type MessageIDGen struct {
	mux  sync.Mutex
	nano int64
	now  func() time.Time
	// time is applied to generated ids, see ServerTime.
	time ServerTime
}

// ServerTime returns server time tracker applied to generated message ids.
func (g *MessageIDGen) ServerTime() *ServerTime {
	return &g.time
}

// New generates new message id for provided type, protecting from collisions
// that are caused by low system time resolution.
func (g *MessageIDGen) New(t MessageType) int64 {
	g.mux.Lock()
	defer g.mux.Unlock()

	// Minimum resolution is required because id is only approximately
	// equal to unix nano time, some part is replaced by message type.
	const minResolutionNanos = 10

	nano := g.now().UnixNano() + int64(g.time.Offset())
	if nano > g.nano {
		g.nano = nano
	} else {
		g.nano += minResolutionNanos
	}

	return int64(NewMessageIDNano(g.nano, t))
}

// NewMessageIDGen creates new message id generator.
//
// Current time will be provided by now() function.
//
// This generator compensates time resolution problem removing
// probability of id collision.
//
// Such problem can be observed for relatively high RPS, sequential calls to
// time.Now() will return same time which leads to equal ids.
func NewMessageIDGen(now func() time.Time) *MessageIDGen {
	return &MessageIDGen{
		now: now,
	}
}

// MessageIDStatus is result of checking incoming message id against
// MessageIDBuf.
type MessageIDStatus int

const (
	// MessageIDAccepted means that message id is new and was stored.
	MessageIDAccepted MessageIDStatus = iota
	// MessageIDDuplicate means that message id equals to one of stored values:
	// message was already processed and must be ignored (but still
	// acknowledged if it requires acknowledgement).
	MessageIDDuplicate
	// MessageIDTooOld means that buffer is full and message id is lower than
	// all stored values: it is impossible to tell whether message was processed,
	// so session state is unreliable.
	//
	// TDLib (check_message_id_duplicates error 2), Telegram Desktop
	// (ReceivedIdsManager::Result::TooOld) and Telegram Android
	// (isMessageIdProcessed == 2) start a new session in that case.
	MessageIDTooOld
)

// String implements fmt.Stringer.
func (s MessageIDStatus) String() string {
	switch s {
	case MessageIDAccepted:
		return "accepted"
	case MessageIDDuplicate:
		return "duplicate"
	case MessageIDTooOld:
		return "too_old"
	default:
		return fmt.Sprintf("MessageIDStatus(%d)", int(s))
	}
}

// MessageIDBuf stores last N message ids and is used in replay attack mitigation.
type MessageIDBuf struct {
	mux sync.Mutex
	// ids are stored message ids in ascending order, len(ids) <= n.
	ids []int64
	n   int
}

// NewMessageIDBuf initializes new message id buffer for last N stored values.
func NewMessageIDBuf(n int) *MessageIDBuf {
	if n < 1 {
		n = 1
	}
	return &MessageIDBuf{
		ids: make([]int64, 0, n),
		n:   n,
	}
}

// Consume returns false if message should be discarded.
//
// See Check to distinguish duplicate and too old message ids.
func (b *MessageIDBuf) Consume(newID int64) bool {
	return b.Check(newID) == MessageIDAccepted
}

// Check checks message id and stores it if it is accepted.
func (b *MessageIDBuf) Check(newID int64) MessageIDStatus {
	// In addition, the identifiers (msg_id) of the last N messages received
	// from the other side must be stored, and if a message comes in with an
	// msg_id lower than all or equal to any of the stored values, that message
	// is to be ignored. Otherwise, the new message msg_id is added to the set,
	// and, if the number of stored msg_id values is greater than N, the oldest
	// (i. e. the lowest) is discarded.
	//
	// https://core.telegram.org/mtproto/security_guidelines#checking-msg-id
	if newID <= 0 {
		// Message ids are derived from unix time and are always positive.
		return MessageIDTooOld
	}

	b.mux.Lock()
	defer b.mux.Unlock()

	ids := b.ids
	if len(ids) == 0 || newID > ids[len(ids)-1] {
		// Fast path: ids are mostly increasing.
		b.insert(len(ids), newID)
		return MessageIDAccepted
	}
	if len(ids) >= b.n && newID < ids[0] {
		return MessageIDTooOld
	}

	// Binary search for the first id >= newID.
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if ids[mid] < newID {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(ids) && ids[lo] == newID {
		return MessageIDDuplicate
	}
	b.insert(lo, newID)
	return MessageIDAccepted
}

// Contains reports whether message id is stored.
func (b *MessageIDBuf) Contains(id int64) bool {
	b.mux.Lock()
	defer b.mux.Unlock()

	lo, hi := 0, len(b.ids)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if b.ids[mid] < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(b.ids) && b.ids[lo] == id
}

// insert inserts id at position i, evicting the lowest id if buffer is full.
//
// Caller must hold mux.
func (b *MessageIDBuf) insert(i int, id int64) {
	if len(b.ids) >= b.n {
		// Evict the lowest value.
		copy(b.ids, b.ids[1:i])
		b.ids[i-1] = id
		return
	}
	b.ids = append(b.ids, 0)
	copy(b.ids[i+1:], b.ids[i:])
	b.ids[i] = id
}

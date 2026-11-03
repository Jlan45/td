package proto

import (
	"sync"
	"sync/atomic"
	"time"
)

// ServerTime tracks difference between server and local clocks.
//
// Message identifiers must be close to server time: a message is rejected by
// server if its msg_id is over 300 seconds in the past or 30 seconds in the
// future (bad_msg_notification 16 and 17), and the client must ignore server
// messages outside of the same window. With a skewed local clock neither side
// accepts messages of the other one, so the difference is measured from
// msg_id of server messages and applied to both generation and validation.
//
// Update rules follow TDLib AuthData::update_server_time_difference: the
// first sample is taken as is, later ones only increase the difference
// (network latency can only make server time look older), and Reset forgets
// the history, e.g. on bad_msg_notification 17 (msg_id too high).
//
// Zero value is ready to use and means no difference.
type ServerTime struct {
	mux    sync.Mutex
	diff   atomic.Int64 // time.Duration
	synced atomic.Bool
}

// Offset returns server time minus local time.
func (s *ServerTime) Offset() time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(s.diff.Load())
}

// Synced reports whether difference was measured from a server message.
func (s *ServerTime) Synced() bool {
	if s == nil {
		return false
	}
	return s.synced.Load()
}

// Now returns current server time estimation for local time now.
func (s *ServerTime) Now(now time.Time) time.Time {
	return now.Add(s.Offset())
}

// serverMessageTime returns time of server message id.
//
// Lower 32 bits of server msg_id are not guaranteed to be nanoseconds, so
// only seconds are used, like TDLib does.
func serverMessageTime(msgID int64) time.Time {
	return time.Unix(msgID>>32, 0)
}

// Observe updates difference using msg_id of server message received at
// local time now. Returns true if difference was changed.
func (s *ServerTime) Observe(serverMsgID int64, now time.Time) bool {
	diff := serverMessageTime(serverMsgID).Sub(now)

	s.mux.Lock()
	defer s.mux.Unlock()
	if s.synced.Load() && diff <= time.Duration(s.diff.Load()) {
		return false
	}
	s.diff.Store(int64(diff))
	s.synced.Store(true)
	return true
}

// Reset sets difference using msg_id of server message received at local
// time now, allowing next Observe to decrease it.
func (s *ServerTime) Reset(serverMsgID int64, now time.Time) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.diff.Store(int64(serverMessageTime(serverMsgID).Sub(now)))
	s.synced.Store(false)
}

// Set sets difference directly, e.g. restored from persistent storage.
// Next Observe can change it in any direction.
func (s *ServerTime) Set(offset time.Duration) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.diff.Store(int64(offset))
	s.synced.Store(false)
}

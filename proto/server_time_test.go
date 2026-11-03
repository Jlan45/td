package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/testutil"
)

func serverID(t time.Time) int64 {
	return int64(NewMessageID(t, MessageFromServer))
}

func TestServerTime(t *testing.T) {
	a := require.New(t)
	local := testutil.Date().Truncate(time.Second)
	var s ServerTime
	a.Zero(s.Offset())
	a.False(s.Synced())

	// Server is 10 minutes ahead.
	a.True(s.Observe(serverID(local.Add(10*time.Minute)), local))
	a.True(s.Synced())
	a.Equal(10*time.Minute, s.Offset())

	// Delayed (older looking) message does not decrease difference.
	a.False(s.Observe(serverID(local.Add(9*time.Minute)), local))
	a.Equal(10*time.Minute, s.Offset())

	// Newer one increases it.
	a.True(s.Observe(serverID(local.Add(11*time.Minute)), local))
	a.Equal(11*time.Minute, s.Offset())

	// Reset (msg_id too high) allows decreasing.
	s.Reset(serverID(local.Add(-time.Minute)), local)
	a.False(s.Synced())
	a.Equal(-time.Minute, s.Offset())
	a.True(s.Observe(serverID(local.Add(-2*time.Minute)), local))
	a.Equal(-2*time.Minute, s.Offset())

	s.Set(time.Hour)
	a.Equal(time.Hour, s.Offset())
	a.False(s.Synced())
	a.Equal(local.Add(time.Hour), s.Now(local))

	var nilTime *ServerTime
	a.Zero(nilTime.Offset())
	a.False(nilTime.Synced())
}

func TestServerTimeSecondsOnly(t *testing.T) {
	// Lower bits of server msg_id are not nanoseconds.
	local := time.Unix(1000, 0)
	var s ServerTime
	s.Observe(int64(1010)<<32|0xfffffffd, local)
	require.Equal(t, 10*time.Second, s.Offset())
}

func TestMessageIDGenServerTime(t *testing.T) {
	a := require.New(t)
	local := testutil.Date().Truncate(time.Second)
	gen := NewMessageIDGen(func() time.Time { return local })
	gen.ServerTime().Set(time.Hour)

	id := MessageID(gen.New(MessageFromClient))
	a.Equal(local.Add(time.Hour).Unix(), id.Time().Unix())
}

func TestMessageIDGenZeroAlloc(t *testing.T) {
	gen := NewMessageIDGen(time.Now)
	gen.ServerTime().Set(time.Second)
	testutil.ZeroAlloc(t, func() {
		_ = gen.New(MessageFromClient)
	})
}

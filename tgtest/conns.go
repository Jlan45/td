package tgtest

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/transport"
)

type connection struct {
	mux sync.Mutex
	transport.Conn
	sent atomic.Bool
}

func (conn *connection) sentCreated() bool {
	return conn.sent.Swap(true)
}

// current returns the latest transport connection of the session.
func (conn *connection) current() transport.Conn {
	conn.mux.Lock()
	defer conn.mux.Unlock()
	return conn.Conn
}

// Close closes the latest transport connection of the session.
func (conn *connection) Close() error {
	return conn.current().Close()
}

// users contains all server connections and sessions.
type users struct {
	sessions    map[[8]byte]crypto.AuthKey
	sessionsMux sync.Mutex

	conns    map[int64]*connection
	connsMux sync.Mutex
}

func newUsers() *users {
	return &users{
		conns:    map[int64]*connection{},
		sessions: map[[8]byte]crypto.AuthKey{},
	}
}

// createConnection returns connection state of session key, binding the session
// to tConn. Like Telegram servers, a session that moved to a new transport
// connection (client reconnect) is served over the newest one and does not get
// new_session_created again.
func (c *users) createConnection(key int64, tConn transport.Conn) *connection {
	c.connsMux.Lock()
	defer c.connsMux.Unlock()

	if v, ok := c.conns[key]; ok {
		v.mux.Lock()
		v.Conn = tConn
		v.mux.Unlock()
		return v
	}

	conn := &connection{
		Conn: tConn,
	}
	c.conns[key] = conn
	return conn
}

func (c *users) getConnection(key int64) (conn *connection, ok bool) {
	c.connsMux.Lock()
	conn, ok = c.conns[key]
	c.connsMux.Unlock()

	return
}

func (c *users) deleteConnection(key int64) {
	c.connsMux.Lock()
	conn := c.conns[key]
	if conn != nil {
		_ = conn.Close()
	}
	delete(c.conns, key)
	c.connsMux.Unlock()
}

func (c *users) addSession(key crypto.AuthKey) {
	c.sessionsMux.Lock()
	c.sessions[key.ID] = key
	c.sessionsMux.Unlock()
}

func (c *users) getSession(k [8]byte) (s crypto.AuthKey, ok bool) {
	c.connsMux.Lock()
	s, ok = c.sessions[k]
	c.connsMux.Unlock()

	return
}

func (c *users) Close() error {
	c.connsMux.Lock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.connsMux.Unlock()

	return nil
}

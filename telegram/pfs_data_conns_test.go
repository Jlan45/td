package telegram

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/log"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/telegram/internal/manager"
	"github.com/gotd/td/tg"
)

type capturedConn struct {
	mode manager.ConnMode
	dc   int
	opts mtproto.Options
	conn manager.ConnOptions
}

type connCapture struct {
	mux   sync.Mutex
	conns []capturedConn
}

func (c *connCapture) constructor() connConstructor {
	return func(
		_ mtproto.Dialer,
		mode manager.ConnMode,
		_ int,
		opts mtproto.Options,
		connOpts manager.ConnOptions,
	) pool.Conn {
		c.mux.Lock()
		c.conns = append(c.conns, capturedConn{mode: mode, dc: connOpts.DC, opts: opts, conn: connOpts})
		c.mux.Unlock()
		return newIdlePoolConn()
	}
}

func (c *connCapture) last(t *testing.T) capturedConn {
	t.Helper()
	c.mux.Lock()
	defer c.mux.Unlock()
	require.NotEmpty(t, c.conns)
	return c.conns[len(c.conns)-1]
}

const (
	dataPFSPrimaryDC = 2
	dataPFSOtherDC   = 4
)

func newDataConnPFSTestClient(t *testing.T) (*Client, *connCapture, crypto.AuthKey) {
	t.Helper()

	c := &Client{log: log.For(log.Nop), primarySession: mtproto.NewSessionState()}
	c.init()
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	c.dataConnPFS = true
	c.onTransfer = func(context.Context, *Client, func(context.Context) error) error {
		return nil
	}
	c.cfg.Store(tg.Config{
		ThisDC: dataPFSPrimaryDC,
		DCOptions: []tg.DCOption{
			{ID: dataPFSPrimaryDC, IPAddress: "127.0.0.1", Port: 443},
			{ID: dataPFSOtherDC, IPAddress: "127.0.0.2", Port: 443},
		},
	})

	key := crypto.Key{7}.WithID()
	primary := pool.Session{DC: dataPFSPrimaryDC, AuthKey: key, Salt: 11}
	c.session = pool.NewSyncSession(primary)
	c.sessions[dataPFSPrimaryDC] = pool.NewSyncSession(primary)

	capture := &connCapture{}
	c.create = capture.constructor()
	return c, capture, key
}

func TestClientDataConnPFSPrimaryConnUnchanged(t *testing.T) {
	c, capture, key := newDataConnPFSTestClient(t)

	c.createPrimaryConn(nil)

	got := capture.last(t)
	require.False(t, got.opts.EnablePFS)
	require.Equal(t, key, got.opts.Key)
	require.True(t, got.opts.PermKey.Zero())
}

func TestClientDataConnPFSPool(t *testing.T) {
	c, capture, key := newDataConnPFSTestClient(t)

	var deadCalls int
	c.onDead = func(error) { deadCalls++ }

	inv, err := c.Pool(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.Equal(t, manager.ConnModeData, got.mode)
	require.True(t, got.opts.EnablePFS)
	require.Equal(t, key, got.opts.PermKey, "data connection must reuse primary permanent key")
	require.True(t, got.opts.Key.Zero(), "temporary key must be generated per connection")

	// Session of the data connection must not overwrite the primary session.
	require.NoError(t, got.conn.Handler.OnSession(tg.Config{ThisDC: dataPFSPrimaryDC}, mtproto.Session{
		Key:     crypto.Key{9}.WithID(),
		PermKey: key,
		Salt:    99,
	}))
	require.Equal(t, int64(11), c.session.Load().Salt)

	// Temp key bind failure must not drop the primary permanent key.
	got.conn.OnDead(mtproto.ErrPFSDropKeysRequired)
	require.Equal(t, key, c.session.Load().AuthKey)
	require.Equal(t, key, c.sessions[dataPFSPrimaryDC].Load().AuthKey)
	require.Equal(t, 1, deadCalls)
}

func TestClientDataConnPFSOtherDC(t *testing.T) {
	c, capture, _ := newDataConnPFSTestClient(t)

	inv, err := c.DC(context.Background(), dataPFSOtherDC, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.Equal(t, dataPFSOtherDC, got.dc)
	require.True(t, got.opts.EnablePFS)
	require.NotNil(t, got.conn.Setup, "fresh non-primary DC key must still be authorized via transfer")
	_, isPrimaryData := got.conn.Handler.(primaryDataConnHandler)
	require.False(t, isPrimaryData)

	// Non-primary DC keys are owned by the pool and are still dropped on reset.
	c.sessions[dataPFSOtherDC].Store(pool.Session{DC: dataPFSOtherDC, AuthKey: crypto.Key{5}.WithID()})
	got.conn.OnDead(mtproto.ErrPFSDropKeysRequired)
	require.True(t, c.sessions[dataPFSOtherDC].Load().AuthKey.Zero())
}

func TestClientDataConnPFSPrimaryDCViaDC(t *testing.T) {
	c, capture, key := newDataConnPFSTestClient(t)

	inv, err := c.DC(context.Background(), dataPFSPrimaryDC, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.True(t, got.opts.EnablePFS)
	require.Equal(t, key, got.opts.PermKey)
	_, isPrimaryData := got.conn.Handler.(primaryDataConnHandler)
	require.True(t, isPrimaryData)

	got.conn.OnDead(mtproto.ErrPFSDropKeysRequired)
	require.Equal(t, key, c.session.Load().AuthKey)
	require.Equal(t, key, c.sessions[dataPFSPrimaryDC].Load().AuthKey)
}

func TestClientDataConnPFSDisabledKeepsPermKey(t *testing.T) {
	c, capture, key := newDataConnPFSTestClient(t)
	c.dataConnPFS = false

	inv, err := c.Pool(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.False(t, got.opts.EnablePFS)
	require.Equal(t, key, got.opts.Key)
}

func TestClientDataConnPFSCDNStaysPermKeyOnly(t *testing.T) {
	c := newCDNPoolTestClient()
	defer c.cancel()
	defer closeCachedCDNPools(c)
	c.dataConnPFS = true

	capture := &connCapture{}
	c.create = capture.constructor()

	inv, err := c.CDN(context.Background(), 203, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.Equal(t, manager.ConnModeCDN, got.mode)
	require.False(t, got.opts.EnablePFS)
}

func TestNewClientDataConnPFSOption(t *testing.T) {
	for _, tt := range []struct {
		name    string
		opt     Options
		dataPFS bool
		basePFS bool
	}{
		{name: "off", opt: Options{}},
		{name: "data only", opt: Options{EnablePFSForDataConns: true}, dataPFS: true},
		{name: "global wins", opt: Options{EnablePFS: true, EnablePFSForDataConns: true}, basePFS: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(1, "hash", tt.opt)
			require.Equal(t, tt.dataPFS, c.dataConnPFS)
			require.Equal(t, tt.basePFS, c.opts.EnablePFS)
		})
	}
}

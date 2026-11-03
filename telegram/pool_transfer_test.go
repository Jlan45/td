package telegram

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
)

func TestClientDataPoolTransfer(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dc       int
		media    bool
		transfer bool
	}{
		{name: "PrimaryMediaOnly", dc: dataPFSPrimaryDC, media: true},
		{name: "PrimaryDC", dc: dataPFSPrimaryDC},
		{name: "OtherMediaOnly", dc: dataPFSOtherDC, media: true, transfer: true},
		{name: "OtherDC", dc: dataPFSOtherDC, transfer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, capture, key := newDataConnPFSTestClient(t)
			c.dataConnPFS = false
			var transfers int
			c.onTransfer = func(context.Context, *Client, func(context.Context) error) error {
				transfers++
				return errors.New("transfer must not be called")
			}

			open := c.DC
			if tt.media {
				open = c.MediaOnly
			}
			inv, err := open(context.Background(), tt.dc, 1)
			if tt.transfer {
				require.Error(t, err)
				require.Equal(t, 1, transfers)
				return
			}
			require.NoError(t, err)
			defer func() { require.NoError(t, inv.Close()) }()
			require.NoError(t, inv.Invoke(context.Background(), nil, nil))
			require.Zero(t, transfers)

			got := capture.last(t)
			require.Equal(t, tt.dc, got.dc)
			require.Equal(t, key, got.opts.Key, "primary DC data session must reuse the primary key")
			require.Nil(t, got.conn.Setup)
		})
	}
}

// Data pools to the primary DC must use the primary key even before the
// primary connection reported its session into the per-DC map, and must
// never write the primary session.
func TestClientDataPoolPrimaryDCUsesPrimaryKey(t *testing.T) {
	c, capture, key := newDataConnPFSTestClient(t)
	c.dataConnPFS = false
	delete(c.sessions, dataPFSPrimaryDC)

	inv, err := c.MediaOnly(context.Background(), dataPFSPrimaryDC, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))

	got := capture.last(t)
	require.Equal(t, key, got.opts.Key)
	_, isPrimaryData := got.conn.Handler.(primaryDataConnHandler)
	require.True(t, isPrimaryData, "primary DC data connection must not write primary session")

	got.conn.OnDead(mtproto.ErrPFSDropKeysRequired)
	require.Equal(t, key, c.session.Load().AuthKey, "primary key must not be dropped by data connection")
}

func TestClientDataPoolPrimaryDCWithoutKey(t *testing.T) {
	c, _, _ := newDataConnPFSTestClient(t)
	c.dataConnPFS = false
	c.session.Store(pool.Session{DC: dataPFSPrimaryDC})

	_, err := c.MediaOnly(context.Background(), dataPFSPrimaryDC, 1)
	require.Error(t, err, "data pool must not generate an unauthorized key for the primary DC")
}

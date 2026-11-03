package telegram

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
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

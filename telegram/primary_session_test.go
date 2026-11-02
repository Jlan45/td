package telegram

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Successive primary connections share one MTProto session, like official
// clients: a reconnect must not open a second main session on the same
// authorization (AUTH_KEY_DUPLICATED).
func TestPrimaryConnsShareSession(t *testing.T) {
	c, capture, _ := newDataConnPFSTestClient(t)
	c.primarySession = nil

	c.createPrimaryConn(nil)
	require.Nil(t, capture.last(t).opts.Session, "client without session state must not share sessions")

	c, capture, _ = newDataConnPFSTestClient(t)
	c.createPrimaryConn(nil)
	first := capture.last(t).opts.Session
	require.NotNil(t, first)

	c.createPrimaryConn(nil)
	require.Same(t, first, capture.last(t).opts.Session, "reconnect must reuse the primary session")
}

// Pool and data connections run concurrently with the primary connection, so
// they must never share its session.
func TestPoolConnsDoNotSharePrimarySession(t *testing.T) {
	c, capture, _ := newDataConnPFSTestClient(t)
	c.dataConnPFS = false

	inv, err := c.Pool(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, inv.Close()) }()
	require.NoError(t, inv.Invoke(context.Background(), nil, nil))
	require.Nil(t, capture.last(t).opts.Session)

	other, err := c.DC(context.Background(), dataPFSOtherDC, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, other.Close()) }()
	require.NoError(t, other.Invoke(context.Background(), nil, nil))
	require.Nil(t, capture.last(t).opts.Session)
}

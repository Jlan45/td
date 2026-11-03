package telegram

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/tg"
)

func newVerifyTestClient(t *testing.T) (*Client, *connCapture, crypto.AuthKey) {
	t.Helper()
	c, capture, key := newDataConnPFSTestClient(t)
	c.dataConnPFS = false
	return c, capture, key
}

// After -404 for the primary key, the next primary connection must verify the
// key by binding a temporary key to it instead of the key being regenerated.
func TestPrimaryKeyVerifiedAfterAuthKeyNotFound(t *testing.T) {
	a := require.New(t)
	c, capture, key := newVerifyTestClient(t)

	c.createPrimaryConn(nil)
	a.False(capture.last(t).opts.EnablePFS)

	c.handlePrimaryConnDead(errors.Wrap(mtproto.ErrAuthKeyNotFound, "read"))
	a.Equal(key, c.session.Load().AuthKey, "key must be kept until verified")

	c.createPrimaryConn(nil)
	got := capture.last(t)
	a.True(got.opts.EnablePFS, "verification connection must use PFS")
	a.Equal(key, got.opts.PermKey)

	// Successful bind: session reported with permanent key.
	a.NoError(c.onSession(tg.Config{ThisDC: dataPFSPrimaryDC}, mtproto.Session{
		Key:     crypto.Key{9}.WithID(),
		PermKey: key,
	}))
	a.False(c.verifyPrimaryKey.Load())
	a.Equal(key, c.session.Load().AuthKey)

	c.createPrimaryConn(nil)
	a.False(capture.last(t).opts.EnablePFS, "PFS is used only for verification")
}

// Failed verification drops the key like before.
func TestPrimaryKeyDroppedAfterFailedVerification(t *testing.T) {
	a := require.New(t)
	c, _, _ := newVerifyTestClient(t)

	c.handlePrimaryConnDead(mtproto.ErrAuthKeyNotFound)
	c.handlePrimaryConnDead(mtproto.ErrPFSDropKeysRequired)
	a.True(c.session.Load().AuthKey.Zero())
}

// Non-primary DC key can be re-created and authorized again via export/import.
func TestOtherDCKeyDroppedOnAuthKeyNotFound(t *testing.T) {
	a := require.New(t)
	c, _, _ := newVerifyTestClient(t)
	other := crypto.Key{3}.WithID()
	c.sessions[dataPFSOtherDC] = pool.NewSyncSession(pool.Session{DC: dataPFSOtherDC, AuthKey: other})

	c.handleDCConnDead(dataPFSOtherDC, mtproto.ErrAuthKeyNotFound)
	a.True(c.sessions[dataPFSOtherDC].Load().AuthKey.Zero())
	a.False(c.verifyPrimaryKey.Load())
}

package telegram_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// When the server reports -404 for the stored key, the client verifies the key
// with a PFS bind before dropping it (TDLib "Use PFS to check main key"). The
// test server does not know the stored key, so verification fails and a new
// key is generated, after which the client works normally.
func TestClientUnknownStoredKeyVerifiedThenReplaced(t *testing.T) {
	var stale crypto.Key
	_, err := io.ReadFull(rand.Reader, stale[:])
	require.NoError(t, err)
	staleKey := stale.WithID()

	storage := &session.StorageMemory{}
	loader := session.Loader{Storage: storage}
	require.NoError(t, loader.Save(context.Background(), &session.Data{
		DC:        2,
		AuthKey:   staleKey.Value[:],
		AuthKeyID: staleKey.ID[:],
	}))

	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		s.Cluster.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
			func(server *tgtest.Server, req *tgtest.Request) error {
				return server.SendVector(req, &tg.User{ID: 10, AccessHash: 10, Username: "username"})
			},
		)
	}, func(ctx context.Context, c clientSetup) error {
		opts := c.Options
		opts.SessionStorage = storage
		opts.ReconnectionBackoff = nil
		client := telegram.NewClient(1, "hash", opts)

		return client.Run(ctx, func(ctx context.Context) error {
			if _, err := client.Self(ctx); err != nil {
				return errors.Wrap(err, "self")
			}
			c.Complete()
			return nil
		})
	})(t)

	data, err := loader.Load(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, staleKey.ID[:], data.AuthKeyID, "unknown key must be replaced after verification")
}

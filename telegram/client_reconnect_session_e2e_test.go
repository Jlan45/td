package telegram_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// killableResolver records transport connections, so a test can break the
// primary connection like a proxy dropping it.
type killableResolver struct {
	dcs.Resolver

	mux   sync.Mutex
	conns []transport.Conn
}

func (r *killableResolver) Primary(ctx context.Context, dc int, list dcs.List) (transport.Conn, error) {
	conn, err := r.Resolver.Primary(ctx, dc, list)
	if err != nil {
		return nil, err
	}
	r.mux.Lock()
	r.conns = append(r.conns, conn)
	r.mux.Unlock()
	return conn, nil
}

func (r *killableResolver) killLast() int {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.conns) > 0 {
		_ = r.conns[len(r.conns)-1].Close()
	}
	return len(r.conns)
}

func (r *killableResolver) dials() int {
	r.mux.Lock()
	defer r.mux.Unlock()
	return len(r.conns)
}

// On reconnect the client must keep its MTProto session (session_id) and move it
// to the new TCP connection, like official clients. Opening a new session_id on
// the same authorization while the old TCP connection may still be alive on the
// server side triggers AUTH_KEY_DUPLICATED and revokes the key.
func TestClientReconnectKeepsSession(t *testing.T) {
	var (
		mux      sync.Mutex
		sessions []int64
		called   = make(chan struct{}, 16)
	)
	resolver := &killableResolver{}

	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		s.Cluster.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
			func(server *tgtest.Server, req *tgtest.Request) error {
				mux.Lock()
				sessions = append(sessions, req.Session.ID)
				mux.Unlock()
				select {
				case called <- struct{}{}:
				default:
				}
				return server.SendVector(req, &tg.User{ID: 10, AccessHash: 10, Username: "username"})
			},
		)
	}, func(ctx context.Context, c clientSetup) error {
		opts := c.Options
		resolver.Resolver = opts.Resolver
		opts.Resolver = resolver
		opts.ReconnectionBackoff = nil
		client := telegram.NewClient(1, "hash", opts)

		wait := func() error {
			select {
			case <-called:
				return nil
			case <-time.After(30 * time.Second):
				return errors.New("users.getUsers was not called")
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return client.Run(ctx, func(ctx context.Context) error {
			// Self() on the first connection.
			if err := wait(); err != nil {
				return err
			}
			// Proxy drops the connection: client reconnects and calls Self() again.
			resolver.killLast()
			if err := wait(); err != nil {
				return err
			}
			require.GreaterOrEqual(c.TB, resolver.dials(), 2, "client must have reconnected")

			// RPC on the resumed session works.
			if _, err := client.Self(ctx); err != nil {
				return errors.Wrap(err, "self after reconnect")
			}
			c.Complete()
			return nil
		})
	})(t)

	mux.Lock()
	defer mux.Unlock()
	require.GreaterOrEqual(t, len(sessions), 3)
	for _, id := range sessions {
		require.Equal(t, sessions[0], id, "all requests must use one MTProto session across reconnects")
	}
}

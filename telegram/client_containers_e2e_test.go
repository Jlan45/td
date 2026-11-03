package telegram_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"golang.org/x/sync/errgroup"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// Concurrent requests are packed into containers when written while another
// write is in progress; every request must still get its own answer.
func TestClientConcurrentRequestsPacked(t *testing.T) {
	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		s.Cluster.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
			func(server *tgtest.Server, req *tgtest.Request) error {
				return server.SendVector(req, &tg.User{ID: 10, AccessHash: 10, Username: "username"})
			},
		)
		s.Cluster.Dispatch(2, "server").HandleFunc(tg.HelpGetNearestDCRequestTypeID,
			func(server *tgtest.Server, req *tgtest.Request) error {
				return server.SendResult(req, &tg.NearestDC{Country: "XX", ThisDC: 2, NearestDC: 2})
			},
		)
	}, func(ctx context.Context, c clientSetup) error {
		client := telegram.NewClient(1, "hash", c.Options)
		return client.Run(ctx, func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			g, ctx := errgroup.WithContext(ctx)
			for i := 0; i < 50; i++ {
				g.Go(func() error {
					nearest, err := client.API().HelpGetNearestDC(ctx)
					if err != nil {
						return err
					}
					if nearest.Country != "XX" {
						return errors.Errorf("unexpected country %q", nearest.Country)
					}
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
			c.Complete()
			return nil
		})
	})(t)
}

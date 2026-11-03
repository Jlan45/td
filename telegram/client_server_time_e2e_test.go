package telegram_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// skewedClock is a system clock shifted by offset.
type skewedClock struct {
	clock.Clock
	offset time.Duration
}

func (c skewedClock) Now() time.Time { return c.Clock.Now().Add(c.offset) }

// Client with local clock far from server time must still work: server
// messages would be rejected as too old/new without server time correction.
func TestClientSkewedLocalClock(t *testing.T) {
	for _, skew := range []time.Duration{10 * time.Minute, -10 * time.Minute} {
		t.Run(skew.String(), testCluster(transport.Intermediate, false, func(s clusterSetup) {
			s.Cluster.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
				func(server *tgtest.Server, req *tgtest.Request) error {
					return server.SendVector(req, &tg.User{ID: 10, AccessHash: 10, Username: "username"})
				},
			)
		}, func(ctx context.Context, c clientSetup) error {
			opts := c.Options
			opts.Clock = skewedClock{Clock: clock.System, offset: skew}
			client := telegram.NewClient(1, "hash", opts)

			return client.Run(ctx, func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if _, err := client.Self(ctx); err != nil {
					return errors.Wrap(err, "self")
				}
				c.Complete()
				return nil
			})
		}))
	}
}

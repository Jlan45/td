package telegram_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// A request in flight when the connection breaks must complete exactly once
// in the resumed session, like in official clients:
//
//   - acknowledged request (H4): the result sent by server to the new
//     connection of the session is delivered to the caller;
//   - not acknowledged request (M3): its state is queried with msgs_state_req
//     on the new connection; the server received it, so it is not resent.
//
// Previously the acknowledged request failed and the result was dropped, and
// the not acknowledged one was resent with a new msg_id, executing it twice.
func TestClientInFlightRequestSurvivesReconnect(t *testing.T) {
	for _, tt := range []struct {
		name string
		ack  bool
	}{
		{name: "Acknowledged", ack: true},
		{name: "NotAcknowledged"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			resolver := &killableResolver{}

			testCluster(transport.Intermediate, false, func(s clusterSetup) {
				s.Cluster.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
					func(server *tgtest.Server, req *tgtest.Request) error {
						return server.SendVector(req, &tg.User{ID: 10, AccessHash: 10, Username: "username"})
					},
				)
				s.Cluster.Dispatch(2, "server").HandleFunc(tg.HelpGetNearestDCRequestTypeID,
					func(server *tgtest.Server, req *tgtest.Request) error {
						calls.Add(1)
						ctx := context.Background()
						if tt.ack {
							if err := server.SendAck(ctx, req.Session, req.MsgID); err != nil {
								return err
							}
						}
						// Connection breaks before the answer.
						before := resolver.killLast()
						go func() {
							deadline := time.Now().Add(30 * time.Second)
							for resolver.dials() <= before && time.Now().Before(deadline) {
								time.Sleep(10 * time.Millisecond)
							}
							// Let the resumed connection bind to the session.
							time.Sleep(500 * time.Millisecond)

							var result bin.Buffer
							if err := result.Encode(&tg.NearestDC{Country: "XX", ThisDC: 2, NearestDC: 2}); err != nil {
								panic(err)
							}
							_ = server.Send(ctx, req.Session, proto.MessageServerResponse, &proto.Result{
								RequestMessageID: req.MsgID,
								Result:           result.Raw(),
							})
						}()
						return nil
					},
				)
			}, func(ctx context.Context, c clientSetup) error {
				opts := c.Options
				resolver.Resolver = opts.Resolver
				opts.Resolver = resolver
				opts.ReconnectionBackoff = nil
				client := telegram.NewClient(1, "hash", opts)

				return client.Run(ctx, func(ctx context.Context) error {
					ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					nearest, err := client.API().HelpGetNearestDC(ctx)
					if err != nil {
						return errors.Wrap(err, "help.getNearestDc")
					}
					require.Equal(c.TB, "XX", nearest.Country)
					c.Complete()
					return nil
				})
			})(t)

			require.Equal(t, int32(1), calls.Load(), "request must be executed exactly once")
		})
	}
}

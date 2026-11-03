package telegram

import (
	"context"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/internal/manager"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"
)

type clientHandler struct {
	client *Client
}

func (c clientHandler) OnSession(cfg tg.Config, s mtproto.Session) error {
	return c.client.onSession(cfg, s)
}

// OnSessionCreated implements mtproto.SessionCreatedHandler.
func (c clientHandler) OnSessionCreated() {
	c.client.onSessionCreated()
}

func (c clientHandler) OnMessage(b *bin.Buffer) error {
	return c.client.handleUpdates(b)
}

func (c *Client) asHandler() manager.Handler {
	return clientHandler{
		client: c,
	}
}

type cdnClientHandler struct {
	client *Client
}

func (c cdnClientHandler) OnSession(cfg tg.Config, s mtproto.Session) error {
	// CDN sessions are stored separately from regular DC sessions.
	return c.client.onCDNSession(cfg, s)
}

func (cdnClientHandler) OnMessage(*bin.Buffer) error {
	// CDN connections never deliver updates.
	return nil
}

func (c *Client) asCDNHandler() manager.Handler {
	return cdnClientHandler{
		client: c,
	}
}

type connConstructor func(
	create mtproto.Dialer,
	mode manager.ConnMode,
	appID int,
	opts mtproto.Options,
	connOpts manager.ConnOptions,
) pool.Conn

func defaultConstructor() connConstructor {
	return func(
		create mtproto.Dialer,
		mode manager.ConnMode,
		appID int,
		opts mtproto.Options,
		connOpts manager.ConnOptions,
	) pool.Conn {
		return manager.CreateConn(create, mode, appID, opts, connOpts)
	}
}

func (c *Client) dcList() dcs.List {
	cfg := c.cfg.Load()
	return dcs.List{
		Options: cfg.DCOptions,
		Domains: c.domains,
		Test:    c.testDC,
	}
}

func (c *Client) primaryDC(dc int) mtproto.Dialer {
	return func(ctx context.Context) (transport.Conn, error) {
		return c.resolver.Primary(ctx, dc, c.dcList())
	}
}

// createPrimaryConn creates primary connection. Successive primary
// connections share one MTProto session (c.primarySession), so a reconnect
// moves the session to the new TCP connection instead of opening a second main
// session on the same authorization (AUTH_KEY_DUPLICATED). Pool and data
// connections run concurrently with the primary one and keep their own session.
func (c *Client) createPrimaryConn(setup manager.SetupCallback) pool.Conn {
	opts := c.opts
	opts.Session = c.primarySession
	if c.verifyPrimaryKey.Load() {
		// Bind a temporary key to the permanent one to check that the server
		// still knows it, see requestPrimaryKeyVerification.
		opts.EnablePFS = true
	}
	return c.createConnWith(opts, c.asHandler(), 0, c.defaultMode, setup, c.onDead)
}

func (c *Client) createConn(
	id int64,
	mode manager.ConnMode,
	setup manager.SetupCallback,
	onDead func(error),
) pool.Conn {
	return c.createConnWith(c.opts, c.asHandler(), id, mode, setup, onDead)
}

// createConnWith creates connection to the primary DC using the given base
// options and handler.
func (c *Client) createConnWith(
	base mtproto.Options,
	handler manager.Handler,
	id int64,
	mode manager.ConnMode,
	setup manager.SetupCallback,
	onDead func(error),
) pool.Conn {
	opts, s := c.session.Options(base)
	opts.Logger = c.log.Named("conn").With(
		log.Int64("conn_id", id),
		log.Int("dc_id", s.DC),
	).Logger()

	return c.create(
		c.primaryDC(s.DC), mode, c.appID,
		opts, manager.ConnOptions{
			DC:      s.DC,
			Test:    c.testDC,
			Layer:   c.layer,
			Device:  c.device,
			Handler: handler,
			Setup:   setup,
			OnDead:  onDead,
		},
	)
}

// dataConnOpts returns base options for data connections: PFS is enabled
// when only data connections use it.
func (c *Client) dataConnOpts() mtproto.Options {
	opts := c.opts
	if c.dataConnPFS {
		opts.EnablePFS = true
	}
	return opts
}

// primaryDataConnHandler handles additional data connections to the primary
// DC. They are separate sessions over the primary authorization key (with a
// temporary key of their own when only data connections use PFS), so their
// session must neither overwrite the primary session nor be persisted.
type primaryDataConnHandler struct {
	clientHandler
}

func (primaryDataConnHandler) OnSession(tg.Config, mtproto.Session) error {
	return nil
}

func (c *Client) asPrimaryDataConnHandler() manager.Handler {
	return primaryDataConnHandler{clientHandler: clientHandler{client: c}}
}

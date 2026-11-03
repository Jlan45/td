package telegram

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
)

func (c *Client) handlePrimaryConnDead(err error) {
	if errors.Is(err, mtproto.ErrAuthKeyNotFound) {
		c.requestPrimaryKeyVerification()
		return
	}
	if !errors.Is(err, mtproto.ErrPFSDropKeysRequired) {
		return
	}

	// Keep DC but wipe key/salt so next reconnect performs full key bootstrap.
	dc := c.session.Load().DC
	c.log.Warn(context.Background(), "Dropping stored primary session key after PFS key reset request",
		log.Int("dc_id", dc),
	)
	c.session.Store(pool.Session{DC: dc})

	c.sessionsMux.Lock()
	if s, ok := c.sessions[dc]; ok {
		s.Store(pool.Session{DC: dc})
	}
	c.sessionsMux.Unlock()
}

// requestPrimaryKeyVerification makes the next primary connection verify the
// permanent key after the server reported -404 for it.
//
// Like TDLib ("Use PFS to check main key"), the key is checked by binding a
// temporary key to it: if the key is valid, the -404 was spurious and the key
// is kept; otherwise the bind fails with mtproto.ErrPFSDropKeysRequired and
// the key is dropped by handlePrimaryConnDead.
func (c *Client) requestPrimaryKeyVerification() {
	if c.verifyPrimaryKey.Swap(true) {
		return
	}
	c.log.Warn(context.Background(), "Server does not know primary auth key, verifying it on next connection",
		log.Int("dc_id", c.session.Load().DC),
	)
}

// dropKeyRequired reports whether error means that DC auth key must be dropped
// and generated again.
//
// For non-primary DCs a -404 is enough: their key can be re-created and
// authorized again via auth.exportAuthorization without logging out (TDLib
// can_drop_main_auth_key_without_logging_out).
func dropKeyRequired(err error) bool {
	return errors.Is(err, mtproto.ErrPFSDropKeysRequired) || errors.Is(err, mtproto.ErrAuthKeyNotFound)
}

func (c *Client) handleDCConnDead(dcID int, err error) {
	if !dropKeyRequired(err) {
		// Preserve old error path for non-PFS failures.
		if c.onDead != nil {
			c.onDead(err)
		}
		return
	}

	c.log.Warn(context.Background(), "Dropping stored DC session key after PFS key reset request",
		log.Int("dc_id", dcID),
	)
	c.sessionsMux.Lock()
	s, ok := c.sessions[dcID]
	if !ok {
		s = pool.NewSyncSession(pool.Session{DC: dcID})
		c.sessions[dcID] = s
	}
	s.Store(pool.Session{DC: dcID})
	c.sessionsMux.Unlock()

	if c.onDead != nil {
		c.onDead(err)
	}
}

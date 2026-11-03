package telegram

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/gotd/log"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/internal/upconv"
	"github.com/gotd/td/tg"
)

func (c *Client) updateInterceptor(updates ...tg.UpdateClass) {
	for _, update := range updates {
		switch update.(type) {
		case *tg.UpdateConfig, *tg.UpdateDCOptions:
			c.fetchConfig(c.ctx)
		}
	}
}

func (c *Client) processUpdates(updates tg.UpdatesClass) error {
	switch u := updates.(type) {
	case *tg.Updates:
		c.updateInterceptor(u.Updates...)
		return c.updateHandler.Handle(c.ctx, u)
	case *tg.UpdatesCombined:
		c.updateInterceptor(u.Updates...)
		return c.updateHandler.Handle(c.ctx, u)
	case *tg.UpdateShort:
		c.updateInterceptor(u.Update)
		return c.updateHandler.Handle(c.ctx, u)
	case *tg.UpdateShortMessage:
		return c.updateHandler.Handle(c.ctx, upconv.ShortMessage(u))
	case *tg.UpdateShortChatMessage:
		return c.updateHandler.Handle(c.ctx, upconv.ShortChatMessage(u))
	case *tg.UpdateShortSentMessage:
		return c.updateHandler.Handle(c.ctx, upconv.ShortSentMessage(u))
	case *tg.UpdatesTooLong:
		return c.updateHandler.Handle(c.ctx, u)
	default:
		c.log.Warn(c.ctx, "Ignoring update", log.String("update_type", fmt.Sprintf("%T", u)))
	}
	return nil
}

func (c *Client) handleUpdates(b *bin.Buffer) error {
	updates, err := tg.DecodeUpdates(b)
	if err != nil {
		return errors.Wrap(err, "decode updates")
	}
	return c.processUpdates(updates)
}

// onSessionCreated is called when server created a new session for the
// updates connection: updates sent to the previous session may have been
// lost, so update state is refreshed with getDifference by passing
// updatesTooLong to the handler, like TDLib does for the main session.
func (c *Client) onSessionCreated() {
	if c.noUpdatesMode {
		return
	}
	// Handler may invoke getDifference which needs the read loop, so it must
	// not block the read loop calling this method.
	go func() {
		if err := c.processUpdates(&tg.UpdatesTooLong{}); err != nil {
			c.log.Warn(c.ctx, "Failed to handle updatesTooLong after new session", log.Error(err))
		}
	}()
}

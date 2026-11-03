package telegram

import (
	"context"

	"github.com/gotd/log"
)

// NetworkChanged notifies client about network availability change, e.g.
// reported by the operating system (network interface switched, Wi-Fi to
// cellular, VPN toggled, device woke up).
//
// When network becomes available, the primary connection is restarted
// immediately and reconnection backoff is reset, instead of waiting until the
// dead connection is detected by ping timeout. The MTProto session is kept, so
// the restart is cheap and the server resends not acknowledged messages.
//
// This mirrors official clients: Telegram Desktop restarts all sessions on
// NetworkReachability changes, TDLib clears reconnection backoff on network
// change, Telegram Android reconnects on ConnectivityManager events.
//
// Network loss (available is false) requires no action: the connection fails
// on its own and reconnects with backoff.
func (c *Client) NetworkChanged(available bool) {
	if !available {
		return
	}
	if b := c.connBackoff.Load(); b != nil {
		(*b).Reset()
	}
	select {
	case c.restart <- struct{}{}:
		c.log.Info(context.Background(), "Network changed, restarting primary connection")
	default:
		// Not running or already reconnecting.
		c.log.Debug(context.Background(), "Network changed, connection is not running", log.Bool("available", available))
	}
}

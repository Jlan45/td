package rpc

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"
)

// RetryLimitReachedErr means that server does not acknowledge request
// after multiple retries.
type RetryLimitReachedErr struct {
	Retries int
}

func (r *RetryLimitReachedErr) Error() string {
	return fmt.Sprintf("retry limit reached after %d attempts", r.Retries)
}

// Is reports whether err is RetryLimitReachedErr.
func (r *RetryLimitReachedErr) Is(err error) bool {
	_, ok := err.(*RetryLimitReachedErr)
	return ok
}

// ErrEngineClosed means that engine was closed.
//
// Returned by Do for requests that were not acknowledged by server: per
// MTProto they are safe to resend on a new connection.
var ErrEngineClosed = errors.New("engine was closed")

// ErrClosedAfterAck means that engine was closed after the request was
// acknowledged by server but before the result was received: the server may
// have already processed the request, so resending it is not safe. The result
// can still arrive later in the same MTProto session.
//
// For compatibility errors.Is(ErrClosedAfterAck, context.Canceled) is true.
var ErrClosedAfterAck error = closedAfterAckError{}

type closedAfterAckError struct{}

func (closedAfterAckError) Error() string { return "engine closed after acknowledgement" }

// Is implements errors.Is for compatibility with context.Canceled.
func (closedAfterAckError) Is(target error) bool { return target == context.Canceled }

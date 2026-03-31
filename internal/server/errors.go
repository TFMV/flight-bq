package server

import (
	"errors"
	"fmt"
)

// Sentinel errors for the Flight SQL server.
var (
	ErrSessionLimitReached = errors.New("flightsql: maximum session limit reached")
	ErrHandleNotFound      = errors.New("flightsql: statement handle not found")
	ErrHandleExpired       = errors.New("flightsql: statement handle expired")
	ErrHandleLimitReached  = errors.New("flightsql: maximum handle limit reached")
	ErrStatementClosed     = errors.New("flightsql: statement already closed")
	ErrNotSupported        = errors.New("flightsql: operation not supported by driver")
	ErrEmptyQuery          = errors.New("flightsql: empty query")
)

// WrapError adds operation context to an error.
func WrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("flightsql %s: %w", op, err)
}

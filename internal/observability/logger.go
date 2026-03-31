// Package observability provides structured logging and metrics interfaces
// for the Flight SQL server. All implementations are pluggable; default
// no-op implementations are provided so zero configuration is required.
package observability

import (
	"fmt"
	"log"
	"strings"
)

// Logger defines a structured logging interface. Implementations should be
// safe for concurrent use.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// NopLogger discards all log output. It is the default logger.
type NopLogger struct{}

func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}

// StdLogger writes structured log lines using the standard library log package.
type StdLogger struct{}

func (StdLogger) Info(msg string, kv ...any)  { log.Printf("[INFO]  %s %s", msg, formatKV(kv)) }
func (StdLogger) Warn(msg string, kv ...any)  { log.Printf("[WARN]  %s %s", msg, formatKV(kv)) }
func (StdLogger) Error(msg string, kv ...any) { log.Printf("[ERROR] %s %s", msg, formatKV(kv)) }

func formatKV(kv []any) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i+1 < len(kv); i += 2 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%v=%v", kv[i], kv[i+1])
	}
	// Odd trailing value (programming error) — include it visibly.
	if len(kv)%2 != 0 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "EXTRA=%v", kv[len(kv)-1])
	}
	return b.String()
}

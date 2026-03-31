package observability

import (
	"crypto/rand"
	"fmt"
)

// NewQueryID returns a short, random, hex-encoded query identifier suitable
// for use in logs and metrics. Format: "q-<8 hex chars>".
func NewQueryID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		// Fallback: should never happen on modern OS.
		return "q-00000000"
	}
	return fmt.Sprintf("q-%x", b)
}

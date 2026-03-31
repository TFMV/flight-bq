package server

import (
	"testing"
	"time"

	"github.com/TFMV/flight-bq/internal/observability"
)

func newTestHandleStore(maxHandles int) *HandleStore {
	cfg := DefaultConfig()
	cfg.MaxHandles = maxHandles
	cfg.HandleTTL = 100 * time.Millisecond
	cfg.CleanupInterval = 50 * time.Millisecond
	return NewHandleStore(cfg, observability.NopLogger{})
}

func TestHandleStore_CreateAndConsume(t *testing.T) {
	hs := newTestHandleStore(100)
	hs.Start()
	defer hs.Stop()

	h, err := hs.NewHandle("SELECT 1")
	if err != nil {
		t.Fatalf("NewHandle: %v", err)
	}
	if len(h) != 32 { // 16 bytes = 32 hex chars
		t.Fatalf("expected 32-char handle, got %d: %s", len(h), h)
	}

	sql, err := hs.ConsumeHandle(h)
	if err != nil {
		t.Fatalf("ConsumeHandle: %v", err)
	}
	if sql != "SELECT 1" {
		t.Fatalf("expected 'SELECT 1', got %q", sql)
	}
}

func TestHandleStore_ConsumeOnce(t *testing.T) {
	hs := newTestHandleStore(100)
	hs.Start()
	defer hs.Stop()

	h, _ := hs.NewHandle("SELECT 1")
	hs.ConsumeHandle(h)

	_, err := hs.ConsumeHandle(h)
	if err != ErrHandleNotFound {
		t.Fatalf("expected ErrHandleNotFound on double consume, got: %v", err)
	}
}

func TestHandleStore_Uniqueness(t *testing.T) {
	hs := newTestHandleStore(10000)
	hs.Start()
	defer hs.Stop()

	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		h, err := hs.NewHandle("q")
		if err != nil {
			t.Fatalf("NewHandle %d: %v", i, err)
		}
		if seen[h] {
			t.Fatalf("duplicate handle at iteration %d: %s", i, h)
		}
		seen[h] = true
	}
}

func TestHandleStore_MaxLimit(t *testing.T) {
	hs := newTestHandleStore(3)
	hs.Start()
	defer hs.Stop()

	for i := 0; i < 3; i++ {
		_, err := hs.NewHandle("q")
		if err != nil {
			t.Fatalf("NewHandle %d: %v", i, err)
		}
	}

	_, err := hs.NewHandle("q")
	if err != ErrHandleLimitReached {
		t.Fatalf("expected ErrHandleLimitReached, got: %v", err)
	}
}

func TestHandleStore_Expiry(t *testing.T) {
	// Don't start background cleanup so the handle stays in the map
	// past its TTL, allowing ConsumeHandle to detect it as expired.
	cfg := DefaultConfig()
	cfg.MaxHandles = 100
	cfg.HandleTTL = 100 * time.Millisecond
	hs := NewHandleStore(cfg, observability.NopLogger{})
	// Intentionally NOT calling hs.Start() — no background cleanup.

	h, _ := hs.NewHandle("SELECT 1")

	// Wait for TTL to expire (100ms).
	time.Sleep(150 * time.Millisecond)

	_, err := hs.ConsumeHandle(h)
	if err != ErrHandleExpired {
		t.Fatalf("expected ErrHandleExpired, got: %v", err)
	}
}

func TestHandleStore_BackgroundCleanup(t *testing.T) {
	hs := newTestHandleStore(100)
	hs.Start()
	defer hs.Stop()

	for i := 0; i < 5; i++ {
		hs.NewHandle("q")
	}
	if hs.Len() != 5 {
		t.Fatalf("expected 5 handles, got %d", hs.Len())
	}

	// Wait for TTL (100ms) + cleanup interval (50ms).
	time.Sleep(200 * time.Millisecond)

	if hs.Len() != 0 {
		t.Fatalf("expected 0 handles after cleanup, got %d", hs.Len())
	}
}

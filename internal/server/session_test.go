package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TFMV/flight-bq/internal/observability"
)

func newTestSessionManager(drv *mockDriver, maxSessions int) *SessionManager {
	cfg := DefaultConfig()
	cfg.MaxSessions = maxSessions
	cfg.SessionTTL = 100 * time.Millisecond
	cfg.CleanupInterval = 50 * time.Millisecond
	return NewSessionManager(drv, nil, cfg, observability.NopLogger{}, observability.NopMetrics{})
}

func TestSessionManager_CreateAndReuse(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 10)
	sm.Start()
	defer sm.Shutdown()

	ctx := context.Background()

	s1, err := sm.GetSession(ctx, "sess-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if s1.ID != "sess-1" {
		t.Fatalf("expected ID=sess-1, got %s", s1.ID)
	}

	// Same ID returns same session.
	s2, err := sm.GetSession(ctx, "sess-1")
	if err != nil {
		t.Fatalf("GetSession reuse: %v", err)
	}
	if s1 != s2 {
		t.Fatal("expected same session on reuse")
	}
}

func TestSessionManager_MaxLimit(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 2)
	sm.Start()
	defer sm.Shutdown()

	ctx := context.Background()

	_, err := sm.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession s1: %v", err)
	}
	_, err = sm.GetSession(ctx, "s2")
	if err != nil {
		t.Fatalf("GetSession s2: %v", err)
	}

	// Third session should block; use a timeout context.
	tCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err = sm.GetSession(tCtx, "s3")
	if err == nil {
		t.Fatal("expected error when session limit reached and context times out")
	}
}

func TestSessionManager_ReleaseUnblocks(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 1)
	sm.Start()
	defer sm.Shutdown()

	ctx := context.Background()

	_, err := sm.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	// Release session in background.
	go func() {
		time.Sleep(20 * time.Millisecond)
		sm.CloseSession(ctx, "s1")
	}()

	tCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = sm.GetSession(tCtx, "s2")
	if err != nil {
		t.Fatalf("expected session after release, got: %v", err)
	}
}

func TestSessionManager_TTLEviction(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 10)
	sm.Start()
	defer sm.Shutdown()

	ctx := context.Background()

	_, err := sm.GetSession(ctx, "ephemeral")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	sm.ReleaseSession("ephemeral")

	// Wait for cleanup to run (TTL=100ms, cleanup=50ms).
	time.Sleep(250 * time.Millisecond)

	// Session should be evicted, so getting it creates a new one.
	sm.mu.Lock()
	_, exists := sm.sessions["ephemeral"]
	sm.mu.Unlock()

	if exists {
		t.Fatal("expected session to be evicted after TTL")
	}
}

func TestSessionManager_ConcurrentCreation(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 100)
	sm.Start()
	defer sm.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sessID := "sess-concurrent"
			_, err := sm.GetSession(ctx, sessID)
			if err != nil {
				t.Errorf("GetSession %d: %v", id, err)
			}
			sm.ReleaseSession(sessID)
		}(i)
	}
	wg.Wait()
}

func TestSessionManager_DriverError(t *testing.T) {
	sm := newTestSessionManager(failingDriver(errTest), 10)
	sm.Start()
	defer sm.Shutdown()

	_, err := sm.GetSession(context.Background(), "s1")
	if err == nil {
		t.Fatal("expected error from failing driver")
	}
}

func TestSessionManager_Shutdown(t *testing.T) {
	sm := newTestSessionManager(&mockDriver{}, 10)
	sm.Start()

	ctx := context.Background()
	sm.GetSession(ctx, "s1")
	sm.GetSession(ctx, "s2")

	err := sm.Shutdown()
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

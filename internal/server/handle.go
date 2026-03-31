package server

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/flight-bq/internal/observability"
)

// HandleStore manages a bounded, TTL-enforced map of statement handles.
// Handles are cryptographically random, one-time-use tokens that map a
// GetFlightInfo ticket to the SQL query that produced it.
type HandleStore struct {
	config  ServerConfig
	logger  observability.Logger

	mu      sync.Mutex
	handles map[string]stmtHandle

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// stmtHandle associates a SQL query with an expiration time.
type stmtHandle struct {
	sql     string
	expires time.Time
}

func (h stmtHandle) isExpired() bool { return time.Now().After(h.expires) }

// NewHandleStore creates a handle store. Call Start to begin background cleanup.
func NewHandleStore(cfg ServerConfig, logger observability.Logger) *HandleStore {
	return &HandleStore{
		config:  cfg,
		logger:  logger,
		handles: make(map[string]stmtHandle),
		stopCh:  make(chan struct{}),
	}
}

// Start launches the background cleanup goroutine.
func (hs *HandleStore) Start() {
	hs.wg.Add(1)
	go hs.cleanupLoop()
}

// Stop signals the cleanup goroutine to exit.
func (hs *HandleStore) Stop() {
	hs.stopOnce.Do(func() { close(hs.stopCh) })
	hs.wg.Wait()
}

// NewHandle creates a new statement handle for the given SQL query.
// Returns ErrHandleLimitReached if the maximum handle count is exceeded.
func (hs *HandleStore) NewHandle(sql string) (string, error) {
	handle, err := generateSecureHandle()
	if err != nil {
		return "", WrapError("generate-handle", err)
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.config.MaxHandles > 0 && len(hs.handles) >= hs.config.MaxHandles {
		return "", ErrHandleLimitReached
	}

	hs.handles[handle] = stmtHandle{
		sql:     sql,
		expires: time.Now().Add(hs.config.HandleTTL),
	}
	return handle, nil
}

// ConsumeHandle atomically retrieves and removes a handle. The handle can
// only be consumed once. Returns ErrHandleNotFound or ErrHandleExpired.
func (hs *HandleStore) ConsumeHandle(handle string) (string, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	h, ok := hs.handles[handle]
	if !ok {
		return "", ErrHandleNotFound
	}
	delete(hs.handles, handle)

	if h.isExpired() {
		return "", ErrHandleExpired
	}
	return h.sql, nil
}

// Len returns the current number of outstanding handles.
func (hs *HandleStore) Len() int {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return len(hs.handles)
}

// cleanupLoop periodically removes expired handles.
func (hs *HandleStore) cleanupLoop() {
	defer hs.wg.Done()

	ticker := time.NewTicker(hs.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hs.stopCh:
			return
		case <-ticker.C:
			hs.evictExpired()
		}
	}
}

// evictExpired removes all expired handles.
func (hs *HandleStore) evictExpired() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	evicted := 0
	for k, h := range hs.handles {
		if h.isExpired() {
			delete(hs.handles, k)
			evicted++
		}
	}
	if evicted > 0 {
		hs.logger.Info("handles evicted", "count", evicted, "remaining", len(hs.handles))
	}
}

// generateSecureHandle produces a 32-hex-character random handle using crypto/rand.
func generateSecureHandle() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

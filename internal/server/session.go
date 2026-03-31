package server

import (
	"context"
	"sync"
	"time"

	"github.com/TFMV/flight-bq/internal/observability"
	"github.com/apache/arrow-adbc/go/adbc"
)

// Session represents a single ADBC connection with lifecycle tracking.
type Session struct {
	ID            string
	conn          adbc.Connection
	db            adbc.Database
	createdAt     time.Time
	lastUsed      time.Time
	activeQueries int64
	inUse         bool
	mu            sync.Mutex
}

// markUsed updates the session's last-used timestamp and increments active queries.
func (s *Session) markUsed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastUsed = time.Now()
	s.activeQueries++
	s.inUse = true
}

// markDone decrements the active query count.
func (s *Session) markDone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeQueries--
	if s.activeQueries <= 0 {
		s.activeQueries = 0
		s.inUse = false
	}
}

// isIdle reports whether the session has no active queries.
func (s *Session) isIdle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeQueries == 0
}

// isExpired reports whether the session has exceeded its TTL.
func (s *Session) isExpired(ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastUsed) > ttl
}

// close tears down the ADBC connection and database.
func (s *Session) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		s.conn = nil
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
		s.db = nil
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// SessionManager manages a bounded pool of ADBC sessions with TTL eviction.
type SessionManager struct {
	driver     adbc.Driver
	driverOpts map[string]string
	config     ServerConfig
	logger     observability.Logger
	metrics    observability.MetricsHook

	mu       sync.Mutex
	sessions map[string]*Session
	notify   chan struct{} // Signaled when space may be available.

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewSessionManager creates a session manager. Call Start to begin background cleanup.
func NewSessionManager(driver adbc.Driver, driverOpts map[string]string, cfg ServerConfig, logger observability.Logger, metrics observability.MetricsHook) *SessionManager {
	return &SessionManager{
		driver:     driver,
		driverOpts: driverOpts,
		config:     cfg,
		logger:     logger,
		metrics:    metrics,
		sessions:   make(map[string]*Session),
		notify:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
}

// Start launches the background cleanup goroutine.
func (sm *SessionManager) Start() {
	sm.wg.Add(1)
	go sm.cleanupLoop()
}

// Stop signals the cleanup goroutine to exit and waits for it.
func (sm *SessionManager) Stop() {
	sm.stopOnce.Do(func() {
		close(sm.stopCh)
	})
	sm.wg.Wait()
}

// signal is a non-blocking send on the notify channel, used to wake
// goroutines waiting for session capacity.
func (sm *SessionManager) signal() {
	select {
	case sm.notify <- struct{}{}:
	default:
	}
}

// GetSession returns an existing or new session for the given ID.
// If the session pool is full, it blocks until a session becomes
// available or the context is cancelled.
func (sm *SessionManager) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	for {
		sm.mu.Lock()

		// Fast path: existing session.
		if sess, ok := sm.sessions[sessionID]; ok {
			sess.markUsed()
			sm.mu.Unlock()
			return sess, nil
		}

		// Check capacity.
		if sm.config.MaxSessions <= 0 || len(sm.sessions) < sm.config.MaxSessions {
			// Have capacity — create new session.
			sess, err := sm.createSessionLocked(ctx, sessionID)
			if err != nil {
				sm.mu.Unlock()
				return nil, err
			}
			sm.sessions[sessionID] = sess
			sm.metrics.OnActiveSessionsChange(len(sm.sessions))
			sm.logger.Info("session created", "session_id", sessionID, "active_sessions", len(sm.sessions))
			sm.mu.Unlock()
			return sess, nil
		}

		sm.mu.Unlock()

		// At capacity — wait for a signal or cancellation.
		select {
		case <-ctx.Done():
			return nil, WrapError("get-session", ctx.Err())
		case <-sm.stopCh:
			return nil, WrapError("get-session", context.Canceled)
		case <-sm.notify:
			// Space may be available — loop and retry.
		}
	}
}

// createSessionLocked creates a new ADBC connection. Caller must hold sm.mu.
func (sm *SessionManager) createSessionLocked(ctx context.Context, sessionID string) (*Session, error) {
	// Release lock during potentially slow driver operations.
	sm.mu.Unlock()
	db, err := sm.driver.NewDatabase(sm.driverOpts)
	if err != nil {
		sm.mu.Lock()
		return nil, WrapError("new-database", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		sm.mu.Lock()
		return nil, WrapError("open-connection", err)
	}
	sm.mu.Lock()

	now := time.Now()
	sess := &Session{
		ID:            sessionID,
		conn:          conn,
		db:            db,
		createdAt:     now,
		lastUsed:      now,
		activeQueries: 1,
		inUse:         true,
	}
	return sess, nil
}

// ReleaseSession marks the session as no longer actively in use for a query.
func (sm *SessionManager) ReleaseSession(sessionID string) {
	sm.mu.Lock()
	if sess, ok := sm.sessions[sessionID]; ok {
		sess.markDone()
	}
	sm.mu.Unlock()
	sm.signal()
}

// CloseSession tears down a specific session and removes it from the pool.
func (sm *SessionManager) CloseSession(ctx context.Context, sessionID string) error {
	sm.mu.Lock()
	sess, ok := sm.sessions[sessionID]
	if !ok {
		sm.mu.Unlock()
		return nil
	}
	delete(sm.sessions, sessionID)
	sm.metrics.OnActiveSessionsChange(len(sm.sessions))
	sm.mu.Unlock()
	sm.signal()

	sm.logger.Info("session closed", "session_id", sessionID)
	return sess.close()
}

// Conn returns the ADBC connection for the given session. Returns nil if not found.
func (sm *SessionManager) Conn(sessionID string) adbc.Connection {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sess, ok := sm.sessions[sessionID]; ok {
		return sess.conn
	}
	return nil
}

// Shutdown closes all sessions. Should be called during server teardown.
func (sm *SessionManager) Shutdown() error {
	sm.Stop()

	sm.mu.Lock()
	sessions := make(map[string]*Session, len(sm.sessions))
	for k, v := range sm.sessions {
		sessions[k] = v
	}
	sm.sessions = make(map[string]*Session)
	sm.mu.Unlock()

	var firstErr error
	for id, sess := range sessions {
		if err := sess.close(); err != nil && firstErr == nil {
			firstErr = err
			sm.logger.Error("session close error", "session_id", id, "error", err)
		}
	}
	sm.metrics.OnActiveSessionsChange(0)
	return firstErr
}

// cleanupLoop periodically evicts expired idle sessions.
func (sm *SessionManager) cleanupLoop() {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			sm.evictExpired()
		}
	}
}

// evictExpired removes sessions that are idle and past their TTL.
func (sm *SessionManager) evictExpired() {
	sm.mu.Lock()
	var toEvict []*Session
	var toEvictIDs []string
	for id, sess := range sm.sessions {
		if sess.isIdle() && sess.isExpired(sm.config.SessionTTL) {
			toEvict = append(toEvict, sess)
			toEvictIDs = append(toEvictIDs, id)
			delete(sm.sessions, id)
		}
	}
	if len(toEvict) > 0 {
		sm.metrics.OnActiveSessionsChange(len(sm.sessions))
	}
	sm.mu.Unlock()

	if len(toEvict) > 0 {
		sm.signal()
	}

	for i, sess := range toEvict {
		sm.logger.Info("session evicted (TTL)", "session_id", toEvictIDs[i])
		sess.close()
	}
}

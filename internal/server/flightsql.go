package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

type FlightSQLServer struct {
	flightsql.BaseServer

	driver     adbc.Driver
	driverOpts map[string]string

	mu          sync.RWMutex
	sessions    map[string]*Session
	sessionCond *sync.Cond

	queryHandles map[string]stmtHandle
	handleTTL    time.Duration
}

type stmtHandle struct {
	sql     string
	expires time.Time
}

func (h stmtHandle) isExpired() bool { return time.Now().After(h.expires) }

func newQueryHandle() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte((i*73 + 13) % 256)
	}
	return fmt.Sprintf("%x", b)
}

type Session struct {
	id        string
	conn      adbc.Connection
	db        adbc.Database
	createdAt time.Time
	queries   int64
	mu        sync.Mutex
	inUse     bool
}

func NewFlightSQLServer() (*FlightSQLServer, error) {
	return NewFlightSQLServerWithDriver(nil, nil)
}

func NewFlightSQLServerWithDriver(driver adbc.Driver, driverOpts map[string]string) (*FlightSQLServer, error) {
	s := &FlightSQLServer{
		sessions:     make(map[string]*Session),
		driverOpts:   make(map[string]string),
		queryHandles: make(map[string]stmtHandle),
		handleTTL:    30 * time.Minute,
	}
	if driver != nil {
		s.driver = driver
	}
	if driverOpts != nil {
		s.driverOpts = driverOpts
	}
	s.sessionCond = sync.NewCond(&s.mu)
	return s, nil
}

func (s *FlightSQLServer) SetDriverOptions(opts map[string]string) {
	s.driverOpts = opts
}

func (s *FlightSQLServer) getOrCreateSession(ctx context.Context, sessionID string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if session, exists := s.sessions[sessionID]; exists {
		session.mu.Lock()
		if session.conn != nil {
			session.queries++
			session.mu.Unlock()
			return session, nil
		}
		session.mu.Unlock()
	}

	db, err := s.driver.NewDatabase(s.driverOpts)
	if err != nil {
		return nil, err
	}

	conn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	session := &Session{
		id:        sessionID,
		conn:      conn,
		db:        db,
		createdAt: time.Now(),
		queries:   1,
		inUse:     true,
	}
	s.sessions[sessionID] = session
	return session, nil
}

func (s *FlightSQLServer) closeSessionInternal(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[sessionID]
	if !exists {
		return nil
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.conn != nil {
		session.conn.Close()
		session.conn = nil
	}
	if session.db != nil {
		session.db.Close()
		session.db = nil
	}
	delete(s.sessions, sessionID)
	return nil
}

func (s *FlightSQLServer) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for id, session := range s.sessions {
		session.mu.Lock()
		if session.conn != nil {
			if err := session.conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if session.db != nil {
			if err := session.db.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		session.mu.Unlock()
		delete(s.sessions, id)
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	handle := newQueryHandle()

	s.mu.Lock()
	s.queryHandles[handle] = stmtHandle{
		sql:     query,
		expires: time.Now().Add(s.handleTTL),
	}
	s.mu.Unlock()

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, fmt.Errorf("create ticket: %w", err)
	}

	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{
			Ticket: ticketBytes,
		},
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *FlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(ticket.GetStatementHandle())

	s.mu.Lock()
	h, ok := s.queryHandles[handle]
	if ok {
		delete(s.queryHandles, handle)
	}
	s.mu.Unlock()

	if !ok {
		return nil, nil, fmt.Errorf("statement handle not found")
	}
	if h.isExpired() {
		return nil, nil, fmt.Errorf("statement handle expired")
	}

	query := h.sql
	startTime := time.Now()
	queryID := query
	if len(queryID) > 8 {
		queryID = queryID[:8]
	}

	log.Printf("[%s] Starting query execution", queryID)

	session, err := s.getOrCreateSession(ctx, "default")
	if err != nil {
		log.Printf("[%s] Failed to get session: %v", queryID, err)
		return nil, nil, err
	}

	stmt, err := session.conn.NewStatement()
	if err != nil {
		log.Printf("[%s] Failed to create statement: %v", queryID, err)
		return nil, nil, err
	}

	if err := stmt.SetSqlQuery(query); err != nil {
		log.Printf("[%s] Failed to set query: %v", queryID, err)
		stmt.Close()
		return nil, nil, err
	}

	rdr, rowsAffected, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Printf("[%s] ExecuteQuery failed: %v", queryID, err)
		stmt.Close()
		return nil, nil, err
	}

	log.Printf("[%s] Query started, rows affected: %d, time: %v", queryID, rowsAffected, time.Since(startTime))

	ch := make(chan flight.StreamChunk, 16)

	go func() {
		defer stmt.Close()

		rowsStreamed := int64(0)
		for rdr.Next() {
			select {
			case <-ctx.Done():
				log.Printf("[%s] Context cancelled, rows streamed: %d", queryID, rowsStreamed)
				ch <- flight.StreamChunk{Err: ctx.Err()}
				return
			default:
			}

			rec := rdr.Record()
			rec.Retain()
			rowsStreamed += rec.NumRows()

			ch <- flight.StreamChunk{
				Data: rec,
			}
		}

		if err := rdr.Err(); err != nil {
			log.Printf("[%s] RecordReader error: %v", queryID, err)
			ch <- flight.StreamChunk{Err: err}
		} else {
			log.Printf("[%s] Query complete, total rows: %d, time: %v", queryID, rowsStreamed, time.Since(startTime))
		}
		rdr.Release()
		close(ch)
	}()

	return rdr.Schema(), ch, nil
}

func (s *FlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	sessionID := "txn-" + time.Now().Format("20060102150405")

	_, err := s.getOrCreateSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	return []byte(sessionID), nil
}

func (s *FlightSQLServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	sessionID := string(req.GetTransactionId())
	return s.closeSessionInternal(ctx, sessionID)
}

func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (s *FlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	return flightsql.ActionCreatePreparedStatementResult{}, nil
}

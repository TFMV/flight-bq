package server

import (
	"context"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Statement wraps an ADBC statement with lifecycle management.
// It is safe for concurrent use but does NOT support concurrent execution.
type Statement struct {
	stmt   adbc.Statement
	closed bool
	mu     sync.Mutex
}

// NewStatement creates a new Statement tied to the given connection and query.
// The caller is responsible for calling Close when finished.
func NewStatement(conn adbc.Connection, query string) (*Statement, error) {
	s, err := conn.NewStatement()
	if err != nil {
		return nil, WrapError("new-statement", err)
	}

	if err := s.SetSqlQuery(query); err != nil {
		s.Close()
		return nil, WrapError("set-query", err)
	}

	return &Statement{stmt: s}, nil
}

// NewPreparedStatement creates a statement suitable for prepared execution.
// Parameters can be bound later via SetParameters.
func NewPreparedStatement(conn adbc.Connection) (*Statement, error) {
	s, err := conn.NewStatement()
	if err != nil {
		return nil, WrapError("new-prepared-statement", err)
	}
	return &Statement{stmt: s}, nil
}

// SetSqlQuery sets the SQL query for this statement.
func (s *Statement) SetSqlQuery(query string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStatementClosed
	}
	return s.stmt.SetSqlQuery(query)
}

// Execute runs the statement and returns a streaming RecordReader.
// The RecordReader must be fully consumed or released by the caller.
// Respects context cancellation.
func (s *Statement) Execute(ctx context.Context) (array.RecordReader, int64, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, 0, ErrStatementClosed
	}
	// Hold reference to stmt before releasing lock.
	stmt := s.stmt
	s.mu.Unlock()

	// Check context before executing.
	select {
	case <-ctx.Done():
		return nil, 0, WrapError("execute", ctx.Err())
	default:
	}

	rdr, n, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, 0, WrapError("execute-query", err)
	}

	return rdr, n, nil
}

// Close releases the underlying ADBC statement. It is idempotent.
func (s *Statement) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	return s.stmt.Close()
}

// IsClosed reports whether this statement has been closed.
func (s *Statement) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

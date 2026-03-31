package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Mock ADBC Driver
// ---------------------------------------------------------------------------

type mockDriver struct {
	newDatabaseErr error
	databases      []*mockDatabase
	mu             sync.Mutex
	callCount      int64
}

func (d *mockDriver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	atomic.AddInt64(&d.callCount, 1)
	if d.newDatabaseErr != nil {
		return nil, d.newDatabaseErr
	}
	db := &mockDatabase{}
	d.mu.Lock()
	d.databases = append(d.databases, db)
	d.mu.Unlock()
	return db, nil
}

// ---------------------------------------------------------------------------
// Mock ADBC Database
// ---------------------------------------------------------------------------

type mockDatabase struct {
	openErr    error
	closeErr   error
	closed     int32
	openCount  int64
}

func (db *mockDatabase) SetOptions(_ map[string]string) error { return nil }

func (db *mockDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	atomic.AddInt64(&db.openCount, 1)
	if db.openErr != nil {
		return nil, db.openErr
	}
	return &mockConnection{db: db}, nil
}

func (db *mockDatabase) Close() error {
	atomic.StoreInt32(&db.closed, 1)
	return db.closeErr
}

func (db *mockDatabase) GetOption(_ string) (string, error) {
	return "", adbc.Error{Code: adbc.StatusNotFound}
}
func (db *mockDatabase) GetOptionBytes(_ string) ([]byte, error) {
	return nil, adbc.Error{Code: adbc.StatusNotFound}
}
func (db *mockDatabase) GetOptionDouble(_ string) (float64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (db *mockDatabase) GetOptionInt(_ string) (int64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (db *mockDatabase) SetOption(_, _ string) error              { return nil }
func (db *mockDatabase) SetOptionBytes(_, _ string) error         { return nil }
func (db *mockDatabase) SetOptionDouble(_ string, _ float64) error { return nil }
func (db *mockDatabase) SetOptionInt(_ string, _ int64) error      { return nil }

// ---------------------------------------------------------------------------
// Mock ADBC Connection
// ---------------------------------------------------------------------------

type mockConnection struct {
	mu                sync.Mutex
	db                adbc.Database
	closeErr          error
	closed            int32
	newStatementErr   error
	statementsCreated int64

	queryResult *mockRecordReader
	queryErr    error
}

func (c *mockConnection) NewStatement() (adbc.Statement, error) {
	atomic.AddInt64(&c.statementsCreated, 1)
	if c.newStatementErr != nil {
		return nil, c.newStatementErr
	}
	return &mockStatement{conn: c}, nil
}

func (c *mockConnection) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	return c.closeErr
}

func (c *mockConnection) GetInfo(_ context.Context, _ []adbc.InfoCode) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) GetObjects(_ context.Context, _ adbc.ObjectDepth, _, _, _, _ *string, _ []string) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) GetTableSchema(_ context.Context, _, _ *string, _ string) (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) GetTableTypes(_ context.Context) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) Commit(_ context.Context) error   { return nil }
func (c *mockConnection) Rollback(_ context.Context) error  { return nil }
func (c *mockConnection) ReadPartition(_ context.Context, _ []byte) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) GetOption(_ string) (string, error) {
	return "", adbc.Error{Code: adbc.StatusNotFound}
}
func (c *mockConnection) GetOptionBytes(_ string) ([]byte, error) {
	return nil, adbc.Error{Code: adbc.StatusNotFound}
}
func (c *mockConnection) GetOptionDouble(_ string) (float64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (c *mockConnection) GetOptionInt(_ string) (int64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (c *mockConnection) SetOption(_, _ string) error              { return nil }
func (c *mockConnection) SetOptionBytes(_, _ string) error         { return nil }
func (c *mockConnection) SetOptionDouble(_ string, _ float64) error { return nil }
func (c *mockConnection) SetOptionInt(_ string, _ int64) error      { return nil }
func (c *mockConnection) GetStatistics(_ context.Context, _, _, _ *string, _ bool) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) GetStatisticNames(_ context.Context) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (c *mockConnection) Cancel() error { return nil }

// ---------------------------------------------------------------------------
// Mock ADBC Statement
// ---------------------------------------------------------------------------

type mockStatement struct {
	conn       *mockConnection
	query      string
	closed     int32
	closeErr   error
	executeErr error
}

func (s *mockStatement) Close() error {
	atomic.StoreInt32(&s.closed, 1)
	return s.closeErr
}

func (s *mockStatement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *mockStatement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	if s.executeErr != nil {
		return nil, 0, s.executeErr
	}

	if s.conn != nil {
		s.conn.mu.Lock()
		if s.conn.queryResult != nil {
			rdr := s.conn.queryResult
			s.conn.queryResult = nil
			s.conn.mu.Unlock()
			return rdr, rdr.totalRows, nil
		}
		s.conn.mu.Unlock()
	}

	return newMockRecordReader(nil), 0, nil
}

func (s *mockStatement) ExecuteUpdate(_ context.Context) (int64, error) { return 0, nil }
func (s *mockStatement) Prepare(_ context.Context) error                { return nil }
func (s *mockStatement) SetSubstraitPlan(_ []byte) error                { return nil }
func (s *mockStatement) SetOption(_, _ string) error                    { return nil }
func (s *mockStatement) SetOptionBytes(_, _ string) error               { return nil }
func (s *mockStatement) SetOptionDouble(_ string, _ float64) error      { return nil }
func (s *mockStatement) SetOptionInt(_ string, _ int64) error           { return nil }
func (s *mockStatement) GetOption(_ string) (string, error) {
	return "", adbc.Error{Code: adbc.StatusNotFound}
}
func (s *mockStatement) GetOptionBytes(_ string) ([]byte, error) {
	return nil, adbc.Error{Code: adbc.StatusNotFound}
}
func (s *mockStatement) GetOptionDouble(_ string) (float64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (s *mockStatement) GetOptionInt(_ string) (int64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotFound}
}
func (s *mockStatement) Bind(_ context.Context, _ arrow.Record) error { return nil }
func (s *mockStatement) BindStream(_ context.Context, _ array.RecordReader) error {
	return nil
}
func (s *mockStatement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (s *mockStatement) ExecuteSchema(_ context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (s *mockStatement) ExecutePartitions(_ context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, adbc.Error{Code: adbc.StatusNotImplemented}
}
func (s *mockStatement) Cancel() error { return nil }

// ---------------------------------------------------------------------------
// Mock RecordReader
// ---------------------------------------------------------------------------

type mockRecordReader struct {
	schema    *arrow.Schema
	records   []arrow.Record
	idx       int
	totalRows int64
	err       error
	refCount  int32
}

func newMockRecordReader(records []arrow.Record) *mockRecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
	if len(records) > 0 {
		schema = records[0].Schema()
	}

	var total int64
	for _, r := range records {
		total += r.NumRows()
	}

	return &mockRecordReader{
		schema:    schema,
		records:   records,
		totalRows: total,
		refCount:  1,
	}
}

func (r *mockRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *mockRecordReader) Next() bool {
	if r.idx >= len(r.records) {
		return false
	}
	r.idx++
	return true
}

func (r *mockRecordReader) Record() arrow.Record {
	if r.idx == 0 || r.idx > len(r.records) {
		return nil
	}
	return r.records[r.idx-1]
}

func (r *mockRecordReader) RecordBatch() arrow.RecordBatch {
	return r.Record()
}

func (r *mockRecordReader) Err() error { return r.err }

func (r *mockRecordReader) Retain()  { atomic.AddInt32(&r.refCount, 1) }
func (r *mockRecordReader) Release() { atomic.AddInt32(&r.refCount, -1) }

// ---------------------------------------------------------------------------
// Mock FlightSQL Request Types (all are interfaces in arrow-go v18)
// ---------------------------------------------------------------------------

type mockStatementQuery struct {
	query         string
	transactionID []byte
}

func (q *mockStatementQuery) GetQuery() string         { return q.query }
func (q *mockStatementQuery) GetTransactionId() []byte  { return q.transactionID }

type mockCreatePreparedStatementRequest struct {
	query         string
	transactionID []byte
}

func (r *mockCreatePreparedStatementRequest) GetQuery() string         { return r.query }
func (r *mockCreatePreparedStatementRequest) GetTransactionId() []byte  { return r.transactionID }

type mockClosePreparedStatementRequest struct {
	handle []byte
}

func (r *mockClosePreparedStatementRequest) GetPreparedStatementHandle() []byte { return r.handle }

type mockBeginTransactionRequest struct{}

type mockEndTransactionRequest struct {
	transactionID []byte
	action        int32
}

func (r *mockEndTransactionRequest) GetTransactionId() []byte { return r.transactionID }
func (r *mockEndTransactionRequest) GetAction() flightsql.EndTransactionRequestType {
	return flightsql.EndTransactionRequestType(r.action)
}

type mockStatementQueryTicket struct {
	handle []byte
}

func (t *mockStatementQueryTicket) GetStatementHandle() []byte { return t.handle }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createMockRecords(alloc memory.Allocator, schema *arrow.Schema, batches int, rowsPerBatch int) []arrow.Record {
	records := make([]arrow.Record, batches)
	for i := 0; i < batches; i++ {
		bldr := array.NewRecordBuilder(alloc, schema)
		for j := 0; j < rowsPerBatch; j++ {
			bldr.Field(0).(*array.StringBuilder).Append("val")
		}
		records[i] = bldr.NewRecord()
		bldr.Release()
	}
	return records
}

func simpleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
}

func failingDriver(err error) *mockDriver {
	return &mockDriver{newDatabaseErr: err}
}

var errTest = errors.New("test error")

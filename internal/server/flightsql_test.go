package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TFMV/flight-bq/internal/observability"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newTestServer(drv *mockDriver) *FlightSQLServer {
	cfg := DefaultConfig()
	cfg.MaxSessions = 10
	cfg.SessionTTL = 5 * time.Second
	cfg.HandleTTL = 5 * time.Second
	cfg.CleanupInterval = 1 * time.Second
	cfg.StreamBufferSize = 4

	s, _ := NewFlightSQLServerWithConfig(
		drv,
		nil,
		cfg,
		observability.NopLogger{},
		observability.NopMetrics{},
	)
	s.Start()
	return s
}

func TestFlightSQL_GetFlightInfoAndDoGet(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()

	// GetFlightInfo
	cmd := &mockStatementQuery{query: "SELECT 1"}
	desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD}
	info, err := srv.GetFlightInfoStatement(ctx, cmd, desc)
	if err != nil {
		t.Fatalf("GetFlightInfoStatement: %v", err)
	}
	if len(info.Endpoint) == 0 {
		t.Fatal("expected at least one endpoint")
	}

	// Extract handle via GetStatementQueryTicket.
	ticket, err := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("GetStatementQueryTicket: %v", err)
	}

	schema, ch, err := srv.DoGetStatement(ctx, ticket)
	if err != nil {
		t.Fatalf("DoGetStatement: %v", err)
	}
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}

	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
}

func TestFlightSQL_EmptyQuery(t *testing.T) {
	srv := newTestServer(&mockDriver{})
	defer srv.Shutdown()

	cmd := &mockStatementQuery{query: ""}
	_, err := srv.GetFlightInfoStatement(context.Background(), cmd, &flight.FlightDescriptor{})
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestFlightSQL_HandleConsumedOnce(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()
	cmd := &mockStatementQuery{query: "SELECT 1"}

	info, _ := srv.GetFlightInfoStatement(ctx, cmd, &flight.FlightDescriptor{})
	ticket, _ := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)

	// First DoGet should succeed.
	_, ch, err := srv.DoGetStatement(ctx, ticket)
	if err != nil {
		t.Fatalf("first DoGet: %v", err)
	}
	for c := range ch {
		if c.Data != nil {
			c.Data.Release()
		}
	}

	// Second DoGet with same ticket should fail.
	_, _, err = srv.DoGetStatement(ctx, ticket)
	if err == nil {
		t.Fatal("expected error on second DoGet with same handle")
	}
}

func TestFlightSQL_PreparedStatement_Lifecycle(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()

	// Create.
	createReq := &mockCreatePreparedStatementRequest{query: "SELECT ?"}
	result, err := srv.CreatePreparedStatement(ctx, createReq)
	if err != nil {
		t.Fatalf("CreatePreparedStatement: %v", err)
	}
	if len(result.Handle) == 0 {
		t.Fatal("expected non-empty handle")
	}

	// Close.
	closeReq := &mockClosePreparedStatementRequest{handle: result.Handle}
	if err := srv.ClosePreparedStatement(ctx, closeReq); err != nil {
		t.Fatalf("ClosePreparedStatement: %v", err)
	}
}

func TestFlightSQL_Transaction_Lifecycle(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()

	// Begin.
	txnID, err := srv.BeginTransaction(ctx, &mockBeginTransactionRequest{})
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if len(txnID) == 0 {
		t.Fatal("expected non-empty transaction ID")
	}

	// End.
	endReq := &mockEndTransactionRequest{transactionID: txnID}
	if err := srv.EndTransaction(ctx, endReq); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
}

func TestFlightSQL_ConcurrentQueries(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cmd := &mockStatementQuery{query: "SELECT " + string(rune('A'+id))}
			info, err := srv.GetFlightInfoStatement(ctx, cmd, &flight.FlightDescriptor{})
			if err != nil {
				t.Errorf("GetFlightInfo %d: %v", id, err)
				return
			}

			ticket, _ := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)
			_, ch, err := srv.DoGetStatement(ctx, ticket)
			if err != nil {
				t.Errorf("DoGet %d: %v", id, err)
				return
			}

			for c := range ch {
				if c.Data != nil {
					c.Data.Release()
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestFlightSQL_WithRecordData(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	ctx := context.Background()
	alloc := memory.NewGoAllocator()

	records := createMockRecords(alloc, simpleSchema(), 3, 10)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	// Pre-populate a session so we can install mock results.
	sess, err := srv.sessions.GetSession(ctx, "default")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	mockConn := sess.conn.(*mockConnection)
	mockConn.queryResult = newMockRecordReader(records)
	srv.sessions.ReleaseSession("default")

	cmd := &mockStatementQuery{query: "SELECT * FROM test"}
	info, err := srv.GetFlightInfoStatement(ctx, cmd, &flight.FlightDescriptor{})
	if err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}

	ticket, _ := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)
	schema, ch, err := srv.DoGetStatement(ctx, ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}

	if schema.NumFields() != 1 {
		t.Fatalf("expected 1 field, got %d", schema.NumFields())
	}

	rowCount := int64(0)
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			rowCount += chunk.Data.NumRows()
			chunk.Data.Release()
		}
	}

	if rowCount != 30 {
		t.Fatalf("expected 30 rows, got %d", rowCount)
	}
}

func TestFlightSQL_ServerStartShutdown(t *testing.T) {
	srv := newTestServer(&mockDriver{})
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

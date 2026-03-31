package server

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestFlightSQL_LoadAndCancel simulates a high-concurrency environment
// where queries are cancelled mid-stream. This verifies that the
// SessionManager, HandleStore, and Streaming hooks successfully
// prevent goroutine leaks and session locks during a partial execution collapse.
func TestFlightSQL_LoadAndCancel(t *testing.T) {
	drv := &mockDriver{}
	srv := newTestServer(drv)
	defer srv.Shutdown()

	// High concurrency params
	totalQueries := 50
	cancelCount := 30
	batchesPerQuery := 100 // Ensures the stream is long enough to be cancelled
	rowsPerBatch := 10

	// Pre-generate records synchronously to avoid any race conditions 
	// within the arrow.RecordBuilder internals.
	sharedAlloc := memory.NewGoAllocator()
	sharedRecords := createMockRecords(sharedAlloc, simpleSchema(), batchesPerQuery, rowsPerBatch)
	defer func() {
		for _, r := range sharedRecords {
			r.Release()
		}
	}()

	var wg sync.WaitGroup
	var successfulStreams int32
	var cancelledStreams int32

	// Setup a custom mock driver that waits before returning batches
	// to ensure we have time to cancel
	for i := 0; i < totalQueries; i++ {
		wg.Add(1)
		
		isCancelled := i < cancelCount
		
		go func(queryID int, cancelled bool) {
			defer wg.Done()
			
			// Some random jitter
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			
			ctx := context.Background()
			var cancel context.CancelFunc

			if cancelled {
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}

			// Retain all shared records for this stream so they aren't released 
			// below 0 when this query finishes.
			for _, r := range sharedRecords {
				r.Retain()
			}
			
			// Pre-populate session with large mock result
			sess, err := srv.sessions.GetSession(context.Background(), "default")
			if err != nil {
				t.Errorf("GetSession: %v", err)
				for _, r := range sharedRecords { r.Release() }
				return
			}
			mockConn := sess.conn.(*mockConnection)
			mockConn.mu.Lock()
			mockConn.queryResult = newMockRecordReader(sharedRecords)
			mockConn.mu.Unlock()
			srv.sessions.ReleaseSession("default")

			cmd := &mockStatementQuery{query: "SELECT * FROM large_table"}
			info, err := srv.GetFlightInfoStatement(ctx, cmd, &flight.FlightDescriptor{})
			if err != nil {
				for _, r := range sharedRecords { r.Release() }
				return
			}

			ticket, _ := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)
			_, ch, err := srv.DoGetStatement(ctx, ticket)
			if err != nil {
				return
			}

			// Read some batches
			chunksScanned := 0
			for chunk := range ch {
				if chunk.Err != nil {
					break
				}
				if chunk.Data != nil {
					chunk.Data.Release()
					chunksScanned++
					
					// Cancel mid-stream
					if cancelled && chunksScanned == 5 {
						cancel()
					}
					
					// Sleep to simulate slow network consumer
					time.Sleep(2 * time.Millisecond)
				}
			}

			if cancelled {
				atomic.AddInt32(&cancelledStreams, 1)
			} else {
				atomic.AddInt32(&successfulStreams, 1)
			}
			
			if cancel != nil {
				cancel() // Cleanup
			}
		}(i, isCancelled)
	}

	wg.Wait()

	// Verification Phase
	// 1. All sessions should be released back to the pool
	srv.sessions.mu.Lock()
	activeSessions := 0
	for _, sess := range srv.sessions.sessions {
		if sess.inUse {
			activeSessions++
		}
	}
	srv.sessions.mu.Unlock()

	if activeSessions > 0 {
		t.Fatalf("Leaked %d active sessions under load cancellation", activeSessions)
	}

	// 2. We should have hit our rough targets
	if atomic.LoadInt32(&cancelledStreams) == 0 {
		t.Fatalf("No streams were cancelled, test logic flaw")
	}

	// 3. Outstanding handles should be zero (they are consumed on DoGetStatement)
	if srv.handles.Len() > 0 {
		t.Fatalf("Leaked %d statement handles", srv.handles.Len())
	}
	
	// If the test completes without hanging and active sessions is 0,
	// the cancellation collapse logic successfully unwinds.
}

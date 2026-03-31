package bridge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type testRecordReader struct {
	schema   *arrow.Schema
	records  []arrow.Record
	idx      int
	err      error
	refCount int32
}

func newTestRecordReader(records []arrow.Record, err error) *testRecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
	if len(records) > 0 {
		schema = records[0].Schema()
	}
	return &testRecordReader{
		schema:   schema,
		records:  records,
		err:      err,
		refCount: 1,
	}
}

func (r *testRecordReader) Schema() *arrow.Schema { return r.schema }
func (r *testRecordReader) Next() bool {
	if r.idx >= len(r.records) {
		return false
	}
	r.idx++
	return true
}
func (r *testRecordReader) Record() arrow.Record {
	if r.idx == 0 || r.idx > len(r.records) {
		return nil
	}
	return r.records[r.idx-1]
}
func (r *testRecordReader) Err() error   { return r.err }
func (r *testRecordReader) Retain()      { atomic.AddInt32(&r.refCount, 1) }
func (r *testRecordReader) Release()     { atomic.AddInt32(&r.refCount, -1) }
func (r *testRecordReader) RecordBatch() arrow.RecordBatch { return r.Record() }

func makeTestRecords(alloc memory.Allocator, n int, rowsPerBatch int) []arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
	records := make([]arrow.Record, n)
	for i := 0; i < n; i++ {
		bldr := array.NewRecordBuilder(alloc, schema)
		for j := 0; j < rowsPerBatch; j++ {
			bldr.Field(0).(*array.StringBuilder).Append("value")
		}
		records[i] = bldr.NewRecord()
		bldr.Release()
	}
	return records
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRecordReaderToStream_NormalStreaming(t *testing.T) {
	alloc := memory.NewGoAllocator()
	records := makeTestRecords(alloc, 3, 10)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	rdr := newTestRecordReader(records, nil)
	ch := make(chan flight.StreamChunk, 16)

	go RecordReaderToStream(context.Background(), rdr, ch, 16)

	count := 0
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("unexpected error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			count++
			chunk.Data.Release()
		}
	}
	if count != 3 {
		t.Fatalf("expected 3 chunks, got %d", count)
	}
}

func TestRecordReaderToStream_ContextCancellation(t *testing.T) {
	alloc := memory.NewGoAllocator()
	records := makeTestRecords(alloc, 100, 10)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	rdr := newTestRecordReader(records, nil)
	ch := make(chan flight.StreamChunk, 2) // Small buffer to test backpressure.

	ctx, cancel := context.WithCancel(context.Background())

	go RecordReaderToStream(ctx, rdr, ch, 2)

	// Read one chunk, then cancel.
	chunk := <-ch
	if chunk.Data != nil {
		chunk.Data.Release()
	}
	cancel()

	// Drain channel; should terminate quickly.
	done := make(chan struct{})
	go func() {
		for c := range ch {
			if c.Data != nil {
				c.Data.Release()
			}
		}
		close(done)
	}()

	select {
	case <-done:
		// OK.
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate after context cancellation")
	}
}

func TestRecordReaderToStream_ReaderError(t *testing.T) {
	testErr := errors.New("reader explosion")
	rdr := newTestRecordReader(nil, testErr)
	ch := make(chan flight.StreamChunk, 16)

	go RecordReaderToStream(context.Background(), rdr, ch, 16)

	var gotErr error
	for chunk := range ch {
		if chunk.Err != nil {
			gotErr = chunk.Err
		}
	}
	if !errors.Is(gotErr, testErr) {
		t.Fatalf("expected reader error, got: %v", gotErr)
	}
}

func TestRecordReaderToStream_ChannelAlwaysClosed(t *testing.T) {
	rdr := newTestRecordReader(nil, nil)
	ch := make(chan flight.StreamChunk, 16)

	go RecordReaderToStream(context.Background(), rdr, ch, 16)

	// Channel must close even with empty reader.
	select {
	case _, ok := <-ch:
		if ok {
			// Got a chunk (shouldn't happen with empty reader, but OK).
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel was never closed")
	}
}

func TestRecordReaderToStreamWithHooks(t *testing.T) {
	alloc := memory.NewGoAllocator()
	records := makeTestRecords(alloc, 3, 5)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	rdr := newTestRecordReader(records, nil)
	ch := make(chan flight.StreamChunk, 16)

	var batchCount int32
	var doneErr error
	doneCh := make(chan struct{})

	go RecordReaderToStreamWithHooks(context.Background(), rdr, ch, 16,
		func(rows int64) {
			atomic.AddInt32(&batchCount, 1)
		},
		func(err error) {
			doneErr = err
			close(doneCh)
		},
	)

	for chunk := range ch {
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}

	<-doneCh

	if atomic.LoadInt32(&batchCount) != 3 {
		t.Fatalf("expected 3 onBatch calls, got %d", batchCount)
	}
	if doneErr != nil {
		t.Fatalf("expected nil done error, got: %v", doneErr)
	}
}

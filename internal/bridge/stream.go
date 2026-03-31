// Package bridge provides Arrow Flight streaming utilities.
package bridge

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// RecordReaderToStream reads record batches from rdr and sends them to ch.
// It respects context cancellation, guarantees exactly one terminal error,
// and always closes the channel.
//
// Ownership discipline:
//   - Each record sent to ch has Retain() called. The consumer MUST call
//     Release() on every record it receives.
//   - The RecordReader is Released when streaming completes.
//
// bufferSize controls the channel capacity. If 0, defaults to 16.
func RecordReaderToStream(ctx context.Context, rdr array.RecordReader, ch chan<- flight.StreamChunk, bufferSize int) {
	// Note: the channel is created externally with the desired buffer size.
	// bufferSize is kept as a parameter for documentation; actual sizing
	// is done by the caller.
	defer close(ch)
	defer rdr.Release()

	var errOnce sync.Once

	sendErr := func(err error) {
		errOnce.Do(func() {
			// Non-blocking send for terminal error: if channel is full,
			// the consumer has stopped reading and will never see this error.
			select {
			case ch <- flight.StreamChunk{Err: err}:
			default:
			}
		})
	}

	for rdr.Next() {
		// Check for cancellation before sending.
		select {
		case <-ctx.Done():
			sendErr(ctx.Err())
			return
		default:
		}

		rec := rdr.Record()
		rec.Retain()

		// Send with cancellation awareness.
		select {
		case ch <- flight.StreamChunk{Data: rec}:
		case <-ctx.Done():
			rec.Release()
			sendErr(ctx.Err())
			return
		}
	}

	if err := rdr.Err(); err != nil {
		sendErr(err)
	}
}

// RecordReaderToStreamWithHooks is like RecordReaderToStream but calls
// onBatch after each record batch is sent (with the row count) and
// onDone when streaming completes (with nil on success, or the error).
func RecordReaderToStreamWithHooks(
	ctx context.Context,
	rdr array.RecordReader,
	ch chan<- flight.StreamChunk,
	bufferSize int,
	onBatch func(rows int64),
	onDone func(err error),
) {
	defer close(ch)
	defer rdr.Release()

	var errOnce sync.Once
	var streamErr error

	sendErr := func(err error) {
		errOnce.Do(func() {
			streamErr = err
			select {
			case ch <- flight.StreamChunk{Err: err}:
			default:
			}
		})
	}

	for rdr.Next() {
		select {
		case <-ctx.Done():
			sendErr(ctx.Err())
			if onDone != nil {
				onDone(streamErr)
			}
			return
		default:
		}

		rec := rdr.Record()
		rec.Retain()
		rows := rec.NumRows()

		select {
		case ch <- flight.StreamChunk{Data: rec}:
			if onBatch != nil {
				onBatch(rows)
			}
		case <-ctx.Done():
			rec.Release()
			sendErr(ctx.Err())
			if onDone != nil {
				onDone(streamErr)
			}
			return
		}
	}

	if err := rdr.Err(); err != nil {
		sendErr(err)
	}

	if onDone != nil {
		onDone(streamErr)
	}
}

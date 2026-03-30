package bridge

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

func RecordReaderToStream(ctx context.Context, rdr array.RecordReader, ch chan<- flight.StreamChunk) {
	defer close(ch)
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		ch <- flight.StreamChunk{
			Data: rec,
		}
	}
	if err := rdr.Err(); err != nil {
		ch <- flight.StreamChunk{
			Err: err,
		}
	}
}

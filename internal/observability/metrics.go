package observability

import "time"

// QueryMetrics holds telemetry data for a single query execution.
type QueryMetrics struct {
	QueryID      string
	SQL          string
	Duration     time.Duration
	RowsStreamed int64
	BytesRead    int64
	Error        error
}

// MetricsHook receives notifications about query lifecycle events.
// Implementations must be safe for concurrent use.
type MetricsHook interface {
	// OnQueryStart is called when query execution begins.
	OnQueryStart(queryID, sql string)
	// OnFirstBatch is called when the first record batch is available.
	OnFirstBatch(queryID string, latency time.Duration)
	// OnQueryComplete is called when query streaming finishes successfully.
	OnQueryComplete(m QueryMetrics)
	// OnQueryError is called when a query fails at any stage.
	OnQueryError(queryID string, err error)
	// OnActiveSessionsChange is called when the active session count changes.
	OnActiveSessionsChange(count int)
}

// NopMetrics is a no-op MetricsHook. It is the default.
type NopMetrics struct{}

func (NopMetrics) OnQueryStart(string, string)            {}
func (NopMetrics) OnFirstBatch(string, time.Duration)     {}
func (NopMetrics) OnQueryComplete(QueryMetrics)            {}
func (NopMetrics) OnQueryError(string, error)              {}
func (NopMetrics) OnActiveSessionsChange(int)              {}

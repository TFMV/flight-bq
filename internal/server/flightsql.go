package server

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/flight-bq/internal/bridge"
	"github.com/TFMV/flight-bq/internal/observability"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

// FlightSQLServer is an enterprise-grade Flight SQL server backed by ADBC.
type FlightSQLServer struct {
	flightsql.BaseServer

	config   ServerConfig
	logger   observability.Logger
	metrics  observability.MetricsHook

	sessions *SessionManager
	handles  *HandleStore
}

// NewFlightSQLServer creates a server with default config and no-op observability.
func NewFlightSQLServer() (*FlightSQLServer, error) {
	return NewFlightSQLServerWithConfig(nil, nil, DefaultConfig(), observability.NopLogger{}, observability.NopMetrics{})
}

// NewFlightSQLServerWithDriver creates a server with the given driver and options.
// Preserves the original constructor signature for backward compatibility.
func NewFlightSQLServerWithDriver(driver adbc.Driver, driverOpts map[string]string) (*FlightSQLServer, error) {
	return NewFlightSQLServerWithConfig(driver, driverOpts, DefaultConfig(), observability.NopLogger{}, observability.NopMetrics{})
}

// NewFlightSQLServerWithConfig creates a fully configured Flight SQL server.
func NewFlightSQLServerWithConfig(
	driver adbc.Driver,
	driverOpts map[string]string,
	config ServerConfig,
	logger observability.Logger,
	metrics observability.MetricsHook,
) (*FlightSQLServer, error) {
	baseOpts := config.BigQuery.ToMap()
	if driverOpts == nil {
		driverOpts = baseOpts
	} else {
		for k, v := range baseOpts {
			if _, exists := driverOpts[k]; !exists {
				driverOpts[k] = v
			}
		}
	}
	if logger == nil {
		logger = observability.NopLogger{}
	}
	if metrics == nil {
		metrics = observability.NopMetrics{}
	}

	s := &FlightSQLServer{
		config:   config,
		logger:   logger,
		metrics:  metrics,
		sessions: NewSessionManager(driver, driverOpts, config, logger, metrics),
		handles:  NewHandleStore(config, logger),
	}
	return s, nil
}

// SetDriverOptions updates the driver options. Must be called before Start.
func (s *FlightSQLServer) SetDriverOptions(opts map[string]string) {
	s.sessions.driverOpts = opts
}

// Start launches background goroutines (session cleanup, handle cleanup).
func (s *FlightSQLServer) Start() {
	s.sessions.Start()
	s.handles.Start()
	s.logger.Info("flight sql server started",
		"max_sessions", s.config.MaxSessions,
		"session_ttl", s.config.SessionTTL,
		"stream_buffer", s.config.StreamBufferSize,
	)
}

// Shutdown stops background goroutines and closes all sessions.
func (s *FlightSQLServer) Shutdown() error {
	s.handles.Stop()
	err := s.sessions.Shutdown()
	s.logger.Info("flight sql server shut down")
	return err
}

// ---------------------------------------------------------------------------
// FlightSQL: Statement Query
// ---------------------------------------------------------------------------

// GetFlightInfoStatement handles the first phase of a query: registering the
// query and returning a ticket for DoGet.
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	if query == "" {
		return nil, ErrEmptyQuery
	}

	handle, err := s.handles.NewHandle(query)
	if err != nil {
		return nil, WrapError("get-flight-info", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, WrapError("create-ticket", err)
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetStatement handles the second phase: executing the query and streaming results.
func (s *FlightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(ticket.GetStatementHandle())
	queryID := observability.NewQueryID()

	// Consume the handle (one-time use).
	query, err := s.handles.ConsumeHandle(handle)
	if err != nil {
		return nil, nil, err
	}

	s.logger.Info("query starting", "query_id", queryID, "sql", truncateSQL(query))
	s.metrics.OnQueryStart(queryID, query)
	startTime := time.Now()

	// Get a session.
	session, err := s.sessions.GetSession(ctx, "default")
	if err != nil {
		s.logger.Error("session acquire failed", "query_id", queryID, "error", err)
		s.metrics.OnQueryError(queryID, err)
		return nil, nil, err
	}

	// Create and execute statement.
	stmt, err := NewStatement(session.conn, query)
	if err != nil {
		s.sessions.ReleaseSession("default")
		s.logger.Error("statement creation failed", "query_id", queryID, "error", err)
		s.metrics.OnQueryError(queryID, err)
		return nil, nil, err
	}

	rdr, _, err := stmt.Execute(ctx)
	if err != nil {
		stmt.Close()
		s.sessions.ReleaseSession("default")
		s.logger.Error("query execution failed", "query_id", queryID, "error", err)
		s.metrics.OnQueryError(queryID, err)
		return nil, nil, err
	}

	schema := rdr.Schema()
	ch := make(chan flight.StreamChunk, s.config.StreamBufferSize)

	// Stream in background goroutine.
	go func() {
		defer stmt.Close()
		defer s.sessions.ReleaseSession("default")

		firstBatchSent := false
		rowsStreamed := int64(0)

		bridge.RecordReaderToStreamWithHooks(ctx, rdr, ch, s.config.StreamBufferSize,
			func(rows int64) {
				rowsStreamed += rows
				if !firstBatchSent {
					firstBatchSent = true
					s.metrics.OnFirstBatch(queryID, time.Since(startTime))
				}
			},
			func(err error) {
				if err != nil {
					s.logger.Error("query streaming error", "query_id", queryID, "error", err)
					s.metrics.OnQueryError(queryID, err)
				} else {
					s.logger.Info("query complete", "query_id", queryID, "rows", rowsStreamed, "duration", time.Since(startTime))
					s.metrics.OnQueryComplete(observability.QueryMetrics{
						QueryID:      queryID,
						SQL:          query,
						Duration:     time.Since(startTime),
						RowsStreamed: rowsStreamed,
					})
				}
			},
		)
	}()

	return schema, ch, nil
}

// ---------------------------------------------------------------------------
// FlightSQL: Prepared Statements
// ---------------------------------------------------------------------------

// CreatePreparedStatement creates a reusable prepared statement.
func (s *FlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	query := req.GetQuery()
	if query == "" {
		return flightsql.ActionCreatePreparedStatementResult{}, ErrEmptyQuery
	}

	session, err := s.sessions.GetSession(ctx, "default")
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, WrapError("create-prepared-stmt", err)
	}

	// Create the ADBC statement and set the query.
	adbcStmt, err := session.conn.NewStatement()
	if err != nil {
		s.sessions.ReleaseSession("default")
		return flightsql.ActionCreatePreparedStatementResult{}, WrapError("create-prepared-stmt", err)
	}

	if err := adbcStmt.SetSqlQuery(query); err != nil {
		adbcStmt.Close()
		s.sessions.ReleaseSession("default")
		return flightsql.ActionCreatePreparedStatementResult{}, WrapError("create-prepared-stmt", err)
	}

	// Store the handle so we can retrieve the statement later.
	handle, err := s.handles.NewHandle(query)
	if err != nil {
		adbcStmt.Close()
		s.sessions.ReleaseSession("default")
		return flightsql.ActionCreatePreparedStatementResult{}, WrapError("create-prepared-stmt", err)
	}

	s.sessions.ReleaseSession("default")
	s.logger.Info("prepared statement created", "handle", handle[:8])

	return flightsql.ActionCreatePreparedStatementResult{
		Handle: []byte(handle),
	}, nil
}

// ClosePreparedStatement closes a previously created prepared statement.
func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	handle := string(req.GetPreparedStatementHandle())
	// Consume the handle to remove it; ignore not-found since the statement
	// may have already been consumed via execution.
	_, _ = s.handles.ConsumeHandle(handle)
	s.logger.Info("prepared statement closed", "handle", truncateSQL(handle))
	return nil
}

// ---------------------------------------------------------------------------
// FlightSQL: Transactions
// ---------------------------------------------------------------------------

// BeginTransaction starts a new transaction by creating a dedicated session.
func (s *FlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	queryID := observability.NewQueryID()
	sessionID := "txn-" + queryID

	_, err := s.sessions.GetSession(ctx, sessionID)
	if err != nil {
		return nil, WrapError("begin-transaction", err)
	}

	s.logger.Info("transaction started", "session_id", sessionID)
	return []byte(sessionID), nil
}

// EndTransaction commits or rolls back a transaction and closes its session.
func (s *FlightSQLServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	sessionID := string(req.GetTransactionId())
	s.logger.Info("transaction ended", "session_id", sessionID)
	return s.sessions.CloseSession(ctx, sessionID)
}

// ---------------------------------------------------------------------------
// FlightSQL: Metadata APIs
// ---------------------------------------------------------------------------

// GetFlightInfoCatalogs returns flight info for catalog listing.
func (s *FlightSQLServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	handle, err := s.handles.NewHandle("__catalogs__")
	if err != nil {
		return nil, WrapError("get-catalogs-info", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, WrapError("create-ticket", err)
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// GetFlightInfoSchemas returns flight info for schema listing.
func (s *FlightSQLServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	catalog := ""
	if cmd.GetCatalog() != nil {
		catalog = *cmd.GetCatalog()
	}
	schemaPattern := ""
	if sp := cmd.GetDBSchemaFilterPattern(); sp != nil {
		schemaPattern = *sp
	}
	query := fmt.Sprintf("__schemas__%s__%s", catalog, schemaPattern)

	handle, err := s.handles.NewHandle(query)
	if err != nil {
		return nil, WrapError("get-schemas-info", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, WrapError("create-ticket", err)
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// GetFlightInfoTables returns flight info for table listing.
func (s *FlightSQLServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	catalog := ""
	if cmd.GetCatalog() != nil {
		catalog = *cmd.GetCatalog()
	}
	schemaPattern := ""
	if sp := cmd.GetDBSchemaFilterPattern(); sp != nil {
		schemaPattern = *sp
	}
	tablePattern := ""
	if tp := cmd.GetTableNameFilterPattern(); tp != nil {
		tablePattern = *tp
	}
	query := fmt.Sprintf("__tables__%s__%s__%s", catalog, schemaPattern, tablePattern)

	handle, err := s.handles.NewHandle(query)
	if err != nil {
		return nil, WrapError("get-tables-info", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, WrapError("create-ticket", err)
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func truncateSQL(sql string) string {
	if len(sql) > 64 {
		return sql[:64] + "..."
	}
	return sql
}

package server

import (
	"fmt"
	"time"
)

// AuthType defines how the BigQuery ADBC driver authenticates.
type AuthType string

const (
	AuthTypeDefault               AuthType = "adbc.bigquery.sql.auth_type.auth_bigquery"
	AuthTypeAppDefaultCredentials AuthType = "adbc.bigquery.sql.auth_type.app_default_credentials"
	AuthTypeJSONCredentialFile    AuthType = "adbc.bigquery.sql.auth_type.json_credential_file"
	AuthTypeJSONCredentialString  AuthType = "adbc.bigquery.sql.auth_type.json_credential_string"
	AuthTypeJSONCredentials       AuthType = "adbc.bigquery.sql.auth_type.json_credentials"
	AuthTypeOAuthClientIDs        AuthType = "adbc.bigquery.sql.auth_type.oauth_client_ids"
	AuthTypeUserAuthentication    AuthType = "adbc.bigquery.sql.auth_type.user_authentication"
)

// BigQueryConfig holds all ADBC driver-specific configuration.
type BigQueryConfig struct {
	ProjectID          string
	DatasetID          string
	TableID            string
	Location           string
	AuthType           AuthType
	AuthCredentials    string
	UseLegacySQL       bool
	AllowLargeResults  bool
	DisableQueryCache  bool
	JobTimeout         time.Duration
	PrefetchConcurrency int
	ResultBufferSize    int
}

// ToMap serializes the strongly typed BigQuery config down to the map[string]string
// format expected by the ADBC driver's NewDatabase() call.
func (b BigQueryConfig) ToMap() map[string]string {
	m := make(map[string]string)
	m["driver"] = "bigquery"

	if b.ProjectID != "" {
		m["adbc.bigquery.sql.project_id"] = b.ProjectID
	}
	if b.DatasetID != "" {
		m["adbc.bigquery.sql.dataset_id"] = b.DatasetID
	}
	if b.TableID != "" {
		m["adbc.bigquery.sql.table_id"] = b.TableID
	}
	if b.Location != "" {
		m["adbc.bigquery.sql.location"] = b.Location
	}
	if b.AuthType != "" {
		m["adbc.bigquery.sql.auth_type"] = string(b.AuthType)
	}
	if b.AuthCredentials != "" {
		m["adbc.bigquery.sql.auth_credentials"] = b.AuthCredentials
	}
	if b.UseLegacySQL {
		m["adbc.bigquery.sql.query.use_legacy_sql"] = "true"
	} else {
		m["adbc.bigquery.sql.query.use_legacy_sql"] = "false"
	}
	if b.AllowLargeResults {
		m["adbc.bigquery.sql.query.allow_large_results"] = "true"
	}
	if b.DisableQueryCache {
		m["adbc.bigquery.sql.query.disable_query_cache"] = "true"
	}
	if b.JobTimeout > 0 {
		m["adbc.bigquery.sql.query.job_timeout"] = fmt.Sprintf("%d", int64(b.JobTimeout.Seconds()))
	}
	if b.PrefetchConcurrency > 0 {
		m["adbc.bigquery.sql.query.prefetch_concurrency"] = fmt.Sprintf("%d", b.PrefetchConcurrency)
	}
	if b.ResultBufferSize > 0 {
		m["adbc.bigquery.sql.query.result_buffer_size"] = fmt.Sprintf("%d", b.ResultBufferSize)
	}
	return m
}

// ServerConfig controls resource limits and timeouts for the Flight SQL server.
type ServerConfig struct {
	// BigQuery contains driver-specific configuration
	BigQuery BigQueryConfig

	// MaxSessions is the maximum number of concurrent sessions. 0 = unlimited.
	MaxSessions int
	// SessionTTL is the duration after which an idle session is evicted.
	SessionTTL time.Duration
	// HandleTTL is the duration a statement handle remains valid.
	HandleTTL time.Duration
	// MaxHandles is the maximum number of outstanding statement handles. 0 = unlimited.
	MaxHandles int
	// StreamBufferSize is the channel buffer size for streaming record batches.
	StreamBufferSize int
	// CleanupInterval is how often background cleanup goroutines run.
	CleanupInterval time.Duration
}

// DefaultConfig returns a ServerConfig with production-sensible defaults.
func DefaultConfig() ServerConfig {
	return ServerConfig{
		BigQuery: BigQueryConfig{
			AuthType:            AuthTypeDefault,
			PrefetchConcurrency: 10,
			ResultBufferSize:    200,
		},
		MaxSessions:      64,
		SessionTTL:       30 * time.Minute,
		HandleTTL:        30 * time.Minute,
		MaxHandles:       4096,
		StreamBufferSize: 16,
		CleanupInterval:  30 * time.Second,
	}
}

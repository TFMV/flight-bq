package server

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Statement struct {
	stmt adbc.Statement
}

func NewStatement(conn adbc.Connection, query string) (*Statement, error) {
	s, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}

	if err := s.SetSqlQuery(query); err != nil {
		return nil, err
	}

	return &Statement{stmt: s}, nil
}

func (s *Statement) Execute(ctx context.Context) (array.RecordReader, int64, error) {
	return s.stmt.ExecuteQuery(ctx)
}

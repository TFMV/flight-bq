package server

import (
	"context"
	"sync"
	"testing"
)

func TestStatementClose_Idempotent(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewStatement(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}

	if err := stmt.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("second Close should be no-op: %v", err)
	}
	if !stmt.IsClosed() {
		t.Fatal("expected IsClosed() = true after Close()")
	}
}

func TestStatementExecute_AfterClose(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewStatement(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	stmt.Close()

	_, _, err = stmt.Execute(context.Background())
	if err != ErrStatementClosed {
		t.Fatalf("expected ErrStatementClosed, got: %v", err)
	}
}

func TestStatementExecute_ContextCancelled(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewStatement(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, _, err = stmt.Execute(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestStatementSetSqlQuery_AfterClose(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewStatement(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	stmt.Close()

	err = stmt.SetSqlQuery("SELECT 2")
	if err != ErrStatementClosed {
		t.Fatalf("expected ErrStatementClosed, got: %v", err)
	}
}

func TestStatementConcurrentCloseAndExecute(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewStatement(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			stmt.Execute(context.Background())
		}()
		go func() {
			defer wg.Done()
			stmt.Close()
		}()
	}
	wg.Wait()
}

func TestNewPreparedStatement(t *testing.T) {
	drv := &mockDriver{}
	db, _ := drv.NewDatabase(nil)
	conn, _ := db.Open(context.Background())

	stmt, err := NewPreparedStatement(conn)
	if err != nil {
		t.Fatalf("NewPreparedStatement: %v", err)
	}
	defer stmt.Close()

	if stmt.IsClosed() {
		t.Fatal("expected not closed")
	}
}

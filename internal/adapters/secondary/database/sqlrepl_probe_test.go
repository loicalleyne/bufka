package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/couac"
)

// These probes pin down DuckDB/ADBC behaviour the SQL REPL relies on. Rerun
// them after a DuckDB upgrade (notably 2.0, whose PEG parser is default).

func probeConn(t *testing.T) *couac.Conn {
	t.Helper()
	db, err := couac.NewDuck(couac.WithDriverLookup())
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestProbeCancel records that cancelling the context passed to ExecuteQuery
// does NOT interrupt a running DuckDB query (DuckDB 1.5.5 via ADBC): the Go
// ADBC bindings expose no AdbcStatementCancel and the driver ignores ctx.
// The REPL's ctrl+c therefore stops waiting and discards the result while
// the query finishes in the background. If this test starts failing, real
// cancellation became available: make ctrl+c rely on it.
func TestProbeCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("slow probe")
	}
	conn := probeConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	rr, stmt, _, err := conn.QueryRaw(ctx, `SELECT sum(i % 7) FROM range(600000000) t(i)`)
	if err == nil {
		for rr.Next() {
		}
		err = rr.Err()
		rr.Release()
		_ = stmt.Close()
	}
	took := time.Since(start)
	t.Logf("cancel: returned after %v, err=%v", took, err)
	if err != nil {
		t.Fatalf("query was interrupted (%v after %v): DuckDB/ADBC now honours ctx, update the REPL's ctrl+c", err, took)
	}
}

// TestProbeAutoComplete: sql_auto_complete is available without LOAD and
// returns catalog-aware suggestions.
func TestProbeAutoComplete(t *testing.T) {
	conn := probeConn(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `CREATE TABLE my_topic(_kafka_offset BIGINT)`); err != nil {
		t.Fatal(err)
	}
	res, err := conn.Query(ctx, `SELECT suggestion, suggestion_start, suggestion_type, extra_char FROM sql_auto_complete('SELECT * FROM my_')`)
	if err != nil {
		t.Fatalf("sql_auto_complete: %v", err)
	}
	defer res.Close()
	found := false
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		s := rec.Column(0).(*array.String)
		for i := 0; i < s.Len(); i++ {
			if s.Value(i) == "my_topic" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("my_topic not suggested")
	}
}

// TestProbeDML records what a DML statement returns through QueryRaw.
func TestProbeDML(t *testing.T) {
	conn := probeConn(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `CREATE TABLE t(i INT)`); err != nil {
		t.Fatal(err)
	}
	rr, stmt, n, err := conn.QueryRaw(ctx, `INSERT INTO t SELECT * FROM range(5)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	defer rr.Release()
	t.Logf("dml: n=%d schema=%v", n, rr.Schema())
	var rows int64
	for rr.Next() {
		rows += rr.RecordBatch().NumRows()
	}
	if rr.Schema().NumFields() != 1 || rr.Schema().Field(0).Name != "Count" {
		t.Fatalf("expected a single Count column, got %v", rr.Schema())
	}

	// DDL through QueryRaw.
	rr2, stmt2, n2, err := conn.QueryRaw(ctx, `CREATE TABLE u(i INT)`)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ddl: n=%d schema=%v", n2, rr2.Schema())
	rr2.Release()
	_ = stmt2.Close()

	_, _, _, err = conn.QueryRaw(ctx, `SELEC 1`)
	t.Logf("syntax error: %v", err)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("expected syntax error")
	}
}

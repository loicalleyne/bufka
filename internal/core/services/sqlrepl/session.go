package sqlrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
	"github.com/loicalleyne/couac"
)

// DefaultMaxRows is the DuckDB CLI's default .maxrows.
const DefaultMaxRows = 40

// DefaultKeepRows bounds the rows of the last result kept for .export and
// .browse.
const DefaultKeepRows = 100_000

// Settings are the REPL's output settings, changed by dot-commands.
type Settings struct {
	Format  format.Options
	MaxRows int
	Timer   bool
	Changes bool
	Bail    bool
	Echo    bool
}

// DefaultSettings mirror the DuckDB CLI.
func DefaultSettings() Settings {
	return Settings{Format: format.DefaultOptions(), MaxRows: DefaultMaxRows}
}

// Session is one REPL session: a dedicated DuckDB connection (so SET, USE,
// temp tables and transactions stay private) plus output settings.
type Session struct {
	DB   *couac.DB
	Conn *couac.Conn
	// complete is a second connection for sql_auto_complete, so completion
	// works while a query runs.
	complete   *couac.Conn
	completeMu sync.Mutex

	Settings Settings
	// Deps reaches the rest of bufka for the bufka-specific dot-commands;
	// nil fields disable those commands.
	Deps Deps
	// Dir is the working directory for file arguments (.cd changes it).
	Dir string
	// KeepRows bounds Last.
	KeepRows int

	// Last is the most recent row-returning result, for .export/.browse.
	Last *format.Table
	// LastSQL is the statement that produced Last.
	LastSQL string

	// output redirects results (.output / .once); nil means the screen.
	output    io.WriteCloser
	outPath   string
	onceOnly  bool
	onceOpenX bool
	// onceMode is the mode to restore after .once -x forced csv.
	onceMode format.Mode
	// maxWidthSet is true when .maxwidth was set explicitly; otherwise the
	// screen keeps MaxWidth equal to its width.
	maxWidthSet bool
}

// SetScreenWidth makes box modes fit the screen unless .maxwidth was set.
func (s *Session) SetScreenWidth(w int) {
	if !s.maxWidthSet {
		s.Settings.Format.MaxWidth = w
	}
}

// NewSession opens the REPL's connections on db.
func NewSession(db *couac.DB, deps Deps) (*Session, error) {
	if db == nil {
		return nil, errors.New("the SQL REPL needs a DuckDB database (DUCKDB_PATH)")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	comp, err := db.Connect()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	return &Session{
		DB: db, Conn: conn, complete: comp,
		Settings: DefaultSettings(), Deps: deps, KeepRows: DefaultKeepRows,
	}, nil
}

// Close releases the session's connections and any open output file.
func (s *Session) Close() error {
	s.closeOutput()
	var errs []error
	if s.complete != nil {
		errs = append(errs, s.complete.Close())
	}
	if s.Conn != nil {
		errs = append(errs, s.Conn.Close())
	}
	return errors.Join(errs...)
}

// Result is the outcome of running one statement.
type Result struct {
	SQL string
	// Table is set for row-returning statements.
	Table *format.Table
	// RowsAffected is set (>= 0) for DML; -1 otherwise.
	RowsAffected int64
	// Full holds every row up to KeepRows, for .export and .browse.
	Full    *format.Table
	Elapsed time.Duration
	Err     error
}

// IsQuery reports whether the statement returned rows to display.
func (r Result) IsQuery() bool { return r.Table != nil }

// Run executes one SQL statement. Row-returning statements stream their
// Arrow batches; only the first and last MaxRows/2 rows are kept for
// display when the mode truncates (duckbox), all rows (up to KeepRows)
// otherwise. A DML/DDL statement (DuckDB returns a single "Count" column)
// reports RowsAffected instead of a table.
//
// Cancelling ctx stops reading results, but DuckDB (1.5 via ADBC) does not
// interrupt a statement that is still executing: see TestProbeCancel.
func (s *Session) Run(ctx context.Context, sql string) Result {
	start := time.Now()
	res := Result{SQL: sql, RowsAffected: -1}
	rr, stmt, n, err := s.Conn.QueryRaw(ctx, sql)
	if err != nil {
		res.Err = cleanErr(err)
		res.Elapsed = time.Since(start)
		return res
	}
	defer func() { _ = stmt.Close() }()
	defer rr.Release()

	schema := rr.Schema()
	if isCountResult(schema, sql) {
		var count int64 = -1
		for rr.Next() {
			rec := rr.RecordBatch()
			if rec.NumRows() > 0 {
				if c, ok := rec.Column(0).(*array.Int64); ok && c.IsValid(0) {
					count = c.Value(0)
				}
			}
		}
		if count < 0 && n >= 0 {
			count = n
		}
		res.RowsAffected = count
		res.Err = cleanErr(rr.Err())
		res.Elapsed = time.Since(start)
		return res
	}

	keepHead, keepTail := s.limits()
	tbl := &format.Table{}
	for _, f := range schema.Fields() {
		tbl.Columns = append(tbl.Columns, format.ColumnFor(f))
	}
	full := &format.Table{Columns: tbl.Columns}
	tail := newRing(keepTail)
	for rr.Next() {
		if err := ctx.Err(); err != nil {
			res.Err = err
			break
		}
		rec := rr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := rowAt(rec, r)
			switch {
			case len(tbl.Head) < keepHead:
				tbl.Head = append(tbl.Head, row)
			default:
				tail.push(row)
			}
			if len(full.Head) < s.KeepRows {
				full.Head = append(full.Head, row)
			}
			tbl.Total++
		}
	}
	if res.Err == nil {
		res.Err = cleanErr(rr.Err())
	}
	tbl.Tail = tail.rows()
	full.Total = int64(len(full.Head))
	res.Table = tbl
	if isStatusResult(schema, tbl) {
		// INSTALL, LOAD, ATTACH, SET … return an empty "Success" column
		// through ADBC: like the DuckDB CLI, print nothing.
		res.Table = nil
		full = nil
	}
	if res.Err == nil && full != nil {
		res.Full = full
	}
	res.Elapsed = time.Since(start)
	return res
}

// limits returns how many leading and trailing rows to keep for display.
func (s *Session) limits() (head, tail int) {
	if !s.Settings.Format.Mode.Truncates() || s.Settings.MaxRows <= 0 {
		return s.KeepRows, 0
	}
	head = (s.Settings.MaxRows + 1) / 2
	return head, s.Settings.MaxRows - head
}

// isCountResult reports whether a result is DuckDB's "Count" pseudo result
// of DML/DDL rather than a real query result.
func isCountResult(schema *arrow.Schema, sql string) bool {
	if schema.NumFields() != 1 || schema.Field(0).Name != "Count" || schema.Field(0).Type.ID() != arrow.INT64 {
		return false
	}
	// A query selecting a column named Count is still a query.
	first := strings.ToUpper(firstKeyword(sql))
	switch first {
	case "SELECT", "WITH", "FROM", "VALUES", "TABLE", "SHOW", "DESCRIBE", "SUMMARIZE", "PIVOT", "UNPIVOT", "EXPLAIN", "CALL", "PRAGMA", "(":
		return false
	}
	return true
}

// firstKeyword returns the first non-comment token of sql.
func firstKeyword(sql string) string {
	for _, t := range Lex(sql) {
		if t.Kind == TokSpace || t.Kind == TokComment {
			continue
		}
		return t.Text(sql)
	}
	return ""
}

func rowAt(rec arrow.RecordBatch, r int) []format.Cell {
	row := make([]format.Cell, rec.NumCols())
	for c := range row {
		row[c] = format.CellAt(rec.Column(c), r)
	}
	return row
}

// cleanErr strips couac's wrapping so errors read like the DuckDB CLI's.
func cleanErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, p := range []string{"couac: execute query: ", "couac: set sql query: ", "couac: execute update: ", "Invalid Argument: ", "Internal: ", "Unknown: ", "IO: "} {
		msg = strings.ReplaceAll(msg, p, "")
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return errors.New(strings.TrimSpace(msg))
}

// ring keeps the last n rows.
type ring struct {
	buf   [][]format.Cell
	n     int
	start int
}

func newRing(n int) *ring { return &ring{n: n} }

func (r *ring) push(row []format.Cell) {
	if r.n <= 0 {
		return
	}
	if len(r.buf) < r.n {
		r.buf = append(r.buf, row)
		return
	}
	r.buf[r.start] = row
	r.start = (r.start + 1) % r.n
}

func (r *ring) rows() [][]format.Cell {
	out := make([][]format.Cell, 0, len(r.buf))
	out = append(out, r.buf[r.start:]...)
	return append(out, r.buf[:r.start]...)
}

// isStatusResult reports whether a result is the empty single-column
// status DuckDB returns for non-query statements (e.g. "Success" for
// INSTALL/LOAD).
func isStatusResult(schema *arrow.Schema, t *format.Table) bool {
	return t.Total == 0 && schema.NumFields() == 1 && schema.Field(0).Name == "Success" &&
		schema.Field(0).Type.ID() == arrow.BOOL
}

// loadFromRe matches "LOAD ext FROM repo", which DuckDB does not accept:
// FROM belongs to INSTALL.
var loadFromRe = regexp.MustCompile(`(?is)^\s*(FORCE\s+)?LOAD\s+(\S+)\s+FROM\b`)

// hintFor adds a suggestion to errors with a known common cause.
func hintFor(sql string, err error) error {
	if err == nil {
		return nil
	}
	if m := loadFromRe.FindStringSubmatch(sql); m != nil {
		return fmt.Errorf("%w\nhint: FROM goes with INSTALL; once installed, run LOAD %s;", err, m[2])
	}
	return err
}

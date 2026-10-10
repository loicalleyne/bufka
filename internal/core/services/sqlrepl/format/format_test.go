package format

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/couac"
)

var update = flag.Bool("update", false, "rewrite golden files")

// queryTable runs sql on a fresh in-memory DuckDB and returns the full
// result as a Table.
func queryTable(t *testing.T, sql string) *Table {
	t.Helper()
	db, err := couac.NewDuck(couac.WithDriverLookup())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Exec(context.Background(), "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	res, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	tbl := &Table{}
	for _, f := range res.Reader.Schema().Fields() {
		tbl.Columns = append(tbl.Columns, ColumnFor(f))
	}
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]Cell, rec.NumCols())
			for c := range row {
				row[c] = CellAt(rec.Column(c), r)
			}
			tbl.Head = append(tbl.Head, row)
		}
	}
	tbl.Total = int64(len(tbl.Head))
	return tbl
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update): %v", err)
	}
	if string(want) != got {
		t.Fatalf("%s mismatch:\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

const mixedSQL = `SELECT * FROM (VALUES
  (1, 'alpha', 1.5::DOUBLE, true, DATE '2026-10-09', TIMESTAMP '2026-10-09 12:34:56.789', NULL::VARCHAR, [1,2,3], {'a': 1, 'b': 'x'}, MAP {'k': 2}, 12.30::DECIMAL(10,2), '\x00ab'::BLOB, INTERVAL '1 day 2 hours'),
  (22, 'b|eta, "q"', -0.25::DOUBLE, false, DATE '1999-01-01', TIMESTAMP '1999-01-01 00:00:00', 'not null', [], {'a': NULL, 'b': 'it''s'}, MAP {}, -1.00::DECIMAL(10,2), ''::BLOB, INTERVAL '14 months'),
  (333, 'héllo 世界', NULL::DOUBLE, NULL, NULL, NULL, 'multi
line', NULL, NULL, NULL, NULL, NULL, NULL)
) t(id, name, score, ok, d, ts, note, ints, st, m, dec, b, iv)`

func TestModesGolden(t *testing.T) {
	tbl := queryTable(t, mixedSQL)
	for _, mode := range Modes {
		t.Run(string(mode), func(t *testing.T) {
			o := DefaultOptions()
			o.Mode = mode
			got, err := String(tbl, o)
			if err != nil {
				t.Fatal(err)
			}
			golden(t, "mixed_"+string(mode), got)
		})
	}
}

func TestDuckboxTruncatedGolden(t *testing.T) {
	full := queryTable(t, `SELECT i, 'row ' || i AS label FROM range(10) t(i)`)
	tbl := &Table{Columns: full.Columns, Head: full.Head[:2], Tail: full.Head[8:], Total: 10}
	got, err := String(tbl, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "duckbox_truncated", got)
}

func TestDuckboxMaxWidthGolden(t *testing.T) {
	tbl := queryTable(t, `SELECT 1 AS a, repeat('x', 60) AS long_text, 3 AS c, 4 AS d`)
	o := DefaultOptions()
	o.MaxWidth = 50
	got, err := String(tbl, o)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "duckbox_maxwidth", got)
}

func TestNullValueAndHeaders(t *testing.T) {
	tbl := queryTable(t, `SELECT NULL::INT AS n, 'v' AS s`)
	o := DefaultOptions()
	o.Mode = ModeList
	o.NullValue = "∅"
	o.Headers = false
	got, err := String(tbl, o)
	if err != nil {
		t.Fatal(err)
	}
	if got != "∅|v\n" {
		t.Fatalf("got %q", got)
	}
}

func TestParseMode(t *testing.T) {
	if m, err := ParseMode("CSV"); err != nil || m != ModeCSV {
		t.Fatalf("ParseMode(CSV) = %v, %v", m, err)
	}
	if _, err := ParseMode("nope"); err == nil {
		t.Fatal("expected error")
	}
}

// Package format renders query results the way the DuckDB CLI does, one
// writer per .mode. It works on an already materialized Table so it has no
// database dependency.
package format

import (
	"fmt"
	"io"
	"strings"
)

// Column describes one result column.
type Column struct {
	Name string
	// Type is the DuckDB type name (e.g. BIGINT, VARCHAR, INTEGER[]).
	Type string
	// Right aligns the column's cells right (numbers).
	Right bool
}

// Cell is one value: Text is its display form, Value a JSON-marshalable
// form (nil for NULL).
type Cell struct {
	Text  string
	Value any
	Null  bool
}

// Table is a (possibly truncated) query result: Head rows, then — when
// Total exceeds len(Head)+len(Tail) — an elision, then Tail rows.
type Table struct {
	Columns []Column
	Head    [][]Cell
	Tail    [][]Cell
	Total   int64
}

// Shown is the number of rows held.
func (t *Table) Shown() int { return len(t.Head) + len(t.Tail) }

// Truncated reports whether rows were left out.
func (t *Table) Truncated() bool { return t.Total > int64(t.Shown()) }

// Rows returns every held row (head then tail).
func (t *Table) Rows() [][]Cell {
	out := make([][]Cell, 0, t.Shown())
	out = append(out, t.Head...)
	return append(out, t.Tail...)
}

// Mode is an output mode, as set by .mode.
type Mode string

const (
	ModeDuckbox   Mode = "duckbox"
	ModeBox       Mode = "box"
	ModeTable     Mode = "table"
	ModeCSV       Mode = "csv"
	ModeTSV       Mode = "tsv"
	ModeJSON      Mode = "json"
	ModeJSONLines Mode = "jsonlines"
	ModeLine      Mode = "line"
	ModeMarkdown  Mode = "markdown"
	ModeList      Mode = "list"
)

// Modes lists every supported mode.
var Modes = []Mode{ModeDuckbox, ModeBox, ModeTable, ModeCSV, ModeTSV, ModeJSON, ModeJSONLines, ModeLine, ModeMarkdown, ModeList}

// ParseMode validates a .mode argument.
func ParseMode(s string) (Mode, error) {
	for _, m := range Modes {
		if strings.EqualFold(s, string(m)) {
			return m, nil
		}
	}
	names := make([]string, len(Modes))
	for i, m := range Modes {
		names[i] = string(m)
	}
	return "", fmt.Errorf("unknown mode %q (one of %s)", s, strings.Join(names, ", "))
}

// Truncates reports whether the mode shows only the first and last rows of
// a result (duckbox only, like the DuckDB CLI); the others print every row.
func (m Mode) Truncates() bool { return m == ModeDuckbox }

// Options control rendering.
type Options struct {
	Mode    Mode
	Headers bool
	// NullValue is how NULL is printed (not in json modes).
	NullValue string
	// ColSep / RowSep are the list-mode separators (csv uses ColSep too
	// when it is set to something other than "|").
	ColSep, RowSep string
	// MaxWidth bounds a duckbox/box/table line in display cells; 0 means
	// unbounded. Columns that do not fit are hidden.
	MaxWidth int
	// Widths are per-column widths set by .width (0 = automatic).
	Widths []int
}

// DefaultOptions are the DuckDB CLI defaults.
func DefaultOptions() Options {
	return Options{Mode: ModeDuckbox, Headers: true, NullValue: "NULL", ColSep: "|", RowSep: "\n"}
}

// Write renders t to w.
func Write(w io.Writer, t *Table, o Options) error {
	switch o.Mode {
	case ModeDuckbox:
		return writeBox(w, t, o, boxDuck)
	case ModeBox:
		return writeBox(w, t, o, boxPlain)
	case ModeTable:
		return writeBox(w, t, o, boxASCII)
	case ModeMarkdown:
		return writeMarkdown(w, t, o)
	case ModeCSV:
		sep := ","
		if o.ColSep != "" && o.ColSep != "|" {
			sep = o.ColSep
		}
		return writeDelimited(w, t, o, sep)
	case ModeTSV:
		return writeDelimited(w, t, o, "\t")
	case ModeJSON:
		return writeJSON(w, t, false)
	case ModeJSONLines:
		return writeJSON(w, t, true)
	case ModeLine:
		return writeLine(w, t, o)
	case ModeList:
		return writeList(w, t, o)
	}
	return fmt.Errorf("unknown mode %q", o.Mode)
}

// String renders t to a string.
func String(t *Table, o Options) (string, error) {
	var b strings.Builder
	err := Write(&b, t, o)
	return b.String(), err
}

func (o Options) text(c Cell) string {
	if c.Null {
		return o.NullValue
	}
	return c.Text
}

package format

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"strings"

	"github.com/charmbracelet/x/ansi"
)

func writeDelimited(w io.Writer, t *Table, o Options, sep string) error {
	var b bytes.Buffer
	if len(sep) == 1 && sep != "\t" {
		cw := csv.NewWriter(&b)
		cw.Comma = rune(sep[0])
		if o.Headers {
			names := make([]string, len(t.Columns))
			for i, c := range t.Columns {
				names[i] = c.Name
			}
			_ = cw.Write(names)
		}
		for _, row := range t.Rows() {
			vals := make([]string, len(row))
			for i, c := range row {
				if c.Null {
					vals[i] = ""
					if o.NullValue != "NULL" {
						vals[i] = o.NullValue
					}
					continue
				}
				vals[i] = c.Text
			}
			_ = cw.Write(vals)
		}
		cw.Flush()
		if err := cw.Error(); err != nil {
			return err
		}
		_, err := w.Write(b.Bytes())
		return err
	}
	// tsv or multi-character separators: no quoting, escape tabs/newlines.
	esc := strings.NewReplacer("\t", `\t`, "\n", `\n`, "\r", `\r`)
	if o.Headers {
		names := make([]string, len(t.Columns))
		for i, c := range t.Columns {
			names[i] = esc.Replace(c.Name)
		}
		b.WriteString(strings.Join(names, sep) + "\n")
	}
	for _, row := range t.Rows() {
		vals := make([]string, len(row))
		for i, c := range row {
			if c.Null {
				if o.NullValue != "NULL" {
					vals[i] = o.NullValue
				}
				continue
			}
			vals[i] = esc.Replace(c.Text)
		}
		b.WriteString(strings.Join(vals, sep) + "\n")
	}
	_, err := w.Write(b.Bytes())
	return err
}

// orderedRow marshals a row as a JSON object keeping column order.
type orderedRow struct {
	cols []Column
	row  []Cell
}

func (r orderedRow) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, c := range r.cols {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(c.Name)
		b.Write(k)
		b.WriteByte(':')
		v, err := json.Marshal(r.row[i].Value)
		if err != nil {
			v, _ = json.Marshal(r.row[i].Text)
		}
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func writeJSON(w io.Writer, t *Table, lines bool) error {
	var b bytes.Buffer
	rows := t.Rows()
	if !lines {
		b.WriteString("[")
	}
	for i, row := range rows {
		enc, err := orderedRow{t.Columns, row}.MarshalJSON()
		if err != nil {
			return err
		}
		switch {
		case lines:
			b.Write(enc)
			b.WriteString("\n")
		default:
			if i > 0 {
				b.WriteString(",\n")
			} else {
				b.WriteString("\n")
			}
			b.Write(enc)
		}
	}
	if !lines {
		if len(rows) > 0 {
			b.WriteString("\n")
		}
		b.WriteString("]\n")
	}
	_, err := w.Write(b.Bytes())
	return err
}

func writeLine(w io.Writer, t *Table, o Options) error {
	nameW := 0
	for _, c := range t.Columns {
		nameW = max(nameW, ansi.StringWidth(c.Name))
	}
	var b strings.Builder
	for r, row := range t.Rows() {
		if r > 0 {
			b.WriteString("\n")
		}
		for i, c := range t.Columns {
			b.WriteString(pad(c.Name, nameW, true) + " = " + o.text(row[i]) + "\n")
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeList(w io.Writer, t *Table, o Options) error {
	colSep, rowSep := o.ColSep, o.RowSep
	if colSep == "" {
		colSep = "|"
	}
	if rowSep == "" {
		rowSep = "\n"
	}
	var b strings.Builder
	if o.Headers {
		names := make([]string, len(t.Columns))
		for i, c := range t.Columns {
			names[i] = c.Name
		}
		b.WriteString(strings.Join(names, colSep) + rowSep)
	}
	for _, row := range t.Rows() {
		vals := make([]string, len(row))
		for i, c := range row {
			vals[i] = o.text(c)
		}
		b.WriteString(strings.Join(vals, colSep) + rowSep)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

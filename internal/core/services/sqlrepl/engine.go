package sqlrepl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
)

// Action asks the screen to do something beyond printing a block.
type Action int

const (
	ActionNone Action = iota
	// ActionExit leaves the REPL.
	ActionExit
	// ActionClear clears the screen's scrollback.
	ActionClear
	// ActionBrowse opens Session.Last in the table browser.
	ActionBrowse
	// ActionConfirm asks a y/n question; on yes the screen runs
	// Block.Confirm as new input.
	ActionConfirm
)

// Block is one piece of REPL output: the echo of what ran and its result.
type Block struct {
	// Input is the statement or dot-command that produced this block.
	Input string
	// Text is the rendered output (may be empty).
	Text string
	// Info is a dim status line (timer, rows affected, notes).
	Info string
	Err  error
	// Action is a request for the screen.
	Action Action
	// Confirm is the input to run when the user accepts an ActionConfirm.
	Confirm string
	// Table and Format are set when Text is a rendered result, so a screen
	// can re-render it (e.g. at a new width) with RenderTable.
	Table  *format.Table
	Format format.Options
	// FitScreen is true when Format.MaxWidth follows the screen width (no
	// explicit .maxwidth), so a re-render should use the new width.
	FitScreen bool
}

// maxReadDepth bounds nested .read.
const maxReadDepth = 8

// Execute runs every complete unit of input (SQL statements and
// dot-commands) in order and returns one block per unit. Execution stops
// at the first error when .bail is on, or when ctx is cancelled. An
// incomplete trailing statement is run as-is (the screen only calls
// Execute with incomplete input when the user forces it with ctrl+enter).
func (s *Session) Execute(ctx context.Context, input string) []Block {
	return s.execute(ctx, input, 0)
}

func (s *Session) execute(ctx context.Context, input string, depth int) []Block {
	units, rest := Split(input)
	if !onlyNoise(rest) {
		units = append(units, Unit{Text: strings.TrimSpace(rest)})
	}
	var blocks []Block
	for _, u := range units {
		if err := ctx.Err(); err != nil {
			blocks = append(blocks, Block{Input: u.Text, Err: err})
			break
		}
		var bs []Block
		if u.Dot {
			bs = s.runDot(ctx, u.Text, depth)
		} else {
			bs = []Block{s.runSQL(ctx, u.Text)}
		}
		blocks = append(blocks, bs...)
		failed := false
		for _, b := range bs {
			if b.Err != nil {
				failed = true
			}
			if b.Action == ActionExit {
				return blocks
			}
		}
		if failed && s.Settings.Bail {
			break
		}
	}
	return blocks
}

// runSQL runs one statement and renders its result.
func (s *Session) runSQL(ctx context.Context, sql string) Block {
	res := s.Run(ctx, sql)
	res.Err = hintFor(sql, res.Err)
	if res.Full != nil {
		s.Last, s.LastSQL = res.Full, sql
	}
	return s.render(res)
}

// render turns a Result into a Block, honouring .output/.once, .timer and
// .changes.
func (s *Session) render(res Result) Block {
	b := Block{Input: res.SQL, Err: res.Err}
	var info []string
	if res.Table != nil && (res.Err == nil || res.Table.Total > 0) {
		text, err := s.renderTable(res.Table)
		if err != nil {
			b.Err = err
		}
		if s.output != nil {
			if _, err := s.output.Write([]byte(text)); err != nil {
				b.Err = fmt.Errorf("write %s: %w", s.outPath, err)
			}
			info = append(info, fmt.Sprintf("%d rows written to %s", res.Table.Total, s.outPath))
			if s.onceOnly {
				if err := s.finishOnce(); err != nil {
					b.Err = err
				}
			}
		} else {
			b.Text = text
			b.Table, b.Format, b.FitScreen = res.Table, s.Settings.Format, !s.maxWidthSet
		}
	}
	if res.RowsAffected >= 0 && s.Settings.Changes {
		info = append(info, fmt.Sprintf("changes: %d", res.RowsAffected))
	}
	if s.Settings.Timer {
		info = append(info, fmt.Sprintf("Run Time: %s", res.Elapsed.Round(time.Microsecond)))
	}
	b.Info = strings.Join(info, " · ")
	return b
}

// renderTable formats t with the current settings. EXPLAIN output is shown
// as its plan text, like the DuckDB CLI.
func (s *Session) renderTable(t *format.Table) (string, error) {
	o := s.Settings.Format
	if s.output != nil {
		// Files get the full rows, no width limit.
		o.MaxWidth = 0
	}
	return RenderTable(t, o)
}

// RenderTable formats a result with o. EXPLAIN output is shown as its plan
// text, like the DuckDB CLI.
func RenderTable(t *format.Table, o format.Options) (string, error) {
	if len(t.Columns) == 2 && t.Columns[0].Name == "explain_key" && t.Columns[1].Name == "explain_value" {
		var b strings.Builder
		for _, row := range t.Rows() {
			b.WriteString(row[1].Text)
			if !strings.HasSuffix(row[1].Text, "\n") {
				b.WriteString("\n")
			}
		}
		return b.String(), nil
	}
	return format.String(t, o)
}

// path resolves a file argument against the session directory.
func (s *Session) path(p string) string {
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			p = filepath.Join(home, p[2:])
		}
	}
	if filepath.IsAbs(p) || s.Dir == "" {
		return p
	}
	return filepath.Join(s.Dir, p)
}

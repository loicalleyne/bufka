package sqlrepl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
)

// shellOutputLimit bounds what .shell keeps of a command's output.
const shellOutputLimit = 1 << 20

func init() {
	register(&dotCommand{
		name: "read", usage: ".read FILE", group: groupIO,
		help: "Run the SQL and dot-commands in FILE",
		run: func(ctx context.Context, s *Session, args []string, depth int) []Block {
			if len(args) != 1 {
				return usageErr("read")
			}
			if depth >= maxReadDepth {
				return fail(fmt.Errorf(".read nested more than %d deep", maxReadDepth))
			}
			data, err := os.ReadFile(s.path(args[0]))
			if err != nil {
				return fail(err)
			}
			blocks := s.execute(ctx, string(data), depth+1)
			if !s.Settings.Echo {
				for i := range blocks {
					blocks[i].Input = ""
				}
			}
			return append([]Block{{Info: fmt.Sprintf("read %s", args[0])}}, blocks...)
		},
	})
	register(&dotCommand{
		name: "output", usage: ".output [FILE]", group: groupIO,
		help: "Send query results to FILE (appending is not supported: it is overwritten); no argument: back to the screen",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			s.closeOutput()
			if len(args) == 0 {
				return nil
			}
			if len(args) != 1 {
				return usageErr("output")
			}
			if err := s.openOutput(s.path(args[0]), false, false); err != nil {
				return fail(err)
			}
			return []Block{{Info: "results now go to " + s.outPath}}
		},
	})
	register(&dotCommand{
		name: "once", usage: ".once [-x] [FILE]", group: groupIO,
		help: "Send only the next query result to FILE; -x opens it (as csv) in the default app, e.g. a spreadsheet",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			s.closeOutput()
			openX := false
			if len(args) > 0 && args[0] == "-x" {
				openX = true
				args = args[1:]
			}
			var path string
			switch {
			case len(args) == 1:
				path = s.path(args[0])
			case len(args) == 0 && openX:
				f, err := os.CreateTemp("", "bufka-*.csv")
				if err != nil {
					return fail(err)
				}
				path = f.Name()
				_ = f.Close()
			default:
				return usageErr("once")
			}
			if openX && s.Deps.OpenURL == nil {
				return fail(errors.New(".once -x is not available here"))
			}
			if err := s.openOutput(path, true, openX); err != nil {
				return fail(err)
			}
			return []Block{{Info: "the next result goes to " + s.outPath}}
		},
	})
	register(&dotCommand{
		name: "export", usage: ".export FILE", group: groupIO,
		help: "Write the last query's full result to FILE: .csv, .tsv, .parquet, .json or .jsonl (by extension)",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("export")
			}
			if s.LastSQL == "" {
				return fail(errors.New("no query result to export yet"))
			}
			path := s.path(args[0])
			opts, err := copyOptions(path)
			if err != nil {
				return fail(err)
			}
			// Re-run the query through COPY so the file holds every row, not
			// just what was kept for display.
			query := strings.TrimSuffix(strings.TrimSpace(s.LastSQL), ";")
			sql := fmt.Sprintf("COPY (%s) TO %s (%s)", query, quoteString(path), opts)
			res := s.Run(ctx, sql)
			if res.Err != nil {
				return fail(res.Err)
			}
			n := res.RowsAffected
			return []Block{{Info: fmt.Sprintf("exported %d rows to %s", n, path)}}
		},
	})
	register(&dotCommand{
		name: "shell", aliases: []string{"system"}, usage: ".shell CMD ...", group: groupIO,
		help: "Run a shell command and show its output",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) == 0 {
				return usageErr("shell")
			}
			return s.shell(ctx, strings.Join(args, " "))
		},
	})
	register(&dotCommand{
		name: "cd", usage: ".cd DIR", group: groupIO,
		help: "Change the directory used for relative file names (no argument: show it)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			if len(args) == 0 {
				return info("%s\n", s.cwd())
			}
			if len(args) != 1 {
				return usageErr("cd")
			}
			dir := s.path(args[0])
			st, err := os.Stat(dir)
			if err != nil {
				return fail(err)
			}
			if !st.IsDir() {
				return fail(fmt.Errorf("%s is not a directory", dir))
			}
			abs, err := filepath.Abs(dir)
			if err != nil {
				return fail(err)
			}
			s.Dir = abs
			return []Block{{Info: abs}}
		},
	})
	register(&dotCommand{
		name: "open", aliases: []string{"attach"}, usage: ".open FILE [ALIAS] [--readonly]", group: groupIO,
		help: "ATTACH another database file (DuckDB, SQLite, …) under ALIAS (default: file name) and USE it",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			readOnly := false
			var rest []string
			for _, a := range args {
				if a == "--readonly" || a == "-readonly" {
					readOnly = true
					continue
				}
				rest = append(rest, a)
			}
			if len(rest) < 1 || len(rest) > 2 {
				return usageErr("open")
			}
			path := s.path(rest[0])
			alias := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
			if len(rest) == 2 {
				alias = rest[1]
			}
			sql := fmt.Sprintf("ATTACH %s AS %s", quoteString(path), quoteIdent(alias))
			if readOnly {
				sql += " (READ_ONLY)"
			}
			if res := s.Run(ctx, sql); res.Err != nil {
				return fail(res.Err)
			}
			if res := s.Run(ctx, "USE "+quoteIdent(alias)); res.Err != nil {
				return fail(res.Err)
			}
			return []Block{{Info: fmt.Sprintf("attached %s as %s (now in use; .detach %s to remove)", path, alias, alias)}}
		},
	})
	register(&dotCommand{
		name: "detach", usage: ".detach ALIAS", group: groupIO,
		help: "DETACH a database attached with .open (switches back to the bufka database first)",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("detach")
			}
			// Detaching the database in use fails: go back to the default one.
			if res := s.Run(ctx, `SELECT current_database() = `+quoteString(args[0])+`, (SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_oid LIMIT 1)`); res.Err == nil && res.Table != nil && res.Table.Total > 0 {
				row := res.Table.Rows()[0]
				if row[0].Text == "true" {
					if r := s.Run(ctx, "USE "+quoteIdent(row[1].Text)); r.Err != nil {
						return fail(r.Err)
					}
				}
			}
			if res := s.Run(ctx, "DETACH "+quoteIdent(args[0])); res.Err != nil {
				return fail(res.Err)
			}
			return []Block{{Info: "detached " + args[0]}}
		},
	})
}

// copyOptions picks COPY options from a file extension.
func copyOptions(path string) (string, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		return "FORMAT csv, HEADER", nil
	case ".tsv":
		return "FORMAT csv, HEADER, DELIMITER '\t'", nil
	case ".parquet":
		return "FORMAT parquet", nil
	case ".json":
		return "FORMAT json, ARRAY true", nil
	case ".jsonl", ".ndjson":
		return "FORMAT json", nil
	}
	return "", fmt.Errorf("cannot tell the format of %s: use .csv, .tsv, .parquet, .json or .jsonl", path)
}

// cwd is the directory relative file names resolve against.
func (s *Session) cwd() string {
	if s.Dir != "" {
		return s.Dir
	}
	wd, _ := os.Getwd()
	return wd
}

func (s *Session) openOutput(path string, once, openX bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	s.output, s.outPath, s.onceOnly, s.onceOpenX = f, path, once, openX
	if openX {
		// A spreadsheet wants csv.
		s.onceMode = s.Settings.Format.Mode
		s.Settings.Format.Mode = format.ModeCSV
	}
	return nil
}

// finishOnce closes a .once file and, for -x, opens it.
func (s *Session) finishOnce() error {
	path, openX := s.outPath, s.onceOpenX
	s.closeOutput()
	if openX && s.Deps.OpenURL != nil {
		return s.Deps.OpenURL(path)
	}
	return nil
}

func (s *Session) closeOutput() {
	if s.output != nil {
		_ = s.output.Close()
	}
	if s.onceOpenX && s.onceMode != "" {
		s.Settings.Format.Mode = s.onceMode
	}
	s.output, s.outPath, s.onceOnly, s.onceOpenX, s.onceMode = nil, "", false, false, ""
}

func (s *Session) shell(ctx context.Context, cmdline string) []Block {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/C", cmdline)
	} else {
		sh := os.Getenv("SHELL")
		if sh == "" {
			sh = "/bin/sh"
		}
		cmd = exec.CommandContext(ctx, sh, "-c", cmdline)
	}
	cmd.Dir = s.cwd()
	var out limitedBuffer
	out.limit = shellOutputLimit
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	b := Block{Text: out.String()}
	if out.truncated {
		b.Info = "output truncated"
	}
	if err != nil {
		b.Err = err
	}
	return []Block{b}
}

type limitedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if room := l.limit - l.Len(); room < len(p) {
		l.truncated = true
		if room > 0 {
			l.Buffer.Write(p[:room])
		}
		return len(p), nil
	}
	return l.Buffer.Write(p)
}

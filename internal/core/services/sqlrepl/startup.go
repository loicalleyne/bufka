package sqlrepl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// startupTable stores the REPL's startup script (what the DuckDB CLI keeps
// in ~/.duckdbrc) in the database, so all bufka settings live in DuckDB.
const startupTable = "sql_repl_settings"

const startupKey = "startup_script"

// StartupScript returns the saved startup script ("" when none).
func (s *Session) StartupScript(ctx context.Context) (string, error) {
	table := quoteIdent(s.defaultDB(ctx)) + "." + startupTable
	res, err := s.Conn.Query(ctx, `SELECT value FROM `+table+` WHERE key = '`+startupKey+`'`)
	if err != nil {
		// The table does not exist until a script is first saved.
		if strings.Contains(err.Error(), "does not exist") {
			return "", nil
		}
		return "", cleanErr(err)
	}
	defer res.Close()
	var out string
	for res.Reader.Next() {
		col := res.Reader.RecordBatch().Column(0).(*array.String)
		if col.Len() > 0 && col.IsValid(0) {
			out = strings.Clone(col.Value(0))
		}
	}
	return out, cleanErr(res.Reader.Err())
}

// SetStartupScript saves the startup script; "" removes it.
func (s *Session) SetStartupScript(ctx context.Context, script string) error {
	table := quoteIdent(s.defaultDB(ctx)) + "." + startupTable
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + table + ` (key VARCHAR PRIMARY KEY, value VARCHAR)`,
		`DELETE FROM ` + table + ` WHERE key = '` + startupKey + `'`,
	}
	if strings.TrimSpace(script) != "" {
		stmts = append(stmts, `INSERT INTO `+table+` VALUES ('`+startupKey+`', `+quoteString(script)+`)`)
	}
	for _, q := range stmts {
		if _, err := s.Conn.Exec(ctx, q); err != nil {
			return cleanErr(err)
		}
	}
	return nil
}

// defaultDB is bufka's own database (the first attached), so the script is
// saved there even after .open switched to another database.
func (s *Session) defaultDB(ctx context.Context) string {
	res, err := s.Conn.Query(ctx, `SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_oid LIMIT 1`)
	if err != nil {
		return "memory"
	}
	defer res.Close()
	name := "memory"
	for res.Reader.Next() {
		col := res.Reader.RecordBatch().Column(0).(*array.String)
		if col.Len() > 0 {
			name = strings.Clone(col.Value(0))
		}
	}
	return name
}

// RunStartup runs the saved startup script, if any.
func (s *Session) RunStartup(ctx context.Context) []Block {
	script, err := s.StartupScript(ctx)
	if err != nil {
		return []Block{{Err: fmt.Errorf("read startup script: %w", err)}}
	}
	if strings.TrimSpace(script) == "" {
		return nil
	}
	blocks := s.execute(ctx, script, 1)
	// Only show what failed: a clean startup is silent.
	var out []Block
	for _, b := range blocks {
		if b.Err != nil {
			b.Info = strings.TrimPrefix(b.Info+" · in the startup script (.sqlrc)", " · ")
			out = append(out, b)
		}
	}
	return out
}

func init() {
	register(&dotCommand{
		name: "sqlrc", usage: ".sqlrc [show|set TEXT|add LINE|clear|run|import FILE]", group: groupREPL,
		help: "The startup script run when the REPL opens, saved in the database (like ~/.duckdbrc)",
		run: func(ctx context.Context, s *Session, args []string, depth int) []Block {
			sub := "show"
			if len(args) > 0 {
				sub = strings.ToLower(args[0])
				args = args[1:]
			}
			cur, err := s.StartupScript(ctx)
			if err != nil {
				return fail(err)
			}
			save := func(script, msg string) []Block {
				if err := s.SetStartupScript(ctx, script); err != nil {
					return fail(err)
				}
				return []Block{{Info: msg}}
			}
			switch sub {
			case "show":
				if strings.TrimSpace(cur) == "" {
					return info("no startup script: add lines with .sqlrc add, e.g. .sqlrc add .mode box\n")
				}
				return []Block{{Text: strings.TrimRight(cur, "\n") + "\n"}}
			case "set":
				if len(args) == 0 {
					return usageErr("sqlrc")
				}
				return save(unescape(strings.Join(args, " "))+"\n", "startup script replaced")
			case "add":
				if len(args) == 0 {
					return usageErr("sqlrc")
				}
				line := strings.Join(args, " ")
				if cur != "" && !strings.HasSuffix(cur, "\n") {
					cur += "\n"
				}
				return save(cur+line+"\n", "added to the startup script: "+line)
			case "clear":
				return save("", "startup script cleared")
			case "run":
				return append([]Block{{Info: "running the startup script"}}, s.execute(ctx, cur, depth+1)...)
			case "import":
				if len(args) != 1 {
					return usageErr("sqlrc")
				}
				data, err := os.ReadFile(s.path(args[0]))
				if err != nil {
					return fail(err)
				}
				return save(string(data), "startup script imported from "+args[0])
			}
			return fail(errors.New("usage: " + lookupDot("sqlrc").usage))
		},
	})
}

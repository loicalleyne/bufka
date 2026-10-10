package sqlrepl

import (
	"context"
	"fmt"
	"strings"
)

// queryBlock runs sql and renders it like a user query, but keeps the
// command line as the block's input and leaves Session.Last alone.
func (s *Session) queryBlock(ctx context.Context, sql string) []Block {
	res := s.Run(ctx, sql)
	b := s.render(res)
	b.Input = ""
	return []Block{b}
}

// likePattern turns an optional shell-ish pattern into a SQL LIKE pattern.
func likePattern(args []string) string {
	if len(args) == 0 {
		return "%"
	}
	return strings.NewReplacer("*", "%", "?", "_").Replace(args[0])
}

func init() {
	register(&dotCommand{
		name: "tables", usage: ".tables [PATTERN]", group: groupSchema,
		help: "List tables and views (LIKE pattern; * and ? work too)",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			sql := fmt.Sprintf(`SELECT database_name AS database, schema_name AS schema, table_name AS name, kind
FROM (
  SELECT database_name, schema_name, table_name, 'table' AS kind FROM duckdb_tables() WHERE NOT internal
  UNION ALL
  SELECT database_name, schema_name, view_name, 'view' FROM duckdb_views() WHERE NOT internal
)
WHERE table_name ILIKE %s
ORDER BY database_name, schema_name, table_name`, quoteString(likePattern(args)))
			return s.queryBlock(ctx, sql)
		},
	})
	register(&dotCommand{
		name: "schema", usage: ".schema [PATTERN]", group: groupSchema,
		help: "Show the CREATE statements of matching tables, views and indexes",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			pat := quoteString(likePattern(args))
			sql := fmt.Sprintf(`SELECT sql FROM (
  SELECT table_name AS name, sql, 1 AS o FROM duckdb_tables() WHERE NOT internal AND table_name ILIKE %[1]s
  UNION ALL SELECT view_name, sql, 2 FROM duckdb_views() WHERE NOT internal AND view_name ILIKE %[1]s
  UNION ALL SELECT table_name, sql, 3 FROM duckdb_indexes() WHERE table_name ILIKE %[1]s AND sql IS NOT NULL
) ORDER BY o, name`, pat)
			res := s.Run(ctx, sql)
			if res.Err != nil {
				return fail(res.Err)
			}
			var b strings.Builder
			for _, row := range res.Table.Rows() {
				stmt := strings.TrimSpace(row[0].Text)
				if !strings.HasSuffix(stmt, ";") {
					stmt += ";"
				}
				b.WriteString(stmt + "\n")
			}
			if b.Len() == 0 {
				return info("no matching tables\n")
			}
			return []Block{{Text: b.String()}}
		},
	})
	register(&dotCommand{
		name: "describe", usage: ".describe TABLE", group: groupSchema,
		help: "Columns and types of a table, view or (quoted) query",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("describe")
			}
			return s.queryBlock(ctx, "DESCRIBE "+relation(args[0]))
		},
	})
	register(&dotCommand{
		name: "summarize", usage: ".summarize TABLE", group: groupSchema,
		help: "Per-column statistics (min, max, nulls, distinct …) of a table or (quoted) query",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("summarize")
			}
			return s.queryBlock(ctx, "SUMMARIZE "+relation(args[0]))
		},
	})
	register(&dotCommand{
		name: "databases", usage: ".databases", group: groupSchema,
		help: "Attached databases",
		run: func(ctx context.Context, s *Session, _ []string, _ int) []Block {
			return s.queryBlock(ctx, `SELECT database_name AS name, path, readonly, type FROM duckdb_databases() WHERE NOT internal ORDER BY database_oid`)
		},
	})
	register(&dotCommand{
		name: "indexes", aliases: []string{"indices"}, usage: ".indexes [TABLE]", group: groupSchema,
		help: "Indexes, optionally only those of TABLE",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			where := ""
			if len(args) > 0 {
				where = " WHERE table_name ILIKE " + quoteString(likePattern(args))
			}
			return s.queryBlock(ctx, `SELECT index_name, table_name, is_unique, expressions FROM duckdb_indexes()`+where+` ORDER BY table_name, index_name`)
		},
	})
	register(&dotCommand{
		name: "settings", usage: ".settings [PATTERN]", group: groupSchema,
		help: "DuckDB settings whose name matches PATTERN",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			return s.queryBlock(ctx, fmt.Sprintf(`SELECT name, value, description FROM duckdb_settings() WHERE name ILIKE %s ORDER BY name`, quoteString("%"+strings.Trim(likePattern(args), "%")+"%")))
		},
	})
	register(&dotCommand{
		name: "extensions", usage: ".extensions", group: groupSchema,
		help: "Installed and loaded DuckDB extensions",
		run: func(ctx context.Context, s *Session, _ []string, _ int) []Block {
			return s.queryBlock(ctx, `SELECT extension_name, loaded, installed, extension_version, description FROM duckdb_extensions() WHERE installed OR loaded ORDER BY extension_name`)
		},
	})
}

// relation is a table name (quoted when needed) or a parenthesized query.
func relation(arg string) string {
	a := strings.TrimSpace(arg)
	up := strings.ToUpper(a)
	if strings.HasPrefix(a, "(") || strings.HasPrefix(up, "SELECT ") || strings.HasPrefix(up, "FROM ") || strings.HasPrefix(up, "WITH ") {
		a = strings.TrimSuffix(a, ";")
		if !strings.HasPrefix(a, "(") {
			a = "(" + a + ")"
		}
		return a
	}
	if isPlainIdentPath(a) {
		return a
	}
	return quoteIdent(a)
}

func isPlainIdentPath(s string) bool {
	for _, p := range strings.Split(s, ".") {
		if p == "" {
			return false
		}
		if strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`) && len(p) >= 2 {
			continue
		}
		for i, r := range p {
			if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9')) {
				return false
			}
		}
	}
	return true
}

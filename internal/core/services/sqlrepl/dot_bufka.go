package sqlrepl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
)

// defaultTailRows is .tail's default row count.
const defaultTailRows = 20

func init() {
	register(&dotCommand{
		name: "topics", usage: ".topics", group: groupBufka,
		help: "Cached Kafka topics: table, rows, latest _kafka_offset/_kafka_timestamp and sync status",
		run: func(ctx context.Context, s *Session, _ []string, _ int) []Block {
			if s.Deps.Topics == nil {
				return fail(errors.New(".topics is not available here"))
			}
			topics, err := s.Deps.Topics(ctx)
			if err != nil {
				return fail(err)
			}
			if len(topics) == 0 {
				return info("no topics are cached: add one in kafka > cache\n")
			}
			return []Block{{Text: s.topicsTable(ctx, topics)}}
		},
	})
	register(&dotCommand{
		name: "sync", usage: ".sync TOPIC", group: groupBufka,
		help: "Start (or resume) the background sync of a cached topic",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("sync")
			}
			if s.Deps.Sync == nil {
				return fail(errors.New(".sync is not available here"))
			}
			if err := s.Deps.Sync(ctx, args[0]); err != nil {
				return fail(err)
			}
			return []Block{{Info: "syncing " + args[0] + " in the background (.topics shows progress)"}}
		},
	})
	register(&dotCommand{
		name: "tail", usage: ".tail TOPIC [N]", group: groupBufka,
		help: fmt.Sprintf("The latest N (default %d) cached messages of a topic, newest first", defaultTailRows),
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) < 1 || len(args) > 2 {
				return usageErr("tail")
			}
			n := defaultTailRows
			if len(args) == 2 {
				v, err := strconv.Atoi(args[1])
				if err != nil || v <= 0 {
					return fail(fmt.Errorf("N must be a positive number, got %q", args[1]))
				}
				n = v
			}
			table, err := s.topicTable(ctx, args[0])
			if err != nil {
				return fail(err)
			}
			sql := fmt.Sprintf(`SELECT * FROM %s ORDER BY _kafka_timestamp DESC, _kafka_offset DESC LIMIT %d;`, quoteIdent(table), n)
			res := s.Run(ctx, sql)
			b := s.render(res)
			b.Input = ""
			b.Info = strings.TrimPrefix(strings.Join([]string{b.Info, "from " + table}, " · "), " · ")
			return []Block{b}
		},
	})
	register(&dotCommand{
		name: "ui", usage: ".ui", group: groupBufka,
		help: "Start DuckDB's browser UI (localhost only) and open it",
		run: func(ctx context.Context, s *Session, _ []string, _ int) []Block {
			if s.Deps.StartUI == nil {
				return fail(errors.New(".ui is not available here"))
			}
			url, err := s.Deps.StartUI(ctx)
			if err != nil {
				return fail(err)
			}
			if s.Deps.OpenURL != nil {
				if err := s.Deps.OpenURL(url); err != nil {
					return []Block{{Info: "DuckDB UI running: open " + url + " (" + err.Error() + ")"}}
				}
				return []Block{{Info: "DuckDB UI running at " + url + " (opened in your browser)"}}
			}
			return []Block{{Info: "DuckDB UI running at " + url}}
		},
	})
	register(&dotCommand{
		name: "explain", usage: ".explain [analyze] QUERY", group: groupBufka,
		help: "Show the query plan (analyze runs the query and adds timings)",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if len(args) == 0 {
				return usageErr("explain")
			}
			prefix := "EXPLAIN "
			if strings.EqualFold(args[0], "analyze") {
				prefix = "EXPLAIN ANALYZE "
				args = args[1:]
			}
			if len(args) == 0 {
				return usageErr("explain")
			}
			q := strings.TrimSuffix(strings.TrimSpace(strings.Join(args, " ")), ";")
			return s.queryBlock(ctx, prefix+q)
		},
	})
	register(&dotCommand{
		name: "compact", usage: ".compact", group: groupBufka,
		help: "Checkpoint and compact the database to reclaim disk space (asks first; blocks queries and syncs while it runs)",
		run: func(ctx context.Context, s *Session, args []string, _ int) []Block {
			if s.Deps.Compact == nil {
				return fail(errors.New(".compact is not available here"))
			}
			if len(args) == 1 && args[0] == "--yes" {
				if err := s.Deps.Compact(ctx); err != nil {
					return fail(err)
				}
				return []Block{{Info: "database compacted"}}
			}
			if len(args) != 0 {
				return usageErr("compact")
			}
			return []Block{{
				Action:  ActionConfirm,
				Text:    "Compact the database now? Queries and topic syncs wait until it finishes.",
				Confirm: ".compact --yes",
			}}
		},
	})
	register(&dotCommand{
		name: "browse", usage: ".browse", group: groupBufka,
		help: "Open the last query result in a scrollable table browser",
		run: func(_ context.Context, s *Session, _ []string, _ int) []Block {
			if s.Last == nil {
				return fail(errors.New("no query result to browse yet"))
			}
			return []Block{{Action: ActionBrowse}}
		},
	})
}

// topicTable resolves a topic (or its table name) to its cache table.
func (s *Session) topicTable(ctx context.Context, name string) (string, error) {
	if s.Deps.Topics != nil {
		topics, err := s.Deps.Topics(ctx)
		if err == nil {
			for _, t := range topics {
				if t.Topic == name || t.Table == name {
					return t.Table, nil
				}
			}
		}
	}
	if strings.HasPrefix(name, "kafka_") && isPlainIdentPath(name) {
		return name, nil
	}
	return kafkaingest.TableName(name)
}

// topicsTable builds the .topics table: per topic, its cache table's row
// count and latest offset/timestamp (blank when the table does not exist
// yet) and sync status.
func (s *Session) topicsTable(ctx context.Context, topics []TopicCache) string {
	cols := []format.Column{
		{Name: "topic", Type: "VARCHAR"},
		{Name: "table", Type: "VARCHAR"},
		{Name: "rows", Type: "BIGINT", Right: true},
		{Name: "max_offset", Type: "BIGINT", Right: true},
		{Name: "latest", Type: "TIMESTAMP"},
		{Name: "sync", Type: "VARCHAR"},
	}
	tbl := &format.Table{Columns: cols}
	for _, t := range topics {
		rows, maxOff, latest := format.Cell{Null: true}, format.Cell{Null: true}, format.Cell{Null: true}
		res := s.Run(ctx, fmt.Sprintf(`SELECT count(*), max(_kafka_offset), max(_kafka_timestamp)::VARCHAR FROM %s`, quoteIdent(t.Table)))
		if res.Err == nil && res.Table != nil && res.Table.Total == 1 {
			r := res.Table.Rows()[0]
			rows, maxOff, latest = r[0], r[1], r[2]
		}
		status := "paused"
		if t.Active {
			status = "running"
		}
		if t.Err != "" {
			status = "error: " + t.Err
		}
		tbl.Head = append(tbl.Head, []format.Cell{
			{Text: t.Topic}, {Text: t.Table}, rows, maxOff, latest, {Text: status},
		})
	}
	tbl.Total = int64(len(tbl.Head))
	out, err := format.String(tbl, s.Settings.Format)
	if err != nil {
		return err.Error() + "\n"
	}
	return out
}

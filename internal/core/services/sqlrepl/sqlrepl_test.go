package sqlrepl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
	"github.com/loicalleyne/couac"
)

func newTestSession(t *testing.T, deps Deps) *Session {
	t.Helper()
	db, err := couac.NewDuck(couac.WithDriverLookup())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := NewSession(db, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.Dir = t.TempDir()
	return s
}

func run(t *testing.T, s *Session, input string) []Block {
	t.Helper()
	return s.Execute(context.Background(), input)
}

func mustOK(t *testing.T, blocks []Block) []Block {
	t.Helper()
	for _, b := range blocks {
		if b.Err != nil {
			t.Fatalf("%s: %v", b.Input, b.Err)
		}
	}
	return blocks
}

func TestRunQueryAndDML(t *testing.T) {
	s := newTestSession(t, Deps{})
	mustOK(t, run(t, s, "CREATE TABLE t(i INT); INSERT INTO t SELECT * FROM range(5);"))
	s.Settings.Changes = true
	b := mustOK(t, run(t, s, "INSERT INTO t VALUES (9);"))
	if b[0].Info != "changes: 1" {
		t.Fatalf("info = %q", b[0].Info)
	}
	b = mustOK(t, run(t, s, "SELECT count(*) AS Count FROM t;"))
	if !strings.Contains(b[0].Text, "│      6 │") {
		t.Fatalf("a query with a Count column must still be shown:\n%s", b[0].Text)
	}
	b = run(t, s, "SELEC 1;")
	if b[0].Err == nil || strings.Contains(b[0].Err.Error(), "couac") {
		t.Fatalf("err = %v", b[0].Err)
	}
}

func TestMaxRowsHeadTail(t *testing.T) {
	s := newTestSession(t, Deps{})
	s.Settings.MaxRows = 4
	res := s.Run(context.Background(), "SELECT * FROM range(100) t(i)")
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	tb := res.Table
	if len(tb.Head) != 2 || len(tb.Tail) != 2 || tb.Total != 100 {
		t.Fatalf("head=%d tail=%d total=%d", len(tb.Head), len(tb.Tail), tb.Total)
	}
	if tb.Tail[0][0].Text != "98" || tb.Tail[1][0].Text != "99" {
		t.Fatalf("tail = %v", tb.Tail)
	}
	if res.Full.Total != 100 {
		t.Fatalf("Full keeps every row, got %d", res.Full.Total)
	}
	s.Settings.Format.Mode = format.ModeCSV
	res = s.Run(context.Background(), "SELECT * FROM range(100) t(i)")
	if len(res.Table.Head) != 100 {
		t.Fatalf("csv shows every row, got %d", len(res.Table.Head))
	}
}

func TestBail(t *testing.T) {
	s := newTestSession(t, Deps{})
	if b := run(t, s, "SELECT nope; SELECT 1;"); len(b) != 2 {
		t.Fatalf("without bail both run, got %d", len(b))
	}
	mustOK(t, run(t, s, ".bail on"))
	if b := run(t, s, "SELECT nope; SELECT 1;"); len(b) != 1 {
		t.Fatalf("with bail only the first runs, got %d", len(b))
	}
}

func TestOutputDotCommands(t *testing.T) {
	s := newTestSession(t, Deps{})
	mustOK(t, run(t, s, ".mode csv\n.headers off\n.nullvalue -\n.timer on\n.maxrows 7\n.width 3 4"))
	st := s.Settings
	if st.Format.Mode != format.ModeCSV || st.Format.Headers || st.Format.NullValue != "-" || !st.Timer || st.MaxRows != 7 || len(st.Format.Widths) != 2 {
		t.Fatalf("settings = %+v", st)
	}
	b := mustOK(t, run(t, s, "SELECT 1, NULL;"))
	if b[0].Text != "1,-\n" || !strings.HasPrefix(b[0].Info, "Run Time:") {
		t.Fatalf("got %q / %q", b[0].Text, b[0].Info)
	}
	if b := run(t, s, ".mode nope"); b[0].Err == nil {
		t.Fatal("expected error")
	}
	if b := run(t, s, ".nosuch"); b[0].Err == nil {
		t.Fatal("expected error")
	}
	b = mustOK(t, run(t, s, ".show"))
	if !strings.Contains(b[0].Text, "mode: csv") {
		t.Fatalf(".show:\n%s", b[0].Text)
	}
	b = mustOK(t, run(t, s, ".help"))
	for _, c := range []string{".mode", ".tables", ".topics", ".browse", ".compact"} {
		if !strings.Contains(b[0].Text, c) {
			t.Errorf(".help lacks %s", c)
		}
	}
}

func TestIntrospection(t *testing.T) {
	s := newTestSession(t, Deps{})
	mustOK(t, run(t, s, "CREATE TABLE kafka_orders(_kafka_offset BIGINT, v VARCHAR); CREATE INDEX idx ON kafka_orders(v); CREATE VIEW vw AS SELECT 1 AS x;"))
	s.Settings.Format.Mode = format.ModeCSV
	for cmd, want := range map[string]string{
		".tables":                "kafka_orders,table",
		".tables kafka*":         "kafka_orders",
		".schema kafka_orders":   "CREATE TABLE kafka_orders",
		".describe kafka_orders": "_kafka_offset,BIGINT",
		".summarize vw":          "x,INTEGER",
		".databases":             "memory",
		".indexes":               "idx,kafka_orders",
		".settings threads":      "threads",
		".extensions":            "extension_name",
		".explain SELECT 1":      "PROJECTION",
	} {
		b := mustOK(t, run(t, s, cmd))
		if !strings.Contains(b[0].Text, want) {
			t.Errorf("%s: want %q in\n%s", cmd, want, b[0].Text)
		}
	}
	if s.Last != nil {
		t.Fatal("introspection commands must not replace the last result")
	}
}

func TestFilesAndShell(t *testing.T) {
	s := newTestSession(t, Deps{})
	dir := s.Dir
	if err := os.WriteFile(filepath.Join(dir, "init.sql"), []byte(".mode list\nCREATE TABLE r AS SELECT 42 AS a;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustOK(t, run(t, s, ".read init.sql"))
	if s.Settings.Format.Mode != format.ModeList {
		t.Fatal(".read did not apply .mode")
	}

	mustOK(t, run(t, s, ".output out.txt\nSELECT a FROM r;\n.output"))
	if got, _ := os.ReadFile(filepath.Join(dir, "out.txt")); string(got) != "a\n42\n" {
		t.Fatalf("out.txt = %q", got)
	}
	mustOK(t, run(t, s, ".once once.txt\nSELECT 1 AS b;\nSELECT 2 AS c;"))
	if got, _ := os.ReadFile(filepath.Join(dir, "once.txt")); string(got) != "b\n1\n" {
		t.Fatalf("once.txt = %q", got)
	}

	for _, ext := range []string{"csv", "parquet", "json", "jsonl", "tsv"} {
		mustOK(t, run(t, s, "SELECT * FROM range(3) t(i);"))
		mustOK(t, run(t, s, ".export x."+ext))
		if _, err := os.Stat(filepath.Join(dir, "x."+ext)); err != nil {
			t.Fatal(err)
		}
	}
	b := mustOK(t, run(t, s, "SELECT count(*) FROM read_parquet("+quoteString(filepath.Join(dir, "x.parquet"))+");"))
	if !strings.Contains(b[0].Text, "3") {
		t.Fatalf("parquet: %s", b[0].Text)
	}
	if b := run(t, s, ".export x.txt"); b[0].Err == nil {
		t.Fatal("unknown extension must fail")
	}

	b = mustOK(t, run(t, s, ".shell echo hi"))
	if b[0].Text != "hi\n" {
		t.Fatalf(".shell = %q", b[0].Text)
	}
	sub := filepath.Join(dir, "sub")
	_ = os.Mkdir(sub, 0o755)
	mustOK(t, run(t, s, ".cd sub"))
	if s.Dir != sub {
		t.Fatalf("dir = %s", s.Dir)
	}

	other := filepath.Join(dir, "other.duckdb")
	mustOK(t, run(t, s, ".open "+other+" o"))
	mustOK(t, run(t, s, "CREATE TABLE in_o(x INT);"))
	b = mustOK(t, run(t, s, "SELECT current_database();"))
	if !strings.Contains(b[0].Text, "o") {
		t.Fatalf("current db: %q", b[0].Text)
	}
	mustOK(t, run(t, s, ".detach o"))
}

func TestBufkaDotCommands(t *testing.T) {
	var synced, opened string
	compacted := false
	deps := Deps{
		Topics: func(context.Context) ([]TopicCache, error) {
			return []TopicCache{
				{Topic: "orders.v1", Table: "kafka_orders_v1", Active: true},
				{Topic: "empty", Table: "kafka_empty"},
			}, nil
		},
		Sync:    func(_ context.Context, topic string) error { synced = topic; return nil },
		StartUI: func(context.Context) (string, error) { return "http://localhost:4213/", nil },
		OpenURL: func(u string) error { opened = u; return nil },
		Compact: func(context.Context) error { compacted = true; return nil },
	}
	s := newTestSession(t, deps)
	mustOK(t, run(t, s, `CREATE TABLE kafka_orders_v1 AS
SELECT i AS _kafka_offset, 0 AS _kafka_partition, TIMESTAMP '2026-01-01' + to_seconds(i) AS _kafka_timestamp, NULL::BLOB AS _kafka_key, i * 10 AS v
FROM range(30) t(i);`))

	s.Settings.Format.Mode = format.ModeCSV
	b := mustOK(t, run(t, s, ".topics"))
	if !strings.Contains(b[0].Text, "orders.v1,kafka_orders_v1,30,29,2026-01-01 00:00:29,running") ||
		!strings.Contains(b[0].Text, "empty,kafka_empty,NULL,NULL,NULL,paused") && !strings.Contains(b[0].Text, "empty,kafka_empty,,,,paused") {
		t.Fatalf(".topics:\n%s", b[0].Text)
	}

	b = mustOK(t, run(t, s, ".tail orders.v1 3"))
	lines := strings.Split(strings.TrimSpace(b[0].Text), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[1], "29,") {
		t.Fatalf(".tail:\n%s", b[0].Text)
	}
	if b := run(t, s, ".tail orders.v1 x"); b[0].Err == nil {
		t.Fatal("bad N must fail")
	}

	mustOK(t, run(t, s, ".sync orders.v1"))
	if synced != "orders.v1" {
		t.Fatal("sync not called")
	}
	b = mustOK(t, run(t, s, ".ui"))
	if opened != "http://localhost:4213/" || !strings.Contains(b[0].Info, "4213") {
		t.Fatalf(".ui: %q opened=%q", b[0].Info, opened)
	}

	b = mustOK(t, run(t, s, ".compact"))
	if b[0].Action != ActionConfirm || compacted {
		t.Fatal(".compact must ask first")
	}
	mustOK(t, run(t, s, b[0].Confirm))
	if !compacted {
		t.Fatal("compact not called")
	}

	s.Last = nil
	if b := run(t, s, ".browse"); b[0].Err == nil {
		t.Fatal(".browse without a result must fail")
	}
	mustOK(t, run(t, s, "SELECT 1;"))
	if b := mustOK(t, run(t, s, ".browse")); b[0].Action != ActionBrowse {
		t.Fatal("expected ActionBrowse")
	}
	if b := mustOK(t, run(t, s, ".exit")); b[0].Action != ActionExit {
		t.Fatal("expected ActionExit")
	}

	// Unavailable dependencies are reported, not panics.
	bare := newTestSession(t, Deps{})
	for _, c := range []string{".topics", ".sync x", ".ui", ".compact"} {
		if b := run(t, bare, c); b[0].Err == nil {
			t.Errorf("%s without deps must fail", c)
		}
	}
}

func TestHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h")
	h, err := LoadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	h.Limit = 3
	for _, e := range []string{"SELECT 1;", "SELECT 1;", "SELECT\n  2;", "  ", ".tables", "SELECT 3;"} {
		if err := h.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	h2, err := LoadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	got := h2.Entries()
	want := []string{"SELECT\n  2;", ".tables", "SELECT 3;"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("entries = %q", got)
	}
	if i := h2.Search("select", h2.Len()); i != 2 {
		t.Fatalf("search = %d", i)
	}
	if i := h2.Search("select", 2); i != 0 {
		t.Fatalf("search older = %d", i)
	}
}

func TestComplete(t *testing.T) {
	s := newTestSession(t, Deps{Topics: func(context.Context) ([]TopicCache, error) {
		return []TopicCache{{Topic: "orders.v1", Table: "kafka_orders_v1"}}, nil
	}})
	mustOK(t, run(t, s, "CREATE TABLE kafka_orders_v1(_kafka_offset BIGINT);"))
	_ = os.WriteFile(filepath.Join(s.Dir, "query.sql"), nil, 0o644)

	has := func(sugg []Suggestion, text string) bool {
		for _, x := range sugg {
			if x.Text == text {
				return true
			}
		}
		return false
	}
	ctx := context.Background()

	in := "SELECT * FROM kafka_or"
	sugg := s.Complete(ctx, in, len(in))
	if !has(sugg, "kafka_orders_v1") {
		t.Fatalf("tables: %+v", sugg)
	}
	for _, x := range sugg {
		if x.Text == "kafka_orders_v1" && in[x.Start:] != "kafka_or" {
			t.Fatalf("start = %d", x.Start)
		}
	}

	in = "SELECT _kafka_o FROM kafka_orders_v1"
	if sugg := s.Complete(ctx, in, len("SELECT _kafka_o")); !has(sugg, "_kafka_offset") {
		t.Logf("column completion before FROM not offered by DuckDB: %+v", sugg)
	}

	in = "SELECT 'é' AS x, * FROM kafka_or"
	for _, x := range s.Complete(ctx, in, len(in)) {
		if x.Text == "kafka_orders_v1" && in[x.Start:] != "kafka_or" {
			t.Fatalf("multibyte start = %d (%q)", x.Start, in[x.Start:])
		}
	}

	if sugg := s.Complete(ctx, ".ta", 3); !has(sugg, ".tables") || !has(sugg, ".tail") {
		t.Fatalf("dot: %+v", sugg)
	}
	in = ".read que"
	if sugg := s.Complete(ctx, in, len(in)); !has(sugg, "query.sql") || sugg[0].Start != len(".read ") {
		t.Fatalf("path: %+v", sugg)
	}
	in = ".tail ord"
	if sugg := s.Complete(ctx, in, len(in)); !has(sugg, "orders.v1") {
		t.Fatalf("topic: %+v", sugg)
	}
	in = ".mode js"
	if sugg := s.Complete(ctx, in, len(in)); !has(sugg, "json") || !has(sugg, "jsonlines") {
		t.Fatalf("mode: %+v", sugg)
	}
	// A "." inside pending SQL is not a dot-command.
	in = "SELECT\n.5"
	for _, x := range s.Complete(ctx, in, len(in)) {
		if x.Kind == "command" {
			t.Fatalf("dot-command offered inside SQL: %+v", x)
		}
	}

	fb := s.completeFallback(ctx, "SEL")
	if !has(fb, "SELECT") {
		t.Fatalf("fallback: %+v", fb)
	}
	if p := CommonPrefix([]Suggestion{{Text: ".tables"}, {Text: ".tail"}}); p != ".ta" {
		t.Fatalf("prefix = %q", p)
	}
}

func TestSplitArgs(t *testing.T) {
	got, err := splitArgs(`once -x "my file.csv" 'a b' c\ d`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "|") != "once|-x|my file.csv|a b|c d" {
		t.Fatalf("%q", got)
	}
	if _, err := splitArgs(`x "open`); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatusStatementsAndLoadHint(t *testing.T) {
	s := newTestSession(t, Deps{})
	// LOAD of a built-in extension returns DuckDB's empty "Success" result.
	b := mustOK(t, run(t, s, "LOAD json;"))
	if b[0].Text != "" || b[0].Table != nil {
		t.Fatalf("LOAD must print nothing, got:\n%s", b[0].Text)
	}
	b = run(t, s, "load json from community;")
	if b[0].Err == nil || !strings.Contains(b[0].Err.Error(), "hint: FROM goes with INSTALL; once installed, run LOAD json;") {
		t.Fatalf("err = %v", b[0].Err)
	}
	// A real query with a Success column is still shown.
	b = mustOK(t, run(t, s, "SELECT true AS Success;"))
	if !strings.Contains(b[0].Text, "true") {
		t.Fatalf("query hidden:\n%s", b[0].Text)
	}
}

func TestSqlrcStoredInDatabase(t *testing.T) {
	s := newTestSession(t, Deps{})
	ctx := context.Background()
	b := mustOK(t, run(t, s, ".sqlrc"))
	if !strings.Contains(b[0].Text, "no startup script") {
		t.Fatalf("empty: %q", b[0].Text)
	}
	mustOK(t, run(t, s, ".sqlrc add .mode csv\n.sqlrc add .timer on"))
	if got, _ := s.StartupScript(ctx); got != ".mode csv\n.timer on\n" {
		t.Fatalf("script = %q", got)
	}
	// Saved in bufka's database even after switching to another one.
	mustOK(t, run(t, s, ".open "+filepath.Join(s.Dir, "o.duckdb")+" o"))
	if got, _ := s.StartupScript(ctx); got != ".mode csv\n.timer on\n" {
		t.Fatalf("after .open: %q", got)
	}
	mustOK(t, run(t, s, ".detach o"))

	if blocks := s.RunStartup(ctx); len(blocks) != 0 {
		t.Fatalf("a clean startup is silent: %+v", blocks)
	}
	if s.Settings.Format.Mode != format.ModeCSV || !s.Settings.Timer {
		t.Fatalf("startup not applied: %+v", s.Settings)
	}

	mustOK(t, run(t, s, `.sqlrc set ".mode box\nSELECT nope;"`))
	blocks := s.RunStartup(ctx)
	if len(blocks) != 1 || blocks[0].Err == nil || !strings.Contains(blocks[0].Info, ".sqlrc") {
		t.Fatalf("startup errors must be reported: %+v", blocks)
	}

	file := filepath.Join(s.Dir, "rc.sql")
	_ = os.WriteFile(file, []byte(".headers off\n"), 0o644)
	mustOK(t, run(t, s, ".sqlrc import rc.sql"))
	if got, _ := s.StartupScript(ctx); got != ".headers off\n" {
		t.Fatalf("import: %q", got)
	}
	mustOK(t, run(t, s, ".sqlrc clear"))
	if got, _ := s.StartupScript(ctx); got != "" {
		t.Fatalf("clear: %q", got)
	}
	if b := run(t, s, ".sqlrc bogus"); b[0].Err == nil {
		t.Fatal("unknown subcommand must fail")
	}
}

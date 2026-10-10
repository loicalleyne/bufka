package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

// newTestRepl opens a REPL on an in-memory database with ~/.bufka pointed
// at a temp dir.
func newTestRepl(t *testing.T) sqlReplModel {
	t.Helper()
	dir := t.TempDir()
	setSQLDirs(t, dir)
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newSQLReplModel(context.Background(), Deps{DB: db}).resize(120, 40)
	t.Cleanup(m.close)
	return m
}

func replKey(s string) tea.KeyPressMsg {
	r := []rune(s)
	return tea.KeyPressMsg{Code: r[0], Text: s}
}

var (
	keyEnter      = tea.KeyPressMsg{Code: tea.KeyEnter}
	keyAltEnter   = tea.KeyPressMsg{Code: tea.KeyEnter, Mod: tea.ModAlt}
	keyShiftEnter = tea.KeyPressMsg{Code: tea.KeyEnter, Mod: tea.ModShift}
	keyCtrlEnter  = tea.KeyPressMsg{Code: tea.KeyEnter, Mod: tea.ModCtrl}
	keyCtrlC      = tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
	keyCtrlR      = tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl}
	keyTab        = tea.KeyPressMsg{Code: tea.KeyTab}
	keyUp         = tea.KeyPressMsg{Code: tea.KeyUp}
	keyDown       = tea.KeyPressMsg{Code: tea.KeyDown}
	keyEsc        = tea.KeyPressMsg{Code: tea.KeyEscape}
)

// replType types s into the REPL key by key.
func replType(m sqlReplModel, s string) sqlReplModel {
	for _, r := range s {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	return m
}

// drive delivers msg and runs every resulting command (synchronously,
// following batches) until none remain, skipping spinner ticks.
func drive(t *testing.T, m sqlReplModel, msg tea.Msg) sqlReplModel {
	t.Helper()
	var cmd tea.Cmd
	m, cmd = m.update(msg)
	return runCmds(t, m, cmd)
}

func runCmds(t *testing.T, m sqlReplModel, cmd tea.Cmd) sqlReplModel {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		if c == nil {
			continue
		}
		switch msg := c().(type) {
		case tea.BatchMsg:
			queue = append(queue, msg...)
		case sqlRunDoneMsg, sqlCompleteMsg:
			var next tea.Cmd
			m, next = m.update(msg)
			queue = append(queue, next)
		}
	}
	return m
}

func screen(m sqlReplModel) string { return ansi.Strip(strings.Join(m.lines, "\n")) }

func TestReplRunsCompleteStatementOnEnter(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT 41 + 1 AS answer;")
	m = drive(t, m, keyEnter)
	if m.input.Value() != "" {
		t.Fatalf("input not cleared: %q", m.input.Value())
	}
	if s := screen(m); !strings.Contains(s, "answer") || !strings.Contains(s, "42") {
		t.Fatalf("screen:\n%s", s)
	}
}

func TestReplEnterContinuesIncompleteInput(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT 1")
	m = drive(t, m, keyEnter)
	if m.input.Value() != "SELECT 1\n" {
		t.Fatalf("enter without ; must add a line, got %q", m.input.Value())
	}
	m = replType(m, "AS x;")
	m = drive(t, m, keyEnter)
	if s := screen(m); !strings.Contains(s, "│ x ") && !strings.Contains(s, "  x  ") {
		t.Fatalf("multi-line statement did not run:\n%s", s)
	}

	// A ; inside a string does not complete the statement.
	m = replType(m, "SELECT 'a;")
	m = drive(t, m, keyEnter)
	if !strings.Contains(m.input.Value(), "\n") {
		t.Fatalf("; inside a string must not run, got %q", m.input.Value())
	}
}

func TestReplNewlineKeys(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT 1;")
	m = drive(t, m, keyAltEnter)
	if m.input.Value() != "SELECT 1;\n" {
		t.Fatalf("alt+enter after ; must add a line, got %q", m.input.Value())
	}
	m = drive(t, m, keyShiftEnter)
	if m.input.Value() != "SELECT 1;\n\n" {
		t.Fatalf("shift+enter must add a line, got %q", m.input.Value())
	}
}

func TestReplCtrlEnterRunsWithoutSemicolon(t *testing.T) {
	m := newTestRepl(t)
	m.enhanced = true
	m = replType(m, "SELECT 7 AS seven")
	m = drive(t, m, keyCtrlEnter)
	s := screen(m)
	if !strings.Contains(s, "seven") || !strings.Contains(s, "SELECT 7 AS seven;") {
		t.Fatalf("ctrl+enter must run with a ; added:\n%s", s)
	}
}

func TestReplFooterMatchesTerminal(t *testing.T) {
	m := newTestRepl(t)
	if f := ansi.Strip(m.footer()); strings.Contains(f, "shift+enter") || !strings.Contains(f, "alt+enter") {
		t.Fatalf("fallback footer: %q", f)
	}
	m.enhanced = true
	if f := ansi.Strip(m.footer()); !strings.Contains(f, "shift+enter") || !strings.Contains(f, "ctrl+enter") {
		t.Fatalf("enhanced footer: %q", f)
	}

	// The root model remembers the startup report for a REPL opened later.
	root := New(context.Background(), Deps{})
	mod, _ := root.Update(tea.KeyboardEnhancementsMsg{Flags: 1})
	if !mod.(Model).keyboardEnhanced {
		t.Fatal("root did not keep keyboard enhancements")
	}
}

func TestReplHistory(t *testing.T) {
	m := newTestRepl(t)
	for _, q := range []string{"SELECT 1;", "SELECT 2;"} {
		m = replType(m, q)
		m = drive(t, m, keyEnter)
	}
	m = replType(m, "draft")
	m = drive(t, m, keyUp)
	if m.input.Value() != "SELECT 2;" {
		t.Fatalf("up: %q", m.input.Value())
	}
	m = drive(t, m, keyUp)
	if m.input.Value() != "SELECT 1;" {
		t.Fatalf("up up: %q", m.input.Value())
	}
	m = drive(t, m, keyDown)
	m = drive(t, m, keyDown)
	if m.input.Value() != "draft" {
		t.Fatalf("down back to the draft: %q", m.input.Value())
	}

	// ctrl+r search.
	m = drive(t, m, keyCtrlR)
	m = replType(m, "1;")
	if m.input.Value() != "SELECT 1;" {
		t.Fatalf("search: %q", m.input.Value())
	}
	m = drive(t, m, keyEnter)
	if m.searching {
		t.Fatal("enter must end the search")
	}

	// Persisted.
	data, err := os.ReadFile(filepath.Join(sqlStateDir(), "sql_history"))
	if err != nil || !strings.Contains(string(data), "SELECT 2;") {
		t.Fatalf("history file: %q %v", data, err)
	}
}

func TestReplTabCompletion(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "CREATE TABLE kafka_orders_v1(_kafka_offset BIGINT);")
	m = drive(t, m, keyEnter)
	m = replType(m, "SELECT * FROM kafka_ord")
	m = drive(t, m, keyTab)
	if got := m.input.Value(); got != "SELECT * FROM kafka_orders_v1" {
		t.Fatalf("tab: %q (suggestions %+v)", got, m.sugg)
	}

	m.input.Reset()
	m = replType(m, ".ta")
	m = drive(t, m, keyTab)
	if len(m.sugg) < 2 || m.input.Value() != ".ta" {
		t.Fatalf("expected a list for .ta: %q %+v", m.input.Value(), m.sugg)
	}
	if !m.capturingInput() {
		t.Fatal("esc must close the list, not leave")
	}
	m = drive(t, m, keyEnter)
	if v := m.input.Value(); v != ".tables" && v != ".tail" {
		t.Fatalf("accept: %q", v)
	}
}

func TestReplCtrlC(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT")
	m = drive(t, m, keyCtrlC)
	if m.input.Value() != "" {
		t.Fatal("ctrl+c must clear the input")
	}

	// While running: stop waiting, discard the late result.
	m.running, m.runGen = true, 5
	cancelled := false
	m.runCancel = func() { cancelled = true }
	m = drive(t, m, keyCtrlC)
	if !cancelled || !m.abandoned {
		t.Fatal("ctrl+c while running must cancel and abandon")
	}
	m = replType(m, "SELECT 1;")
	m, _ = m.update(keyEnter)
	if !strings.Contains(screen(m), "still running") {
		t.Fatal("new input must wait for the abandoned statement")
	}
	m, _ = m.update(sqlRunDoneMsg{gen: 5})
	if m.running || !strings.Contains(screen(m), "discarded") {
		t.Fatalf("late result:\n%s", screen(m))
	}

	// Twice on an empty input leaves.
	m.input.Reset()
	m, _ = m.update(keyCtrlC)
	if !m.quitArmed {
		t.Fatal("first ctrl+c arms")
	}
	_, cmd := m.update(keyCtrlC)
	if cmd == nil {
		t.Fatal("second ctrl+c leaves")
	}
	if _, ok := cmd().(sqlLeaveMsg); !ok {
		t.Fatal("expected sqlLeaveMsg")
	}
}

func TestReplBrowseAndConfirm(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT i, 'v' || i AS v FROM range(50) t(i);")
	m = drive(t, m, keyEnter)
	m = replType(m, ".browse")
	m = drive(t, m, keyEnter)
	if m.browse == nil {
		t.Fatalf("no browser:\n%s", screen(m))
	}
	if v := ansi.Strip(m.View()); !strings.Contains(v, "v49") && !strings.Contains(v, "50 rows") {
		t.Fatalf("browse view:\n%s", v)
	}
	m = drive(t, m, keyEnter)
	if !m.browse.showDetail {
		t.Fatal("enter shows the row")
	}
	m = drive(t, m, keyEsc)
	m = drive(t, m, keyEsc)
	if m.browse != nil {
		t.Fatal("esc leaves the browser")
	}

	m = replType(m, ".compact")
	m = drive(t, m, keyEnter)
	if m.confirm == nil {
		t.Fatalf("compact must ask:\n%s", screen(m))
	}
	m = drive(t, m, replKey("n"))
	if m.confirm != nil || !strings.Contains(screen(m), "cancelled") {
		t.Fatal("n cancels")
	}
}

func TestReplLegacySqlrcImportedIntoDB(t *testing.T) {
	dir := t.TempDir()
	setSQLDirs(t, dir)
	legacy := filepath.Join(sqlLegacyDir(), "sqlrc")
	if err := os.WriteFile(legacy, []byte(".mode csv\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sqlLegacyDir(), "sql_history"), []byte("SELECT 99;\x00\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	m := newSQLReplModel(context.Background(), Deps{DB: db}).resize(100, 30)
	defer m.close()
	m = runCmds(t, m, m.Init())
	m = replType(m, "SELECT 1 AS a;")
	m = drive(t, m, keyEnter)
	if !strings.Contains(screen(m), "a\n1") {
		t.Fatalf("sqlrc not applied:\n%s", screen(m))
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatal("the legacy sqlrc must be retired after import")
	}
	if script, _ := m.session.StartupScript(context.Background()); script != ".mode csv\n" {
		t.Fatalf("startup script in the database = %q", script)
	}
	if m.history.Len() == 0 || m.history.At(0) != "SELECT 99;" {
		t.Fatalf("legacy history not moved: %q", m.history.Entries())
	}
	if _, err := os.Stat(filepath.Join(sqlStateDir(), "sql_history")); err != nil {
		t.Fatal("history must now live in the state directory")
	}
}

func TestReplWithoutDatabase(t *testing.T) {
	setSQLDirs(t, t.TempDir())
	m := newSQLReplModel(context.Background(), Deps{}).resize(80, 20)
	if !strings.Contains(screen(m), "DUCKDB_PATH") {
		t.Fatalf("screen:\n%s", screen(m))
	}
	m = replType(m, "SELECT 1;")
	m = drive(t, m, keyEnter) // must not panic
}

func TestReplEscReturnsToMenuKeepingSession(t *testing.T) {
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	setSQLDirs(t, t.TempDir())

	var mod tea.Model = New(context.Background(), Deps{DB: db})
	mod, _ = mod.Update(menuSelectedMsg{target: menuTargetSQLRepl})
	root := mod.(Model)
	if root.page != pageSQLRepl || !root.sqlReplOpen {
		t.Fatal("repl not opened")
	}
	sess := root.sqlRepl.session
	mod, _ = mod.Update(keyEsc)
	if mod.(Model).page != pageMenu {
		t.Fatal("esc must return to the menu")
	}
	mod, _ = mod.Update(menuSelectedMsg{target: menuTargetSQLRepl})
	if mod.(Model).sqlRepl.session != sess {
		t.Fatal("the session must survive leaving the screen")
	}
	mod.(Model).Close()
}

func TestReplScrollsOutput(t *testing.T) {
	m := newTestRepl(t)
	m = replType(m, "SELECT * FROM range(30) t(i);")
	m = drive(t, m, keyEnter)
	if !m.output.AtBottom() {
		t.Fatal("new output scrolls to the bottom")
	}
	m = drive(t, m, tea.MouseWheelMsg{Button: tea.MouseWheelUp})
	if m.output.AtBottom() {
		t.Fatal("the wheel must scroll the output up")
	}
	m.output.GotoBottom()
	m = drive(t, m, tea.KeyPressMsg{Code: tea.KeyPgUp})
	if m.output.AtBottom() {
		t.Fatal("pgup must scroll the output up")
	}
	m.output.GotoBottom()
	m = drive(t, m, tea.KeyPressMsg{Code: tea.KeyUp, Mod: tea.ModShift})
	if m.output.AtBottom() {
		t.Fatal("shift+up must scroll the output up")
	}

	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mod, _ := New(context.Background(), Deps{DB: db}).Update(menuSelectedMsg{target: menuTargetSQLRepl})
	if mod.(Model).View().MouseMode != tea.MouseModeCellMotion {
		t.Fatal("the REPL screen must enable mouse wheel reporting")
	}
	mod.(Model).Close()
}

func TestReplDuckboxFillsAndFollowsWidth(t *testing.T) {
	m := newTestRepl(t).resize(200, 40)
	m = replType(m, "SELECT 1 AS id, repeat('x', 150) AS long_text;")
	m = drive(t, m, keyEnter)
	widest := func(m sqlReplModel) int {
		w := 0
		for _, l := range m.lines {
			if strings.ContainsAny(l, "│┌└├") {
				w = max(w, ansi.StringWidth(l))
			}
		}
		return w
	}
	if !strings.Contains(screen(m), strings.Repeat("x", 150)) {
		t.Fatalf("a 150-char string fits on a 200-wide screen and must not be cut:\n%s", screen(m))
	}
	m = m.resize(80, 40)
	if w := widest(m); w > 80 {
		t.Fatalf("after shrinking to 80 the table is %d wide", w)
	}
	m = m.resize(200, 40)
	if !strings.Contains(screen(m), strings.Repeat("x", 150)) {
		t.Fatal("growing the screen again must show the full string")
	}
}

func TestReplQuitCommandsLeave(t *testing.T) {
	for _, c := range []string{".q", ".quit", ".exit", ".Q"} {
		m := newTestRepl(t)
		m = replType(m, c)
		var cmd tea.Cmd
		m, cmd = m.update(keyEnter)
		left := false
		queue := []tea.Cmd{cmd}
		for len(queue) > 0 {
			c := queue[0]
			queue = queue[1:]
			if c == nil {
				continue
			}
			switch msg := c().(type) {
			case tea.BatchMsg:
				queue = append(queue, msg...)
			case sqlRunDoneMsg:
				var next tea.Cmd
				m, next = m.update(msg)
				queue = append(queue, next)
			case sqlLeaveMsg:
				left = true
			}
		}
		if !left {
			t.Errorf("%s did not leave the REPL:\n%s", c, screen(m))
		}
	}
}

// setSQLDirs points the REPL's state and legacy directories under dir.
func setSQLDirs(t *testing.T, dir string) {
	t.Helper()
	state, legacy := filepath.Join(dir, "state"), filepath.Join(dir, "legacy")
	_ = os.MkdirAll(legacy, 0o700)
	oldState, oldLegacy := sqlStateDir, sqlLegacyDir
	sqlStateDir = func() string { return state }
	sqlLegacyDir = func() string { return legacy }
	t.Cleanup(func() { sqlStateDir, sqlLegacyDir = oldState, oldLegacy })
}

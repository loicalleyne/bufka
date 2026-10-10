package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl"
)

// sqlScrollbackLines bounds the REPL's kept output.
const sqlScrollbackLines = 5000

// sqlInputMaxHeight bounds the input editor's height.
const sqlInputMaxHeight = 10

var (
	sqlPromptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("63")).Bold(true)
	sqlInfoStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	sqlSelStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(lipgloss.Color("63"))
)

// sqlStateDir is where the REPL keeps sql_history (XDG state directory);
// replaced in tests.
var sqlStateDir = func() string {
	d, err := config.StateDir()
	if err != nil {
		return ""
	}
	return d
}

// sqlLegacyDir is where older versions kept sqlrc and sql_history; they are
// moved once (history to sqlStateDir, sqlrc into the database). Replaced
// in tests.
var sqlLegacyDir = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".bufka")
}

// sqlHistoryPath returns the history file, first moving a legacy one.
func sqlHistoryPath() string {
	dir := sqlStateDir()
	if dir == "" {
		return ""
	}
	path := filepath.Join(dir, "sql_history")
	if legacy := sqlLegacyDir(); legacy != "" {
		old := filepath.Join(legacy, "sql_history")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if _, err := os.Stat(old); err == nil {
				if os.MkdirAll(dir, 0o700) == nil {
					_ = os.Rename(old, path)
				}
			}
		}
	}
	return path
}

// sqlReplModel is the sql > repl screen: a DuckDB-CLI-like REPL with a
// scrollback, a multi-line input, history, completion and dot-commands.
type sqlReplModel struct {
	deps    Deps
	session *sqlrepl.Session
	history *sqlrepl.History
	openErr error

	input  textarea.Model
	output viewport.Model
	// entries is the scrollback; lines is its rendered text.
	entries []sqlEntry
	lines   []string
	spin    spinner.Model

	width, height int

	// enhanced is true once the terminal confirmed keyboard enhancements,
	// so shift+enter and ctrl+enter are distinguishable.
	enhanced bool

	// running is true while a statement executes; runGen identifies the
	// run so a cancelled run's result is discarded.
	running   bool
	runGen    int
	runCancel context.CancelFunc
	runStart  time.Time
	// abandoned is true when ctrl+c gave up on a run that DuckDB is still
	// executing: new input waits until it really finishes.
	abandoned bool

	// History browsing: histPos is the entry shown (Len() = the draft).
	histPos int
	draft   string

	// ctrl+r search.
	searching bool
	search    string
	searchHit int

	// Completion popup.
	sugg       []sqlrepl.Suggestion
	suggSel    int
	suggGen    int
	suggOrigin string // input value the suggestions were computed for

	// confirm holds a pending yes/no question's block.
	confirm *sqlrepl.Block

	// browse shows the last result in a table while non-nil.
	browse *sqlBrowseModel

	// quitArmed is set by ctrl+c on an empty, idle input: a second ctrl+c
	// leaves the REPL.
	quitArmed bool
}

func newSQLReplModel(ctx context.Context, deps Deps) sqlReplModel {
	ta := textarea.New()
	ta.ShowLineNumbers = false
	ta.Prompt = ""
	ta.SetPromptFunc(4, func(pi textarea.PromptInfo) string {
		if pi.LineNumber == 0 {
			return sqlPromptStyle.Render("D ") + "  "
		}
		return sqlPromptStyle.Render("‣ ") + "  "
	})
	ta.Placeholder = "SQL ending in ; or a .command — .help lists them"
	ta.DynamicHeight = true
	ta.MaxHeight = sqlInputMaxHeight
	ta.SetHeight(1)
	// enter is handled by the REPL (run or newline); alt+enter always
	// inserts a newline.
	ta.KeyMap.InsertNewline.SetKeys("alt+enter")
	ta.Focus()

	m := sqlReplModel{
		deps:   deps,
		input:  ta,
		output: viewport.New(viewport.WithWidth(80), viewport.WithHeight(20)),
		spin:   spinner.New(spinner.WithSpinner(spinner.Dot)),
	}
	m.session, m.openErr = sqlrepl.NewSession(deps.DB, sqlDeps(ctx, deps))
	if path := sqlHistoryPath(); path != "" {
		m.history, _ = sqlrepl.LoadHistory(path)
	}
	if m.history == nil {
		m.history = &sqlrepl.History{Limit: sqlrepl.DefaultHistoryLimit}
	}
	m.histPos = m.history.Len()
	if m.openErr != nil {
		m.appendLines(errorStyle.Render(m.openErr.Error()))
	} else {
		m.appendLines(sqlInfoStyle.Render("DuckDB SQL on bufka's database. Statements end with ; — .help for commands, .topics for cached Kafka topics, esc to leave."))
	}
	return m
}

// sqlDeps connects the bufka dot-commands to the TUI's dependencies.
func sqlDeps(ctx context.Context, deps Deps) sqlrepl.Deps {
	d := sqlrepl.Deps{OpenURL: openBrowser}
	if deps.DB == nil {
		return d
	}
	d.Topics = func(ctx context.Context) ([]sqlrepl.TopicCache, error) {
		configs, err := database.ReadCacheConfigs(ctx, deps.DB)
		if err != nil {
			return nil, err
		}
		out := make([]sqlrepl.TopicCache, 0, len(configs))
		for _, c := range configs {
			table, err := kafkaingest.TableName(c.Topic)
			if err != nil {
				continue
			}
			tc := sqlrepl.TopicCache{Topic: c.Topic, Table: table}
			if deps.SyncManager != nil {
				tc.Active = deps.SyncManager.IsActive(c.Topic)
				tc.Err = deps.SyncManager.Error(c.Topic)
			}
			out = append(out, tc)
		}
		return out, nil
	}
	if deps.SyncManager != nil {
		// Syncs outlive the REPL: they run under the program's context.
		d.Sync = func(_ context.Context, topic string) error {
			return deps.SyncManager.Start(ctx, topic)
		}
	}
	d.StartUI = func(ctx context.Context) (string, error) {
		port := deps.DuckDBUI.Port
		if port == 0 {
			port = database.DefaultDuckDBUIPort
		}
		if err := database.StartDuckDBUI(ctx, deps.DB, port); err != nil {
			return "", err
		}
		return database.DuckDBUISettings{Enabled: true, Port: port}.URL(), nil
	}
	d.Compact = func(ctx context.Context) error { return database.CompactDB(ctx, deps.DB) }
	return d
}

// Init imports a legacy ~/.bufka/sqlrc into the database once, then runs
// the saved startup script (.sqlrc).
func (m sqlReplModel) Init() tea.Cmd {
	if m.session == nil {
		return nil
	}
	legacy := ""
	if dir := sqlLegacyDir(); dir != "" {
		legacy = filepath.Join(dir, "sqlrc")
	}
	return m.start(func(ctx context.Context, s *sqlrepl.Session) []sqlrepl.Block {
		var blocks []sqlrepl.Block
		if legacy != "" {
			if data, err := os.ReadFile(legacy); err == nil {
				if cur, err := s.StartupScript(ctx); err == nil && strings.TrimSpace(cur) == "" {
					if err := s.SetStartupScript(ctx, string(data)); err == nil {
						_ = os.Rename(legacy, legacy+".bak")
						blocks = append(blocks, sqlrepl.Block{Info: "imported " + legacy + " into the database (.sqlrc shows it); the file was renamed to sqlrc.bak"})
					}
				}
			}
		}
		return append(blocks, s.RunStartup(ctx)...)
	})
}

// close releases the session; the model is unusable afterwards.
func (m *sqlReplModel) close() {
	if m.runCancel != nil {
		m.runCancel()
	}
	if m.session != nil {
		_ = m.session.Close()
		m.session = nil
	}
}

func (m sqlReplModel) resize(w, h int) sqlReplModel {
	m.width, m.height = w, h
	widthChanged := m.output.Width() != w
	m.input.SetWidth(max(w, 10))
	if m.session != nil {
		m.session.SetScreenWidth(max(w-1, 20))
	}
	if widthChanged && len(m.entries) > 0 {
		atBottom := m.output.AtBottom()
		m.renderScrollback()
		if atBottom {
			m.output.GotoBottom()
		}
	}
	m.layout()
	if m.browse != nil {
		b := m.browse.resize(w, h)
		m.browse = &b
	}
	return m
}

// layout gives the scrollback whatever the input and footer leave.
func (m *sqlReplModel) layout() {
	extra := 2 // footer + spacer
	if len(m.sugg) > 0 {
		extra += min(len(m.sugg), 8) + 1
	}
	if m.searching || m.confirm != nil {
		extra++
	}
	h := max(m.height-m.input.Height()-extra, 3)
	atBottom := m.output.AtBottom()
	m.output.SetWidth(m.width)
	m.output.SetHeight(h)
	if atBottom {
		m.output.GotoBottom()
	}
}

func (m *sqlReplModel) appendLines(s string) {
	m.appendEntry(sqlEntry{text: strings.TrimRight(s, "\n")})
}

func (m *sqlReplModel) appendEntry(e sqlEntry) {
	m.entries = append(m.entries, e)
	m.renderScrollback()
	m.output.GotoBottom()
}

// clearScrollback empties the output.
func (m *sqlReplModel) clearScrollback() {
	m.entries, m.lines = nil, nil
	m.output.SetContent("")
}

// renderScrollback rebuilds the output text, re-rendering result tables
// that follow the screen width, and drops the oldest entries past
// sqlScrollbackLines.
func (m *sqlReplModel) renderScrollback() {
	var lines []string
	counts := make([]int, len(m.entries))
	for i, e := range m.entries {
		ls := strings.Split(e.render(m.tableWidth()), "\n")
		counts[i] = len(ls)
		lines = append(lines, ls...)
	}
	drop := 0
	for over := len(lines) - sqlScrollbackLines; over > 0 && drop < len(m.entries)-1; drop++ {
		over -= counts[drop]
		lines = lines[counts[drop]:]
	}
	m.entries = append([]sqlEntry(nil), m.entries[drop:]...)
	m.lines = lines
	m.output.SetContent(strings.Join(lines, "\n"))
}

// tableWidth is the width result tables fill.
func (m sqlReplModel) tableWidth() int { return max(m.width-1, 20) }

// sqlEntry is one scrollback piece: fixed text, or before/after text around
// a result table re-rendered at the current width.
type sqlEntry struct {
	text   string
	block  *sqlrepl.Block
	before string
	after  string
}

func (e sqlEntry) render(width int) string {
	if e.block == nil {
		return e.text
	}
	o := e.block.Format
	if e.block.FitScreen {
		o.MaxWidth = width
	}
	table, err := sqlrepl.RenderTable(e.block.Table, o)
	if err != nil {
		table = e.block.Text
	}
	parts := []string{}
	for _, p := range []string{e.before, strings.TrimRight(table, "\n"), e.after} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, "\n")
}

// capturingInput reports whether esc belongs to the REPL rather than
// leaving the screen.
func (m sqlReplModel) capturingInput() bool {
	return m.browse != nil || m.searching || len(m.sugg) > 0 || m.confirm != nil
}

// sqlRunDoneMsg carries the blocks of a finished run.
type sqlRunDoneMsg struct {
	gen    int
	blocks []sqlrepl.Block
}

// sqlCompleteMsg carries completion candidates.
type sqlCompleteMsg struct {
	gen    int
	origin string
	sugg   []sqlrepl.Suggestion
}

// start runs fn on the session in the background.
func (m *sqlReplModel) start(fn func(context.Context, *sqlrepl.Session) []sqlrepl.Block) tea.Cmd {
	ctx, cancel := context.WithCancel(context.Background())
	m.running, m.abandoned = true, false
	m.runGen++
	m.runCancel = cancel
	m.runStart = time.Now()
	gen, s := m.runGen, m.session
	return tea.Batch(m.spin.Tick, func() tea.Msg {
		defer cancel()
		return sqlRunDoneMsg{gen: gen, blocks: fn(ctx, s)}
	})
}

// submit runs the current input.
func (m sqlReplModel) submit() (sqlReplModel, tea.Cmd) {
	text := strings.TrimSpace(m.input.Value())
	if text == "" {
		return m, nil
	}
	if m.session == nil {
		m.appendLines(errorStyle.Render(m.openErr.Error()))
		return m, nil
	}
	if m.running {
		m.appendLines(sqlInfoStyle.Render("still running: wait, or press ctrl+c to stop waiting"))
		return m, nil
	}
	_ = m.history.Add(text)
	m.histPos = m.history.Len()
	m.draft = ""
	m.input.Reset()
	m.input.SetHeight(1)
	m.closeSugg()
	m.layout()
	return m, m.start(func(ctx context.Context, s *sqlrepl.Session) []sqlrepl.Block {
		return s.Execute(ctx, text)
	})
}

func (m sqlReplModel) update(msg tea.Msg) (sqlReplModel, tea.Cmd) {
	switch msg := msg.(type) {
	case sqlRunDoneMsg:
		if msg.gen != m.runGen {
			return m, nil
		}
		wasAbandoned := m.abandoned
		m.running, m.abandoned, m.runCancel = false, false, nil
		if wasAbandoned {
			m.appendLines(sqlInfoStyle.Render(fmt.Sprintf("the stopped statement finished after %s; its output was discarded", time.Since(m.runStart).Round(time.Millisecond))))
			return m, nil
		}
		return m.showBlocks(msg.blocks)

	case sqlCompleteMsg:
		if msg.gen != m.suggGen || msg.origin != m.input.Value() {
			return m, nil
		}
		return m.applyCompletion(msg.sugg), nil

	case spinner.TickMsg:
		if !m.running {
			return m, nil
		}
		var cmd tea.Cmd
		m.spin, cmd = m.spin.Update(msg)
		return m, cmd

	case tea.MouseWheelMsg:
		var cmd tea.Cmd
		m.output, cmd = m.output.Update(msg)
		return m, cmd

	case tea.PasteMsg:
		m.closeSugg()
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		m.layout()
		return m, cmd

	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m sqlReplModel) handleKey(key tea.KeyPressMsg) (sqlReplModel, tea.Cmd) {
	k := key.String()
	if k != "ctrl+c" {
		m.quitArmed = false
	}

	if m.browse != nil {
		if k == "esc" && !m.browse.showDetail {
			m.browse = nil
			return m, nil
		}
		b, cmd := m.browse.update(key)
		m.browse = &b
		return m, cmd
	}

	if m.confirm != nil {
		c := m.confirm
		m.confirm = nil
		m.layout()
		if k == "y" || k == "Y" {
			m.input.SetValue(c.Confirm)
			return m.submit()
		}
		m.appendLines(sqlInfoStyle.Render("cancelled"))
		return m, nil
	}

	if m.searching {
		return m.searchKey(key)
	}

	if len(m.sugg) > 0 {
		switch k {
		case "tab", "down":
			m.suggSel = (m.suggSel + 1) % len(m.sugg)
			return m, nil
		case "shift+tab", "up":
			m.suggSel = (m.suggSel - 1 + len(m.sugg)) % len(m.sugg)
			return m, nil
		case "enter":
			m.acceptSuggestion(m.sugg[m.suggSel])
			return m, nil
		case "esc":
			m.closeSugg()
			return m, nil
		}
		m.closeSugg()
	}

	switch k {
	case "ctrl+c":
		return m.ctrlC()
	case "enter":
		if sqlrepl.Complete(m.input.Value()) {
			return m.submit()
		}
		m.input.InsertString("\n")
		m.layout()
		return m, nil
	case "shift+enter":
		m.input.InsertString("\n")
		m.layout()
		return m, nil
	case "ctrl+enter":
		v := strings.TrimSpace(m.input.Value())
		if _, rest := sqlrepl.Split(v); strings.TrimSpace(rest) != "" && !strings.HasSuffix(v, ";") {
			m.input.SetValue(v + ";")
		}
		return m.submit()
	case "tab":
		return m, m.requestCompletion()
	case "ctrl+r":
		m.searching, m.search, m.searchHit = true, "", -1
		m.draft = m.input.Value()
		m.layout()
		return m, nil
	case "pgup", "ctrl+b":
		m.output.PageUp()
		return m, nil
	case "pgdown", "ctrl+f":
		m.output.PageDown()
		return m, nil
	case "shift+up", "ctrl+y":
		m.output.ScrollUp(1)
		return m, nil
	case "shift+down", "ctrl+e":
		m.output.ScrollDown(1)
		return m, nil
	case "ctrl+home":
		m.output.GotoTop()
		return m, nil
	case "ctrl+end":
		m.output.GotoBottom()
		return m, nil
	case "ctrl+l":
		m.clearScrollback()
		return m, nil
	case "up":
		if m.input.Line() == 0 && m.history.Len() > 0 {
			if m.histPos == m.history.Len() {
				m.draft = m.input.Value()
			}
			if m.histPos > 0 {
				m.histPos--
				m.setInput(m.history.At(m.histPos))
				m.input.MoveToBegin()
			}
			return m, nil
		}
	case "down":
		if m.input.Line() == m.input.LineCount()-1 && m.histPos < m.history.Len() {
			m.histPos++
			if m.histPos == m.history.Len() {
				m.setInput(m.draft)
			} else {
				m.setInput(m.history.At(m.histPos))
			}
			return m, nil
		}
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(key)
	m.layout()
	return m, cmd
}

func (m *sqlReplModel) setInput(s string) {
	m.input.SetValue(s)
	m.input.MoveToEnd()
	m.layout()
}

// ctrlC stops waiting for a run, clears the input, or (pressed twice on an
// empty input) asks to leave.
func (m sqlReplModel) ctrlC() (sqlReplModel, tea.Cmd) {
	switch {
	case m.running && !m.abandoned:
		if m.runCancel != nil {
			m.runCancel()
		}
		// DuckDB keeps executing (ADBC exposes no interrupt): stop waiting
		// for it, and refuse new input until it finishes.
		m.abandoned = true
		m.appendLines(errorStyle.Render("stopped waiting: DuckDB finishes the statement in the background, new input runs after it"))
		return m, nil
	case m.input.Value() != "":
		m.input.Reset()
		m.input.SetHeight(1)
		m.histPos = m.history.Len()
		m.layout()
		return m, nil
	case m.quitArmed:
		m.quitArmed = false
		return m, func() tea.Msg { return sqlLeaveMsg{} }
	default:
		m.quitArmed = true
		return m, nil
	}
}

// sqlLeaveMsg asks the root model to go back to the menu.
type sqlLeaveMsg struct{}

func (m sqlReplModel) searchKey(key tea.KeyPressMsg) (sqlReplModel, tea.Cmd) {
	switch k := key.String(); k {
	case "esc", "ctrl+c", "ctrl+g":
		m.searching = false
		m.setInput(m.draft)
		return m, nil
	case "enter", "right", "left", "tab":
		m.searching = false
		m.layout()
		return m, nil
	case "ctrl+r":
		from := m.searchHit
		if from < 0 {
			from = m.history.Len()
		}
		if i := m.history.Search(m.search, from); i >= 0 {
			m.searchHit = i
			m.setInput(m.history.At(i))
		}
		return m, nil
	case "backspace":
		if m.search != "" {
			r := []rune(m.search)
			m.search = string(r[:len(r)-1])
		}
	default:
		if key.Text == "" {
			return m, nil
		}
		m.search += key.Text
	}
	m.searchHit = m.history.Search(m.search, m.history.Len())
	if m.searchHit >= 0 {
		m.setInput(m.history.At(m.searchHit))
	}
	return m, nil
}

// cursorOffset is the input cursor's byte offset into Value().
func (m sqlReplModel) cursorOffset() int {
	lines := strings.Split(m.input.Value(), "\n")
	row := min(m.input.Line(), len(lines)-1)
	off := 0
	for i := 0; i < row; i++ {
		off += len(lines[i]) + 1
	}
	r := []rune(lines[row])
	col := min(m.input.Column(), len(r))
	return off + len(string(r[:col]))
}

func (m *sqlReplModel) requestCompletion() tea.Cmd {
	if m.session == nil {
		return nil
	}
	m.suggGen++
	gen, s, value, cur := m.suggGen, m.session, m.input.Value(), m.cursorOffset()
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return sqlCompleteMsg{gen: gen, origin: value, sugg: s.Complete(ctx, value, cur)}
	}
}

// applyCompletion inserts a unique candidate or the common prefix, and
// shows a list when several remain.
func (m sqlReplModel) applyCompletion(sugg []sqlrepl.Suggestion) sqlReplModel {
	if len(sugg) == 0 {
		return m
	}
	if len(sugg) == 1 {
		m.acceptSuggestion(sugg[0])
		return m
	}
	cur := m.cursorOffset()
	start := sugg[0].Start
	same := true
	for _, s := range sugg {
		if s.Start != start {
			same = false
		}
	}
	if same && start <= cur {
		typed := m.input.Value()[start:cur]
		if p := sqlrepl.CommonPrefix(sugg); len(p) > len(typed) && strings.HasPrefix(strings.ToLower(p), strings.ToLower(typed)) {
			m.replaceRange(start, cur, p)
		}
	}
	m.sugg, m.suggSel = sugg, 0
	m.suggOrigin = m.input.Value()
	m.layout()
	return m
}

func (m *sqlReplModel) acceptSuggestion(s sqlrepl.Suggestion) {
	cur := m.cursorOffset()
	start := min(s.Start, cur)
	m.replaceRange(start, cur, s.Text)
	m.closeSugg()
}

// replaceRange replaces value[start:end] with text and puts the cursor
// after it.
func (m *sqlReplModel) replaceRange(start, end int, text string) {
	v := m.input.Value()
	if start < 0 || end > len(v) || start > end {
		return
	}
	newVal := v[:start] + text
	after := v[end:]
	m.input.SetValue(newVal + after)
	// Put the cursor right after the inserted text.
	m.input.MoveToBegin()
	lines := strings.Split(newVal, "\n")
	for range len(lines) - 1 {
		m.input.CursorDown()
	}
	m.input.CursorStart()
	m.input.SetCursorColumn(len([]rune(lines[len(lines)-1])))
	m.layout()
}

func (m *sqlReplModel) closeSugg() {
	if len(m.sugg) > 0 {
		m.sugg = nil
		m.layout()
	}
}

// showBlocks prints run output and handles screen actions.
func (m sqlReplModel) showBlocks(blocks []sqlrepl.Block) (sqlReplModel, tea.Cmd) {
	var cmd tea.Cmd
	for _, b := range blocks {
		if b.Action == sqlrepl.ActionClear {
			m.clearScrollback()
			continue
		}
		var before, parts []string
		if b.Input != "" {
			before = append(before, echoInput(b.Input))
		}
		if b.Text != "" && b.Table == nil {
			before = append(before, strings.TrimRight(b.Text, "\n"))
		}
		if b.Err != nil {
			msg := b.Err.Error()
			if errors.Is(b.Err, context.Canceled) {
				msg = "cancelled"
			}
			parts = append(parts, errorStyle.Render(msg))
		}
		if b.Info != "" {
			parts = append(parts, sqlInfoStyle.Render(b.Info))
		}
		if b.Table != nil {
			blk := b
			m.appendEntry(sqlEntry{block: &blk, before: strings.Join(before, "\n"), after: strings.Join(parts, "\n")})
		} else if all := append(before, parts...); len(all) > 0 {
			m.appendLines(strings.Join(all, "\n"))
		}
		switch b.Action {
		case sqlrepl.ActionExit:
			cmd = func() tea.Msg { return sqlLeaveMsg{} }
		case sqlrepl.ActionBrowse:
			if m.session != nil && m.session.Last != nil {
				bm := newSQLBrowseModel(m.session.Last, m.session.LastSQL, m.width, m.height)
				m.browse = &bm
			}
		case sqlrepl.ActionConfirm:
			c := b
			m.confirm = &c
		}
	}
	m.layout()
	return m, cmd
}

// echoInput renders executed input like a terminal transcript.
func echoInput(s string) string {
	lines := strings.Split(highlightSQL(s), "\n")
	for i, l := range lines {
		p := sqlPromptStyle.Render("D ")
		if i > 0 {
			p = sqlPromptStyle.Render("‣ ")
		}
		lines[i] = p + l
	}
	return strings.Join(lines, "\n")
}

// footer lists the keys that work in this terminal.
func (m sqlReplModel) footer() string {
	run := "enter runs (when it ends with ;)"
	nl := "alt+enter new line"
	if m.enhanced {
		nl = "shift+enter / alt+enter new line · ctrl+enter run now"
	}
	parts := []string{run, nl, "↑/↓ history", "ctrl+r search", "tab complete", "wheel · pgup/pgdn · shift+↑/↓ scroll", "esc back"}
	if m.running {
		parts = append([]string{m.spin.View() + " running " + time.Since(m.runStart).Round(time.Second).String() + " · ctrl+c stop waiting"}, parts...)
	}
	if m.quitArmed {
		parts = []string{"press ctrl+c again to leave the REPL"}
	}
	return helpStyle.Width(m.width).MaxHeight(1).Render(strings.Join(parts, " · "))
}

func (m sqlReplModel) View() string {
	if m.browse != nil {
		return m.browse.View()
	}
	var b strings.Builder
	b.WriteString(m.output.View())
	b.WriteString("\n")
	b.WriteString(m.input.View())
	b.WriteString("\n")
	if len(m.sugg) > 0 {
		b.WriteString(m.suggView())
		b.WriteString("\n")
	}
	if m.searching {
		hit := ""
		if m.searchHit < 0 && m.search != "" {
			hit = " (no match)"
		}
		b.WriteString(menuKeyStyle.Render("(reverse-i-search) ") + m.search + "▏" + helpStyle.Render(hit+" · ctrl+r older · enter keep · esc cancel"))
		b.WriteString("\n")
	}
	if m.confirm != nil {
		b.WriteString(menuKeyStyle.Render(m.confirm.Text) + " [y/N]\n")
	}
	b.WriteString(m.footer())
	return b.String()
}

func (m sqlReplModel) suggView() string {
	const show = 8
	start := 0
	if m.suggSel >= show {
		start = m.suggSel - show + 1
	}
	end := min(start+show, len(m.sugg))
	var lines []string
	for i := start; i < end; i++ {
		s := m.sugg[i]
		label := fmt.Sprintf("%-32s %s", s.Text, sqlInfoStyle.Render(s.Kind))
		if i == m.suggSel {
			label = sqlSelStyle.Render(fmt.Sprintf("%-32s", s.Text)) + " " + sqlInfoStyle.Render(s.Kind)
		}
		lines = append(lines, "  "+label)
	}
	lines = append(lines, helpStyle.Render(fmt.Sprintf("  %d/%d · tab/↓ next · enter accept · esc close", m.suggSel+1, len(m.sugg))))
	return strings.Join(lines, "\n")
}

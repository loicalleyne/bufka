package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

func TestMenuNavigation(t *testing.T) {
	m := newMenuModel()
	if got := m.selected().target; got != menuTargetTopics {
		t.Fatalf("expected initial selection to be topics, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetGroups {
		t.Fatalf("expected consumer groups after moving down, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetKafkaCache {
		t.Fatalf("expected kafka cache after moving down, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetSQLRepl {
		t.Fatalf("expected sql repl, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetSchemaCatalog {
		t.Fatalf("expected schema catalog, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetTopicMapping {
		t.Fatalf("expected topic mapping, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetKafkaSettings {
		t.Fatalf("expected kafka connection, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetSchemaSources {
		t.Fatalf("expected schema sources, got %v", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetDuckDBUI {
		t.Fatalf("expected duckdb ui, got %v", got)
	}

	// Moving past the last item should not wrap or go out of bounds.
	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := m.selected().target; got != menuTargetDuckDBUI {
		t.Fatalf("expected to stay on duckdb ui, got %v", got)
	}

	// Moving up should walk back through the list.
	m, _ = m.update(tea.KeyPressMsg{Code: 'k', Text: "k"})
	if got := m.selected().target; got != menuTargetSchemaSources {
		t.Fatalf("expected schema sources after moving up, got %v", got)
	}
}

func TestMenuLeafCount(t *testing.T) {
	if got := newMenuModel().leafCount(); got != 9 {
		t.Fatalf("expected 9 leaves, got %d", got)
	}
}

func TestMenuSelectEmitsMsg(t *testing.T) {
	m := newMenuModel()
	_, cmd := m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatalf("expected a cmd from enter")
	}
	msg := cmd()
	sel, ok := msg.(menuSelectedMsg)
	if !ok {
		t.Fatalf("expected menuSelectedMsg, got %T", msg)
	}
	if sel.target != menuTargetTopics {
		t.Errorf("expected topics target, got %v", sel.target)
	}
}

func TestModelMenuSelectionRoutesPages(t *testing.T) {
	deps := Deps{}
	mod := New(nil, deps)
	mod.page = pageMenu

	newMod, _ := mod.Update(menuSelectedMsg{target: menuTargetKafkaSettings})
	m := newMod.(Model)
	if m.page != pageKafkaSettings {
		t.Fatalf("expected pageKafkaSettings, got %v", m.page)
	}

	m.page = pageMenu
	newMod, _ = m.Update(menuSelectedMsg{target: menuTargetSchemaCatalog})
	m = newMod.(Model)
	if m.page != pageProtoMessages {
		t.Fatalf("expected pageProtoMessages, got %v", m.page)
	}

	m.page = pageMenu
	newMod, _ = m.Update(menuSelectedMsg{target: menuTargetSchemaSources})
	m = newMod.(Model)
	if m.page != pageProtoSettings {
		t.Fatalf("expected pageProtoSettings, got %v", m.page)
	}

	m.page = pageMenu
	newMod, _ = m.Update(menuSelectedMsg{target: menuTargetDuckDBUI})
	m = newMod.(Model)
	if m.page != pageDuckDBUISettings {
		t.Fatalf("expected pageDuckDBUISettings, got %v", m.page)
	}

	m.page = pageMenu
	newMod, _ = m.Update(menuSelectedMsg{target: menuTargetTopicMapping})
	m = newMod.(Model)
	if m.page != pageTopicMapping {
		t.Fatalf("expected pageTopicMapping, got %v", m.page)
	}
	newMod, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m = newMod.(Model)
	if m.page != pageMenu {
		t.Fatalf("expected esc to return to menu, got %v", m.page)
	}

	m.page = pageMenu
	newMod, _ = m.Update(menuSelectedMsg{target: menuTargetTopics})
	m = newMod.(Model)
	if m.page != pageList {
		t.Fatalf("expected pageList, got %v", m.page)
	}
}

func TestModelListEscReturnsToMenu(t *testing.T) {
	deps := Deps{}
	mod := New(nil, deps)
	mod.page = pageList

	newMod, _ := mod.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m := newMod.(Model)
	if m.page != pageMenu {
		t.Fatalf("expected esc from topic list to return to pageMenu, got %v", m.page)
	}
}

func TestSplashWaitsForTimerAndTopics(t *testing.T) {
	mod := New(nil, Deps{})

	// Topics loading alone must not leave the splash.
	newMod, _ := mod.Update(topicsLoadedMsg{})
	m := newMod.(Model)
	if m.page != pageSplash {
		t.Fatalf("expected to stay on splash until the timer fires, got %v", m.page)
	}

	// Timer firing after topics loaded moves to the menu.
	newMod, _ = m.Update(splashDoneMsg{})
	m = newMod.(Model)
	if m.page != pageMenu {
		t.Fatalf("expected pageMenu once timer and topics are both ready, got %v", m.page)
	}

	// Timer firing before topics load must also keep the splash.
	mod = New(nil, Deps{})
	newMod, _ = mod.Update(splashDoneMsg{})
	if newMod.(Model).page != pageSplash {
		t.Fatalf("expected to stay on splash until topics load")
	}
}

// TestInitSchedulesSplashTimer guards against Init() dropping the splash
// timer, which would leave the splash stuck forever.
func TestInitSchedulesSplashTimer(t *testing.T) {
	old := splashMinDuration
	splashMinDuration = time.Millisecond
	defer func() { splashMinDuration = old }()

	cmd := New(context.Background(), Deps{}).Init()
	if cmd == nil {
		t.Fatal("Init returned nil")
	}
	batch, ok := cmd().(tea.BatchMsg)
	if !ok {
		t.Fatal("expected Init to return a batch")
	}

	found := make(chan struct{}, len(batch))
	for _, c := range batch {
		if c == nil {
			continue
		}
		go func(c tea.Cmd) {
			// Other commands (spinner, Kafka load, refresh tick) may block
			// or panic with empty Deps; only the splash timer matters here.
			defer func() { _ = recover() }()
			if _, ok := c().(splashDoneMsg); ok {
				found <- struct{}{}
			}
		}(c)
	}

	select {
	case <-found:
	case <-time.After(2 * time.Second):
		t.Fatal("Init() does not schedule splashDoneMsg; splash would never exit")
	}
}

func TestMenuItemsHaveTooltips(t *testing.T) {
	for _, s := range menuSections {
		for _, it := range s.items {
			if it.summary == "" {
				t.Errorf("%s > %s has no tooltip", s.heading, it.label)
			}
		}
	}
}

func TestMenuViewShowsTooltipAndHelp(t *testing.T) {
	m := newMenuModel().resize(160, 50)
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	v := ansi.Strip(m.View())
	if !strings.Contains(v, "kafka › consumer groups") || !strings.Contains(v, "## help") {
		t.Fatalf("expected tooltip for selected item and help section:\n%s", v)
	}
	for _, w := range []int{160, 80, 50} {
		m = m.resize(w, 60)
		for _, line := range strings.Split(m.View(), "\n") {
			if lw := lipgloss.Width(line); lw > w {
				t.Fatalf("width %d: line overflows (%d): %q", w, lw, ansi.Strip(line))
			}
		}
	}
	// Short terminals drop the help section before clipping the menu.
	m = m.resize(160, 24)
	if v := ansi.Strip(m.View()); strings.Contains(v, "## help") || !strings.Contains(v, "schema sources") {
		t.Fatalf("short terminal should keep the menu and drop help:\n%s", v)
	}
}

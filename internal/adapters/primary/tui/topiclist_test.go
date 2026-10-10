package tui

import (
	"context"
	"testing"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
)

func TestTopicListFilter(t *testing.T) {
	deps := Deps{}
	m := newTopicListModel(deps)
	m.all = []topicRow{
		{Topic: "alpha.events"},
		{Topic: "beta.events"},
		{Topic: "gamma.metrics"},
	}
	m.applyFilter()

	if got := len(m.table.Rows()); got != 3 {
		t.Fatalf("expected 3 initial rows, got %d", got)
	}

	// Press '/' to start filtering
	var cmd tea.Cmd
	m, cmd = m.update(context.Background(), tea.KeyPressMsg{Code: '/', Text: "/"})
	if !m.filterOn {
		t.Errorf("expected filterOn to be true after '/'")
	}

	// Type 'alpha'
	for _, ch := range "alpha" {
		m, cmd = m.update(context.Background(), tea.KeyPressMsg{Code: ch, Text: string(ch)})
	}

	if got := len(m.table.Rows()); got != 1 {
		t.Fatalf("expected 1 row matching 'alpha', got %d", got)
	}
	if m.table.Rows()[0][0] != "alpha.events" {
		t.Errorf("expected row to be alpha.events, got %s", m.table.Rows()[0][0])
	}

	// Press Enter to confirm filter and blur
	m, cmd = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.filterOn {
		t.Errorf("expected filterOn to be false after enter")
	}
	if got := len(m.table.Rows()); got != 1 {
		t.Errorf("expected table to retain 1 filtered row, got %d", got)
	}

	// Press Esc to clear filter
	m, cmd = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyEsc})
	if got := len(m.table.Rows()); got != 3 {
		t.Errorf("expected 3 rows after clearing filter with esc, got %d", got)
	}
	_ = cmd
}

func TestTopicListVisibleTopics(t *testing.T) {
	deps := Deps{}
	m := newTopicListModel(deps)
	m.all = []topicRow{
		{Topic: "topic.one"},
		{Topic: "topic.two"},
	}
	m.applyFilter()

	topics := m.visibleTopics()
	if len(topics) != 2 {
		t.Fatalf("expected 2 visible topics, got %d", len(topics))
	}
	if topics[0] != "topic.one" || topics[1] != "topic.two" {
		t.Errorf("unexpected visible topics: %v", topics)
	}
}

func TestModelUpdateListDoesNotInterceptKeysWhenFiltering(t *testing.T) {
	// If filter is active, typing 'q' or 'g' or 'enter' must not quit or switch views.
	deps := Deps{}
	mod := New(context.Background(), deps)
	mod.page = pageList
	mod.list.all = []topicRow{{Topic: "topic.q"}, {Topic: "topic.g"}}
	mod.list.applyFilter()

	// Activate filter
	mod.list.filterOn = true

	// Send 'q' key
	newMod, cmd := mod.Update(tea.KeyPressMsg{Code: 'q', Text: "q"})
	m := newMod.(Model)

	// Should NOT have quit, page should still be pageList
	if m.page != pageList {
		t.Errorf("expected to stay on pageList, got %v", m.page)
	}
	if cmd != nil {
		// tea.Quit produces a special message, but here it shouldn't quit
		msg := cmd()
		if _, ok := msg.(tea.QuitMsg); ok {
			t.Errorf("model unexpectedly quit when typing 'q' in filter")
		}
	}

	// Send 'g' key
	newMod, _ = m.Update(tea.KeyPressMsg{Code: 'g', Text: "g"})
	m = newMod.(Model)
	if m.page != pageList {
		t.Errorf("model navigated to groups when typing 'g' in filter")
	}
}

func TestTopicListBatchKeyWhenNotFiltering(t *testing.T) {
	deps := Deps{}
	m := newTopicListModel(deps)
	m.all = []topicRow{
		{Topic: "topic.one"},
	}
	m.applyFilter()

	// Press 'S' while NOT filtering
	m, cmd := m.update(context.Background(), tea.KeyPressMsg{Code: 'S', Text: "S"})
	if !m.batchRunning {
		t.Fatalf("expected batchRunning to be true after pressing 'S'")
	}
	if cmd == nil {
		t.Fatalf("expected non-nil cmd from startBatch")
	}
	// Cancel it
	m, _ = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyEsc})
}

// Suppress unused imports
var _ = table.Row{}

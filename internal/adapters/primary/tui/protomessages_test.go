package tui

import (
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
)

func protoMessagesTestDeps(t *testing.T) Deps {
	t.Helper()
	msgs := loadTestMessages(t, `syntax = "proto3";
package demo;
message Order { string id = 1; }
message Shipment { string id = 1; }
message Unused { string id = 1; }
`)
	db := schemaTestDB(t,
		database.TopicSchemaRow{Topic: "orders", MessageName: "demo.Order"},
		database.TopicSchemaRow{Topic: "shipments", MessageName: "demo.Shipment"},
	)
	return Deps{Messages: msgs, DB: db}
}

func TestProtoMessagesFilterByFQN(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)

	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	if !m.filtering() {
		t.Fatalf("expected filtering to be on after /")
	}
	for _, r := range "Order" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if len(m.rows) != 1 || m.rows[0].fqn != "demo.Order" {
		t.Fatalf("expected only demo.Order, got %v", m.rows)
	}
}

func TestProtoMessagesFilterByTopic(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)

	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, r := range "shipments" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if len(m.rows) != 1 || m.rows[0].fqn != "demo.Shipment" {
		t.Fatalf("expected only demo.Shipment, got %v", m.rows)
	}
}

func TestProtoMessagesFilterExcludesUnresolved(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)

	// demo.Unused has no matching topic, so filtering by any real topic
	// name must never surface it.
	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, r := range "orders" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	for _, r := range m.rows {
		if r.fqn == "demo.Unused" {
			t.Fatalf("demo.Unused should not match filter %q", m.filter.Value())
		}
	}
}

func TestProtoMessagesFilterEscClearsThenReturnsToMenu(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)
	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, r := range "Order" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.filtering() {
		t.Fatalf("expected filtering off after enter")
	}
	if m.filter.Value() != "Order" {
		t.Fatalf("expected filter value preserved after enter, got %q", m.filter.Value())
	}
	if len(m.rows) != 1 {
		t.Fatalf("expected filter to still apply after enter, got %v", m.rows)
	}

	// esc with a filter value set clears the filter instead of leaving the page.
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if m.filter.Value() != "" {
		t.Fatalf("expected esc to clear filter value, got %q", m.filter.Value())
	}
	if len(m.rows) != len(m.all) {
		t.Fatalf("expected all rows restored after clearing filter, got %d want %d", len(m.rows), len(m.all))
	}
}

func TestModelProtoMessagesEscGoesToMenuOnlyWhenNotFiltering(t *testing.T) {
	deps := protoMessagesTestDeps(t)
	mod := New(nil, deps)
	mod.page = pageProtoMessages
	mod.protoMessages, _ = mod.protoMessages.resize(80, 20)

	// With an active filter value, esc should clear it and stay on the page.
	mod.protoMessages.filter.SetValue("Order")
	mod.protoMessages.applyFilter()
	newMod, _ := mod.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m := newMod.(Model)
	if m.page != pageProtoMessages {
		t.Fatalf("expected to stay on pageProtoMessages while a filter is set, got %v", m.page)
	}

	// With no filter, esc should return to the menu.
	m.protoMessages.filter.SetValue("")
	m.protoMessages.applyFilter()
	newMod, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m = newMod.(Model)
	if m.page != pageMenu {
		t.Fatalf("expected esc with no filter to return to pageMenu, got %v", m.page)
	}
}

func TestProtoMessagesResolvedOnlyToggle(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)
	if len(m.all) != 3 {
		t.Fatalf("expected 3 total messages, got %d", len(m.all))
	}
	if len(m.rows) != 3 {
		t.Fatalf("expected all 3 rows shown before toggling, got %d", len(m.rows))
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	if !m.resolvedOnly {
		t.Fatalf("expected resolvedOnly to be on after pressing r")
	}
	if len(m.rows) != 2 {
		t.Fatalf("expected 2 resolved rows, got %d: %v", len(m.rows), m.rows)
	}
	for _, row := range m.rows {
		if len(row.topics) == 0 {
			t.Errorf("row %s has no resolved topics, should be hidden", row.fqn)
		}
	}

	// Toggling again restores the full list.
	m, _ = m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	if m.resolvedOnly {
		t.Fatalf("expected resolvedOnly to be off after toggling again")
	}
	if len(m.rows) != 3 {
		t.Fatalf("expected all 3 rows restored, got %d", len(m.rows))
	}
}

func TestProtoMessagesResolvedOnlyCombinesWithTextFilter(t *testing.T) {
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)

	m, _ = m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, r := range "demo" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if len(m.rows) != 2 {
		t.Fatalf("expected both resolved messages to still match the FQN filter, got %v", m.rows)
	}

	// Narrowing the text filter to the unresolved message, with
	// resolvedOnly still on, should yield no rows.
	for _, r := range ".Unused" {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if len(m.rows) != 0 {
		t.Fatalf("expected no rows for unresolved message under resolvedOnly, got %v", m.rows)
	}
}

func TestProtoMessagesResolvedOnlyIsSeparateFromTextFilterKey(t *testing.T) {
	// "r" must toggle resolvedOnly rather than being typed into the filter
	// text when the filter is not active.
	m := newProtoMessagesModel(protoMessagesTestDeps(t))
	m, _ = m.resize(80, 20)

	m, _ = m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	if m.filter.Value() != "" {
		t.Fatalf("expected r to not be typed into the filter, got %q", m.filter.Value())
	}
	if m.filtering() {
		t.Fatalf("expected r to not open the filter")
	}
	if !m.resolvedOnly {
		t.Fatalf("expected r to toggle resolvedOnly")
	}

	// But while the filter box is focused, "r" must be treated as ordinary
	// filter input, not the toggle.
	m.resolvedOnly = false
	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	m, _ = m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	if m.resolvedOnly {
		t.Fatalf("expected r while filtering to not toggle resolvedOnly")
	}
	if m.filter.Value() != "r" {
		t.Fatalf("expected r to be typed into the filter while filtering, got %q", m.filter.Value())
	}
}

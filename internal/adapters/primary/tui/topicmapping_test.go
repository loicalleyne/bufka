package tui

import (
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
)

func TestMappingStatus(t *testing.T) {
	cases := []struct {
		name string
		row  mappingRow
		want string
	}{
		{"warnings win", mappingRow{Warnings: []string{"a", "b"}, Review: true}, "warnings (2)"},
		{"review", mappingRow{Review: true, Schema: "x"}, "review"},
		{"no schema", mappingRow{Format: "protobuf"}, "no schema"},
		{"unknown format", mappingRow{}, "no schema"},
		{"not sampled", mappingRow{SpecOnly: true, HasSpec: true, Schema: "a.yaml"}, "not sampled"},
		{"ok msg", mappingRow{Schema: "pkg.Msg"}, "ok"},
		{"ok spec only file", mappingRow{HasSpec: true}, "ok"},
	}
	for _, c := range cases {
		if got := mappingStatus(c.row); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestMappingRows(t *testing.T) {
	schemas := map[string]database.TopicSchemaRow{
		"b": {Topic: "b", Format: "protobuf", MessageName: "pkg.B", ResolvedBy: "detected"},
		"c": {Topic: "c", Format: "json", SpecFile: "/x/orders.yaml", ResolvedBy: "asyncapi", Warnings: []string{"w"}},
		"d": {Topic: "d", Format: "protobuf"},
	}
	rows := mappingRows(schemas, []string{"a", "c"}, map[string]string{"a": "/s/a-spec.yaml", "c": "/x/orders.yaml"})
	if len(rows) != 4 {
		t.Fatalf("got %d rows", len(rows))
	}
	if r := rows[0]; r.Topic != "a" || r.Format != "json (spec)" || r.ResolvedBy != "asyncapi" || r.Schema != "a-spec.yaml" || mappingStatus(r) != "not sampled" {
		t.Errorf("spec-only row: %+v", r)
	}
	if r := rows[2]; r.Topic != "c" || r.Schema != "orders.yaml" || mappingStatus(r) != "warnings (1)" {
		t.Errorf("c row: %+v", r)
	}
	if mappingStatus(rows[3]) != "no schema" || mappingStatus(rows[1]) != "ok" {
		t.Errorf("statuses wrong: %+v", rows)
	}
}

func testMappingModel() topicMappingModel {
	m := newTopicMappingModel(Deps{})
	m = m.resize(120, 30)
	m.all = []mappingRow{
		{Topic: "a", Schema: "pkg.A", ResolvedBy: "override"},
		{Topic: "b", Format: "protobuf"},
		{Topic: "c", Schema: "pkg.C", Warnings: []string{"w"}, ResolvedBy: "detected"},
		{Topic: "d", HasSpec: true, SpecOnly: true, ResolvedBy: "asyncapi"},
	}
	m.applyFilter()
	return m
}

func TestMappingFiltersAndSort(t *testing.T) {
	m := testMappingModel()
	if len(m.table.Rows()) != 4 {
		t.Fatalf("want 4 rows")
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if got := len(m.table.Rows()); got != 2 { // b (no schema), c (warnings)
		t.Fatalf("problems: got %d", got)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if got := len(m.table.Rows()); got != 1 || m.table.Rows()[0][0] != "a" {
		t.Fatalf("ok: got %v", m.table.Rows())
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if len(m.table.Rows()) != 4 {
		t.Fatalf("all expected")
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 's', Text: "s"}) // status
	if got := m.table.Rows()[0][0]; got != "b" {           // "no schema" < "not sampled" < "ok" < "warnings"
		t.Errorf("status sort first = %s", got)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: 's', Text: "s"}) // resolved-by
	if got := m.table.Rows()[0][0]; got != "b" {           // empty resolved-by first
		t.Errorf("resolved sort first = %s", got)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	if got := m.table.Rows()[0][0]; got != "a" {
		t.Errorf("topic sort first = %s", got)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, ch := range "pkg.c" {
		m, _ = m.update(tea.KeyPressMsg{Code: ch, Text: string(ch)})
	}
	if rows := m.table.Rows(); len(rows) != 1 || rows[0][0] != "c" {
		t.Errorf("text filter: %v", rows)
	}
}

func TestMappingSummary(t *testing.T) {
	got := testMappingModel().summary()
	want := "4 topics · 1 ok · 1 warnings · 1 no schema · 1 not sampled"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestMappingEnterOpensTopic(t *testing.T) {
	m := testMappingModel()
	_, cmd := m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("expected cmd")
	}
	msg, ok := cmd().(openTopicMsg)
	if !ok || msg.topic != "a" || msg.from != pageTopicMapping {
		t.Fatalf("got %#v", msg)
	}
}

func TestModelTopicMappingFlow(t *testing.T) {
	mod := New(nil, Deps{})
	mod.page = pageTopicMapping
	mod.topicMapping = testMappingModel()
	nm, cmd := mod.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	mod = nm.(Model)
	nm, _ = mod.Update(cmd())
	mod = nm.(Model)
	if mod.page != pageDetail || mod.detail.topic != "a" {
		t.Fatalf("page=%v topic=%q", mod.page, mod.detail.topic)
	}
	nm, _ = mod.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	mod = nm.(Model)
	if mod.page != pageTopicMapping {
		t.Fatalf("esc from detail -> %v", mod.page)
	}
	nm, _ = mod.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if nm.(Model).page != pageMenu {
		t.Fatalf("esc should reach menu")
	}
}

func TestMappingNoDB(t *testing.T) {
	m := newTopicMappingModel(Deps{})
	if m.Init() != nil || m.View() == "" {
		t.Fatal("expected graceful no-DB view")
	}
}

func TestTopicMappingColumnsFitWidth(t *testing.T) {
	long := "com.example.platform.events.customer-profile-updated.v2"
	schema := "com.example.platform.events.CustomerProfileUpdatedEvent"
	m := newTopicMappingModel(Deps{})
	m.all = []mappingRow{{Topic: long, Schema: schema}}
	m.applyFilter()

	m = m.resize(200, 30)
	cols := m.table.Columns()
	if cols[0].Width < len(long) || cols[2].Width < len(schema) {
		t.Fatalf("wide terminal should show full names, got topic=%d schema=%d", cols[0].Width, cols[2].Width)
	}
	total := 0
	for _, c := range cols {
		total += c.Width + mappingCellPad
	}
	if total != 200 {
		t.Fatalf("columns should fill width 200, got %d", total)
	}

	m = m.resize(90, 30)
	cols = m.table.Columns()
	total = 0
	for _, c := range cols {
		total += c.Width + mappingCellPad
	}
	if total > 90 || cols[0].Width < mappingMinNameW || cols[2].Width < mappingMinNameW {
		t.Fatalf("narrow terminal should fit, got total=%d topic=%d schema=%d", total, cols[0].Width, cols[2].Width)
	}
}

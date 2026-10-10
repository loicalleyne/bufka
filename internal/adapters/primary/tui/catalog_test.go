package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/schemafield"
)

const testAsyncSpec = `asyncapi: 2.6.0
info:
  title: Billing Events
  version: 1.0.0
channels:
  invoices/created:
    description: Invoice topic
    publish:
      message:
        name: InvoiceCreated
        summary: An invoice was created
        payload:
          type: object
          required: [id]
          properties:
            id:
              type: string
              description: Invoice id
            note:
              type: string
              nullable: true
`

func loadTestAsyncAPI(t *testing.T) *asyncapi.Index {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "billing.yaml"), []byte(testAsyncSpec), 0o644); err != nil {
		t.Fatal(err)
	}
	ix, errs := asyncapi.Load([]string{dir})
	if len(errs) != 0 {
		t.Fatalf("load errors: %v", errs)
	}
	if len(ix.Topics()) == 0 {
		t.Fatalf("no topics loaded")
	}
	return ix
}

func catalogDeps(t *testing.T) Deps {
	d := protoMessagesTestDeps(t)
	d.AsyncAPI = loadTestAsyncAPI(t)
	return d
}

func TestCatalogUnifiedRows(t *testing.T) {
	m := newProtoMessagesModel(catalogDeps(t))
	m, _ = m.resize(120, 20)
	if len(m.all) != 4 {
		t.Fatalf("expected 3 proto + 1 asyncapi rows, got %d: %v", len(m.all), m.all)
	}
	var async *protoMessageRow
	for i := range m.all {
		if m.all[i].kind == catalogKindAsyncAPI {
			async = &m.all[i]
		}
	}
	if async == nil || async.name != "InvoiceCreated" || async.source != "billing.yaml" {
		t.Fatalf("unexpected asyncapi row: %+v", async)
	}
	if !strings.Contains(strings.Join(async.topics, ","), "invoices") {
		t.Errorf("topic missing: %v", async.topics)
	}
	for i := 1; i < len(m.all); i++ {
		if m.all[i-1].name > m.all[i].name {
			t.Errorf("rows not sorted by name: %q > %q", m.all[i-1].name, m.all[i].name)
		}
	}
	out := m.render()
	if !containsAll(out, "NAME", "KIND", "TOPIC(S)", "SOURCE FILE", "asyncapi", "proto", "billing.yaml", "test.proto") {
		t.Errorf("render missing columns/content:\n%s", out)
	}
}

func TestCatalogKindFilterCycles(t *testing.T) {
	m := newProtoMessagesModel(catalogDeps(t))
	m, _ = m.resize(120, 20)
	tab := tea.KeyPressMsg{Code: tea.KeyTab}

	m, _ = m.update(tab)
	if m.kindFilter != catalogKindProto || len(m.rows) != 3 {
		t.Fatalf("proto filter: %q %d rows", m.kindFilter, len(m.rows))
	}
	m, _ = m.update(tab)
	if m.kindFilter != catalogKindAsyncAPI || len(m.rows) != 1 || m.rows[0].kind != catalogKindAsyncAPI {
		t.Fatalf("asyncapi filter: %q %v", m.kindFilter, m.rows)
	}
	if !strings.Contains(m.View(), "[kind: asyncapi]") {
		t.Errorf("view should show active kind")
	}
	m, _ = m.update(tab)
	if m.kindFilter != "" || len(m.rows) != 4 {
		t.Fatalf("all filter: %q %d rows", m.kindFilter, len(m.rows))
	}
}

func TestCatalogAsyncOnlyEnterAndEscPreservesState(t *testing.T) {
	deps := Deps{AsyncAPI: loadTestAsyncAPI(t)}
	mod := New(nil, deps)
	mod.page = pageProtoMessages
	mod.protoMessages, _ = mod.protoMessages.resize(100, 20)
	mod.protoMessages.filter.SetValue("invoice")
	mod.protoMessages.applyFilter()

	_, cmd := mod.protoMessages.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	sel, ok := cmd().(asyncMessageSelectedMsg)
	if !ok || sel.topic != "invoices" && !strings.HasPrefix(sel.topic, "invoices") {
		t.Fatalf("expected asyncMessageSelectedMsg, got %+v", sel)
	}
	nm, _ := mod.Update(sel)
	m := nm.(Model)
	if m.page != pageAsyncDetail {
		t.Fatalf("expected pageAsyncDetail, got %v", m.page)
	}
	nm, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m = nm.(Model)
	if m.page != pageProtoMessages || m.protoMessages.filter.Value() != "invoice" || len(m.protoMessages.rows) != 1 {
		t.Fatalf("esc should restore list with filter: page=%v filter=%q", m.page, m.protoMessages.filter.Value())
	}
}

func TestAsyncDetailView(t *testing.T) {
	deps := Deps{AsyncAPI: loadTestAsyncAPI(t)}
	topic := deps.AsyncAPI.Topics()[0].Topic
	m := newAsyncDetailModel(deps, topic, 0)
	if m.err != "" {
		t.Fatal(m.err)
	}
	m = m.resize(100, 30)
	out := m.View()
	if !containsAll(out, "Billing Events", topic, "An invoice was created", "billing.yaml", "PATH", "id", "Invoice id", "note", "string?") {
		t.Errorf("detail missing content:\n%s", out)
	}
	if bad := newAsyncDetailModel(deps, "nope", 0); bad.err == "" {
		t.Errorf("expected error for unknown topic")
	}
}

func TestFieldTableRenderAndTruncate(t *testing.T) {
	long := strings.Repeat("word ", 40)
	ft := newFieldTable([]schemafield.FieldDef{
		{Path: "id", Type: "string", Required: true, Description: "Identifier"},
		{Path: "addr.city", Type: "string", Nullable: true, Description: long},
		{Path: "kind", Type: "enum", Enum: []string{"A", "B"}, Example: "A"},
	}).resize(60, 10)
	out := ft.View()
	if !containsAll(out, "PATH", "TYPE", "REQ", "DESCRIPTION", "*", "string?", "{A|B}", "e.g. A", "…") {
		t.Errorf("unexpected render:\n%s", out)
	}
	for _, l := range strings.Split(out, "\n") {
		if w := len([]rune(stripANSI(l))); w > 60 {
			t.Errorf("line wider than 60: %d %q", w, l)
		}
	}
	if !strings.Contains(out, "  city") {
		t.Errorf("expected indented nested path:\n%s", out)
	}
}

func TestFieldTableScrollBounds(t *testing.T) {
	var fs []schemafield.FieldDef
	for i := 0; i < 50; i++ {
		fs = append(fs, schemafield.FieldDef{Path: "f" + strings.Repeat("x", i%5), Type: "string"})
	}
	ft := newFieldTable(fs).resize(80, 8)
	if ft.Len() != 50 {
		t.Fatal("len")
	}
	ft, _ = ft.update(tea.KeyPressMsg{Code: tea.KeyUp})
	if ft.vp.YOffset() != 0 {
		t.Errorf("scrolled above top: %d", ft.vp.YOffset())
	}
	for i := 0; i < 20; i++ {
		ft, _ = ft.update(tea.KeyPressMsg{Code: tea.KeyPgDown})
	}
	if ft.vp.YOffset() != 50-7 {
		t.Errorf("expected offset %d at bottom, got %d", 50-7, ft.vp.YOffset())
	}
	ft = ft.resize(40, 12)
	if ft.vp.Height() != 11 {
		t.Errorf("resize height: %d", ft.vp.Height())
	}
	empty := newFieldTable(nil).resize(40, 5)
	if !strings.Contains(empty.View(), "no fields") {
		t.Errorf("empty table message missing")
	}
}

func TestProtoSchemaFieldsToggle(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, "syntax = \"proto3\";\npackage demo;\nmessage Ping { string id = 1; }\n")}
	m := newProtoSchemaModel(deps, "demo.Ping")
	m, _ = m.resize(80, 20)
	m, _ = m.update(tea.KeyPressMsg{Code: 'f', Text: "f"})
	if !m.showFields || !containsAll(m.View(), "PATH", "id", "string", "f show source") {
		t.Errorf("fields view wrong:\n%s", m.View())
	}
	m, _ = m.update(tea.KeyPressMsg{Code: 'f', Text: "f"})
	if m.showFields || !strings.Contains(m.View(), "f show fields") {
		t.Errorf("expected source view")
	}
}

func stripANSI(s string) string {
	var b strings.Builder
	in := false
	for _, r := range s {
		switch {
		case r == 0x1b:
			in = true
		case in && r == 'm':
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return b.String()
}

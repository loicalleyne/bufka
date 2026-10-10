package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
)

func schemaTabModel(t *testing.T, deps Deps, topic string) detailModel {
	t.Helper()
	m := newDetailModel(deps, topic)
	m, _ = m.resize(100, 30)
	m.tab = tabSchema
	m.enterTab()
	return m
}

func TestSchemaTabOrder(t *testing.T) {
	if tabSchema != tabMetadata+1 || tabCount != 6 {
		t.Fatalf("Schema tab must follow Metadata; tabCount=%d", tabCount)
	}
	tabs := renderTabs(tabMetadata)
	if !strings.Contains(tabs, "Schema") {
		t.Fatalf("tab bar lacks Schema: %q", tabs)
	}
	if !strings.Contains(detailHelp(tabSchema), "scroll") {
		t.Errorf("help lacks scroll hint")
	}
}

func TestSchemaTabAsyncAPIBeforeSampling(t *testing.T) {
	deps := Deps{AsyncAPI: loadTestAsyncAPI(t)}
	m := schemaTabModel(t, deps, "invoices")
	out := m.View()
	for _, want := range []string{"resolved by", "asyncapi", "json", "InvoiceCreated", "Billing Events", "An invoice was created", "PATH", "Invoice id"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestSchemaTabProtoComments(t *testing.T) {
	msgs := loadTestMessages(t, `syntax = "proto3";
package demo;
// Ping is a heartbeat.
message Ping {
  // the id
  string id = 1;
}
`)
	deps := Deps{Messages: msgs}
	m := newDetailModel(deps, "pings")
	m, _ = m.resize(100, 30)
	m.tab = tabSchema
	res := topicsample.TopicResult{Format: topicsample.FormatProtobuf, MessageName: "demo.Ping", ResolvedBy: topicsample.ResolvedByDetected, File: "test.proto"}
	m.lastResult = &res
	m.refreshSchema()
	out := m.View()
	for _, want := range []string{"detected", "protobuf", "demo.Ping", "test.proto", "Ping is a heartbeat", "the id"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestSchemaTabNothingKnown(t *testing.T) {
	m := schemaTabModel(t, Deps{}, "mystery")
	if !strings.Contains(m.View(), "No schema known for this topic.") {
		t.Fatalf("got:\n%s", m.View())
	}
}

func TestSchemaTabWarningsAndRefreshOnDetection(t *testing.T) {
	deps := Deps{AsyncAPI: loadTestAsyncAPI(t)}
	m := schemaTabModel(t, deps, "invoices")
	if strings.Contains(m.View(), "Warnings:") {
		t.Fatal("unexpected warnings")
	}
	res := topicsample.TopicResult{
		Format: topicsample.FormatJSON, ResolvedBy: topicsample.ResolvedByAsyncAPI,
		Spec:     &topicsample.SpecRef{File: "/x/billing.yaml", Message: "InvoiceCreated"},
		Warnings: []string{`required field "id" missing in 3/10 samples`},
	}
	m, _ = m.update(context.Background(), sampleActionDoneMsg{res: res, records: 10})
	out := m.View()
	if !strings.Contains(out, "Warnings:") || !strings.Contains(out, `required field "id" missing`) {
		t.Fatalf("warnings not shown after detection:\n%s", out)
	}
}

func TestSchemaTabFromDB(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "s.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ix := loadTestAsyncAPI(t)
	res := topicsample.TopicResult{
		Format: topicsample.FormatJSON, ResolvedBy: topicsample.ResolvedByAsyncAPI,
		Spec: &topicsample.SpecRef{File: "billing.yaml", Message: "InvoiceCreated"}, Warnings: []string{"stored warning"},
	}
	if err := database.SaveDetection(ctx, db, "invoices", "t", res, ix, nil); err != nil {
		t.Fatal(err)
	}
	m := schemaTabModel(t, Deps{DB: db}, "invoices")
	out := m.View()
	for _, want := range []string{"asyncapi", "stored warning", "Invoice id"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestSchemaTabSwitchAndScroll(t *testing.T) {
	deps := Deps{AsyncAPI: loadTestAsyncAPI(t)}
	m := newDetailModel(deps, "invoices")
	m, _ = m.resize(100, 30)
	m, _ = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyTab})
	if m.tab != tabSchema || !m.schema.loaded {
		t.Fatalf("tab key should reach Schema; tab=%v", m.tab)
	}
	if !strings.Contains(m.View(), "InvoiceCreated") {
		t.Fatalf("schema tab not rendered:\n%s", m.View())
	}
	m, _ = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyDown})
	m, _ = m.update(context.Background(), tea.KeyPressMsg{Code: tea.KeyPgDown})
	m, _ = m.update(context.Background(), tea.KeyPressMsg{Code: 'h', Text: "h"})
	if m.tab != tabMetadata {
		t.Fatalf("h should go back to Metadata, got %v", m.tab)
	}
}

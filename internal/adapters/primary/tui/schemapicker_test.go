package tui

import (
	"context"
	"os"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
)

const pickerProto = `syntax = "proto3";
package demo;
message Order { string id = 1; }
message Shipment { string id = 1; }
message Wide { string id = 1; int64 n = 2; }
`

func pickerFixture(t *testing.T) (Deps, []database.SampleRow) {
	t.Helper()
	msgs := loadTestMessages(t, pickerProto)
	info, _ := msgs.Get("demo.Order")
	msg := dynamicpb.NewMessage(info.Descriptor)
	if err := protojson.Unmarshal([]byte(`{"id":"o-1"}`), msg); err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	db := schemaTestDB(t, database.TopicSchemaRow{Topic: "orders", Format: "protobuf", MessageName: "demo.Order", SampleCount: 2, MatchedCount: 2})
	deps := Deps{Messages: msgs, DB: db}
	rows := []database.SampleRow{{Topic: "orders", Value: b}, {Topic: "orders", Offset: 1, Value: b}}
	return deps, rows
}

func loadedPicker(t *testing.T) (schemaPickerModel, Deps) {
	deps, rows := pickerFixture(t)
	m := newSchemaPickerModel(deps, "orders")
	m = m.resize(120, 30)
	values := [][]byte{rows[0].Value, rows[1].Value}
	m, _ = m.update(schemaPickerLoadedMsg{topic: "orders", rows: rows, candidates: topicsample.Rank("orders", values, deps.Messages)})
	return m, deps
}

func TestSchemaPickerListsMatchesAndStartsOnCurrent(t *testing.T) {
	m, _ := loadedPicker(t)
	var names []string
	for _, c := range m.candidates {
		names = append(names, c.Name)
	}
	if len(names) != 3 {
		t.Fatalf("expected Order, Shipment, Wide to all decode; got %v", names)
	}
	if c, _ := m.selected(); c.Name != "demo.Order" {
		t.Fatalf("cursor should start on current message, got %s", c.Name)
	}
	if !strings.Contains(m.View(), "o-1") {
		t.Fatalf("preview should show decoded sample:\n%s", m.View())
	}
}

func TestSchemaPickerApplySavesOverride(t *testing.T) {
	m, deps := loadedPicker(t)
	for m.cursor < len(m.candidates) && m.candidates[m.cursor].Name != "demo.Shipment" {
		m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	}
	_, cmd := m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("enter should return the apply command")
	}
	out, ok := cmd().(schemaOverriddenMsg)
	if !ok || out.err != nil || out.message != "demo.Shipment" {
		t.Fatalf("unexpected result %#v", out)
	}
	schemas, err := database.ReadTopicSchemas(context.Background(), deps.DB)
	if err != nil {
		t.Fatal(err)
	}
	if got := schemas["orders"]; got.MessageName != "demo.Shipment" || !got.MessageOverride || got.MatchedCount != 2 {
		t.Fatalf("override not saved to DuckDB: %+v", got)
	}
	if _, err := os.Stat("topics.toml"); err == nil {
		t.Fatal("a schema override must not create topics.toml")
	}
}

func TestModelSchemaPickerEscReturnsToOpener(t *testing.T) {
	deps, _ := pickerFixture(t)
	mod := New(nil, deps)
	mod.page = pageDetail
	mod.detail = newDetailModel(deps, "orders")
	nm, _ := mod.Update(tea.KeyPressMsg{Code: 'm', Text: "m"})
	m := nm.(Model)
	if m.page != pageSchemaPicker || m.schemaPicker.topic != "orders" {
		t.Fatalf("m should open picker, page=%v", m.page)
	}
	nm, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if nm.(Model).page != pageDetail {
		t.Fatalf("esc should return to detail")
	}
}

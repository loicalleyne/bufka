package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

func loadTestMessages(t *testing.T, src string) *protoinspect.OrderedMap[string, protoinspect.MessageInfo] {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "test.proto"), []byte(src), 0o644); err != nil {
		t.Fatalf("write proto: %v", err)
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{root})
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("unexpected failures: %v", failures)
	}
	return msgs
}

func TestProtoMessagesSelectEmitsSchemaMsg(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;
message Ping { string id = 1; }
message Other { string id = 1; }
`)}
	m := newProtoMessagesModel(deps)
	if len(m.rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(m.rows))
	}

	_, cmd := m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatalf("expected a cmd from enter")
	}
	msg := cmd()
	sel, ok := msg.(protoMessageSelectedMsg)
	if !ok {
		t.Fatalf("expected protoMessageSelectedMsg, got %T", msg)
	}
	if sel.fqn != "demo.Other" && sel.fqn != "demo.Ping" {
		t.Errorf("unexpected fqn %q", sel.fqn)
	}

	// Moving down then selecting should open the other message.
	m, _ = m.update(tea.KeyPressMsg{Code: 'j', Text: "j"})
	fqn, ok := m.selected()
	if !ok {
		t.Fatalf("expected a selection")
	}
	if fqn == sel.fqn {
		t.Errorf("expected cursor to move to a different message")
	}
}

func TestProtoSchemaIncludesCommentsAndSource(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;

// Ping is a health check message.
message Ping {
  string id = 1;
}
`)}
	m := newProtoSchemaModel(deps, "demo.Ping")
	if m.err != "" {
		t.Fatalf("unexpected error: %s", m.err)
	}
	if !containsAll(m.body, "Ping is a health check message", "message Ping {", "string id = 1;") {
		t.Errorf("schema body missing expected content: %s", m.body)
	}
}

func TestProtoSchemaUnknownMessage(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;
message Ping { string id = 1; }
`)}
	m := newProtoSchemaModel(deps, "demo.Missing")
	if m.err == "" {
		t.Fatalf("expected an error for an unknown message")
	}
}

func TestProtoSchemaIncludesReferencedMessageComments(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;

// Address is a shipping address.
message Address {
  string city = 1;
}

message Order {
  Address shipping = 1;
}
`)}
	m := newProtoSchemaModel(deps, "demo.Order")
	if m.err != "" {
		t.Fatalf("unexpected error: %s", m.err)
	}
	if !containsAll(m.body, "demo.Address", "Address is a shipping address", "message Address {") {
		t.Errorf("schema body missing referenced message's comments: %s", m.body)
	}
}

func TestProtoSchemaIncludesReferencedMessages(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;

message Address {
  string city = 1;
}

message Order {
  string id = 1;
  Address shipping = 2;
  map<string, Address> extra = 3;
}
`)}
	m := newProtoSchemaModel(deps, "demo.Order")
	if m.err != "" {
		t.Fatalf("unexpected error: %s", m.err)
	}
	if !containsAll(m.body, "Referenced messages", "demo.Address", "message Address {", "string city = 1;") {
		t.Errorf("schema body missing referenced message: %s", m.body)
	}
}

func TestProtoSchemaReferencedMessageNotLoaded(t *testing.T) {
	// deps.Messages only has Order; Address is a dependency of Order but is
	// not itself present in the OrderedMap, simulating an external type.
	src := `syntax = "proto3";
package demo;

message Address {
  string city = 1;
}

message Order {
  Address shipping = 1;
}
`
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "test.proto"), []byte(src), 0o644); err != nil {
		t.Fatalf("write proto: %v", err)
	}
	full, _, err := protoinspect.LoadMessages([]string{root})
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	order, _ := full.Get("demo.Order")
	partial := protoinspect.NewOrderedMap[string, protoinspect.MessageInfo]()
	partial.Set("demo.Order", order)

	m := newProtoSchemaModel(Deps{Messages: partial}, "demo.Order")
	if m.err != "" {
		t.Fatalf("unexpected error: %s", m.err)
	}
	if !containsAll(m.body, "demo.Address", "external type") {
		t.Errorf("expected external-type placeholder for demo.Address: %s", m.body)
	}
}

func TestReferencedMessageNamesExcludesSelfAndMapEntries(t *testing.T) {
	msgs := loadTestMessages(t, `syntax = "proto3";
package demo;

message Node {
  string id = 1;
  repeated Node children = 2;
  map<string, string> labels = 3;
}
`)
	info, ok := msgs.Get("demo.Node")
	if !ok {
		t.Fatalf("demo.Node not found")
	}
	refs := referencedMessageNames(info.Descriptor, "demo.Node")
	if len(refs) != 0 {
		t.Errorf("expected no references (self-reference and string map excluded), got %v", refs)
	}
}

func TestModelProtoMessageSelectedRoutesToSchema(t *testing.T) {
	deps := Deps{Messages: loadTestMessages(t, `syntax = "proto3";
package demo;
message Ping { string id = 1; }
`)}
	mod := New(nil, deps)
	mod.page = pageProtoMessages

	newMod, _ := mod.Update(protoMessageSelectedMsg{fqn: "demo.Ping"})
	m := newMod.(Model)
	if m.page != pageProtoSchema {
		t.Fatalf("expected pageProtoSchema, got %v", m.page)
	}
	if m.protoSchema.fqn != "demo.Ping" {
		t.Errorf("expected schema for demo.Ping, got %q", m.protoSchema.fqn)
	}

	newMod, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m = newMod.(Model)
	if m.page != pageProtoMessages {
		t.Fatalf("expected esc to return to pageProtoMessages, got %v", m.page)
	}
}

func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func typeText(m protoSettingsModel, s string) protoSettingsModel {
	for _, r := range s {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	return m
}

func TestProtoSettingsAddEditDelete(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{"/a"}})

	m, _ = m.update(tea.KeyPressMsg{Code: 'a', Text: "a"})
	if !m.capturingInput() {
		t.Fatal("expected input mode after a")
	}
	m = typeText(m, "/b")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if len(m.secs[0].paths) != 2 || m.secs[0].paths[1] != "/b" || !m.dirty() {
		t.Fatalf("add failed: %v dirty=%v", m.secs[0].paths, m.dirty())
	}

	// edit "/b" -> "/bc"
	m, _ = m.update(tea.KeyPressMsg{Code: 'e', Text: "e"})
	m = typeText(m, "c")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.secs[0].paths[1] != "/bc" {
		t.Fatalf("edit failed: %v", m.secs[0].paths)
	}

	// esc while editing cancels without leaving
	m, _ = m.update(tea.KeyPressMsg{Code: 'e', Text: "e"})
	m = typeText(m, "zzz")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if m.capturingInput() || m.secs[0].paths[1] != "/bc" {
		t.Fatalf("esc should cancel edit: %v", m.secs[0].paths)
	}

	m, _ = m.update(tea.KeyPressMsg{Code: 'd', Text: "d"})
	if len(m.secs[0].paths) != 1 || m.secs[0].paths[0] != "/a" {
		t.Fatalf("delete failed: %v", m.secs[0].paths)
	}
	if m.dirty() {
		t.Fatal("should not be dirty once back to applied paths")
	}
}

func TestProtoSettingsApplyRejectsMissingDir(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{})
	m, _ = m.update(tea.KeyPressMsg{Code: 'a', Text: "a"})
	m = typeText(m, "/definitely/not/here")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	m, cmd := m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	if cmd != nil || m.noteIsOK || m.loading {
		t.Fatalf("expected validation error, note=%q", m.note)
	}
}

func TestModelAppliesProtoPathsAndReloadsMessages(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "x.proto"), []byte("syntax = \"proto3\";\npackage demo;\nmessage Ping { string id = 1; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mod := New(nil, Deps{})
	mod.page = pageProtoSettings

	newMod, _ := mod.Update(tea.KeyPressMsg{Code: 'a', Text: "a"})
	mod = newMod.(Model)
	for _, r := range root {
		newMod, _ = mod.Update(tea.KeyPressMsg{Code: r, Text: string(r)})
		mod = newMod.(Model)
	}
	newMod, _ = mod.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	mod = newMod.(Model)
	newMod, cmd := mod.Update(tea.KeyPressMsg{Code: 's', Text: "s"})
	mod = newMod.(Model)
	if cmd == nil {
		t.Fatalf("expected apply cmd, note=%q", mod.protoSettings.note)
	}
	newMod, _ = mod.Update(cmd())
	mod = newMod.(Model)

	if mod.deps.Messages == nil || mod.deps.Messages.Len() != 1 {
		t.Fatalf("messages not reloaded: %v", mod.deps.Messages)
	}
	if len(mod.protoMessages.all) != 1 {
		t.Fatalf("protobuf messages page not refreshed")
	}
	if mod.protoSettings.dirty() {
		t.Fatal("should be clean after apply")
	}
}

func TestProtoSettingsEscInEditDoesNotLeavePage(t *testing.T) {
	mod := New(nil, Deps{})
	mod.page = pageProtoSettings
	newMod, _ := mod.Update(tea.KeyPressMsg{Code: 'a', Text: "a"})
	newMod, _ = newMod.(Model).Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if newMod.(Model).page != pageProtoSettings {
		t.Fatal("esc during edit must not leave the page")
	}
	newMod, _ = newMod.(Model).Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if newMod.(Model).page != pageMenu {
		t.Fatal("esc outside edit should return to menu")
	}
}

func TestApplyProtoPathsPersistsToDatabase(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "t.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "x.proto"), []byte("syntax = \"proto3\";\npackage demo;\nmessage Ping { string id = 1; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msg := applySchemaSourcesCmd(ctx, db, schemaPaths{proto: []string{root}}, true)().(schemaSourcesLoadedMsg)
	if msg.protoErr != nil || msg.saveErr != nil || !msg.saved {
		t.Fatalf("expected saved apply, got %+v", msg)
	}
	got, found, err := database.ReadProtoImportPaths(ctx, db)
	if err != nil || !found || len(got) != 1 || got[0] != root {
		t.Fatalf("not persisted: %v %v %v", got, found, err)
	}

	// Without a DB the apply still succeeds but reports it was not saved.
	msg = applySchemaSourcesCmd(ctx, nil, schemaPaths{proto: []string{root}}, true)().(schemaSourcesLoadedMsg)
	if msg.protoErr != nil || msg.saved || msg.saveErr != nil {
		t.Fatalf("expected session-only apply, got %+v", msg)
	}
}

func TestApplyEmptyPathsClearsSavedPaths(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "t.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Clearing all paths clears the saved list; startup treats an empty
	// list as "nothing saved" and falls back to PROTO_IMPORT_PATHS.
	msg := applySchemaSourcesCmd(ctx, db, schemaPaths{}, true)().(schemaSourcesLoadedMsg)
	if !msg.saved {
		t.Fatalf("expected empty list to be saved: %+v", msg)
	}
	got, found, _ := database.ReadProtoImportPaths(ctx, db)
	if !found || len(got) != 0 {
		t.Fatalf("expected found+empty, got %v found=%v", got, found)
	}
}

const testSpec = `asyncapi: 2.6.0
info: {title: t, version: "1"}
channels:
  orders:
    publish:
      message:
        name: Order
        payload:
          type: object
          properties:
            id: {type: string}
`

func key(m protoSettingsModel, k string) protoSettingsModel {
	m, _ = m.update(tea.KeyPressMsg{Code: rune(k[0]), Text: k})
	return m
}

func TestSchemaSourcesTabAndAsyncAddDelete(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{"/p"}})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if m.focus != sectionInclude {
		t.Fatal("tab should focus include")
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if m.focus != sectionAsync {
		t.Fatal("second tab should focus asyncapi")
	}
	m = key(m, "a")
	m = typeText(m, "/spec")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if len(m.secs[1].paths) != 1 || m.secs[1].paths[0] != "/spec" || len(m.secs[0].paths) != 1 {
		t.Fatalf("add went to wrong list: %v %v", m.secs[0].paths, m.secs[1].paths)
	}
	m = key(m, "d")
	if len(m.secs[1].paths) != 0 || m.dirty() {
		t.Fatalf("delete failed: %v", m.secs[1].paths)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	if m.focus != sectionProto {
		t.Fatal("tab should return to proto")
	}
}

func TestSchemaSourcesAsyncValidation(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{AsyncAPIPaths: []string{"/definitely/not/here"}})
	m.secs[1].applied = nil
	m, cmd := m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	if cmd != nil || m.noteIsOK || m.loading {
		t.Fatalf("expected validation error, note=%q", m.note)
	}
}

func TestSchemaSourcesCopy(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{"/a", "/b"}, AsyncAPIPaths: []string{"/b", "/c"}})
	m = key(m, "=")
	if got := m.secs[1].paths; len(got) != 3 || got[0] != "/b" || got[1] != "/c" || got[2] != "/a" {
		t.Fatalf("copy proto->async: %v", got)
	}
	m.focus = sectionAsync
	m = key(m, "=")
	if got := m.secs[0].paths; len(got) != 3 {
		t.Fatalf("copy async->proto should dedupe: %v", got)
	}
}

func TestSchemaSourcesReloadUpdatesDeps(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "orders.yaml"), []byte(testSpec), 0o644); err != nil {
		t.Fatal(err)
	}
	mod := New(nil, Deps{AsyncAPIPaths: []string{root}})
	mod.page = pageProtoSettings
	newMod, cmd := mod.Update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	mod = newMod.(Model)
	if cmd == nil {
		t.Fatalf("expected reload cmd, note=%q", mod.protoSettings.note)
	}
	newMod, _ = mod.Update(cmd())
	mod = newMod.(Model)
	if mod.deps.AsyncAPI == nil {
		t.Fatal("asyncapi index not pushed into deps")
	}
	if _, ok := mod.deps.AsyncAPI.Lookup("orders"); !ok {
		t.Fatalf("topic missing: %v", mod.deps.AsyncAPI.Topics())
	}
	if got := mod.protoSettings.results[1].status; got != "1 specs, 1 topics loaded" {
		t.Fatalf("status = %q", got)
	}
	if len(mod.protoMessages.all) == 0 {
		t.Fatal("catalog not refreshed with asyncapi entries")
	}
}

func TestSchemaSourcesBrokenSpecShowsError(t *testing.T) {
	root := t.TempDir()
	bad := filepath.Join(root, "bad.yaml")
	if err := os.WriteFile(bad, []byte("asyncapi: 2.6.0\nchannels: [unclosed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := newProtoSettingsModel(context.Background(), Deps{AsyncAPIPaths: []string{root}})
	m, cmd := m.update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	m, _ = m.update(cmd())
	if len(m.results[1].failures) == 0 {
		t.Fatalf("expected failure listed, got %+v", m.results[1])
	}
	if v := m.View(); !strings.Contains(v, "bad.yaml") {
		t.Fatalf("view should name the broken file:\n%s", v)
	}
}

func TestSchemaSourcesSaveBothLists(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "t.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	root := t.TempDir()
	msg := applySchemaSourcesCmd(ctx, db, schemaPaths{async: []string{root}}, true)().(schemaSourcesLoadedMsg)
	if !msg.saved || msg.saveErr != nil {
		t.Fatalf("%+v", msg)
	}
	got, found, err := database.ReadAsyncAPIPaths(ctx, db)
	if err != nil || !found || len(got) != 1 || got[0] != root {
		t.Fatalf("not persisted: %v %v %v", got, found, err)
	}
}

func TestSchemaSourcesArrowsCrossSections(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{"/p"}, AsyncAPIPaths: []string{"/a"}})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.focus != sectionInclude {
		t.Fatalf("down past proto list should enter the (empty) include list, focus=%d", m.focus)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.focus != sectionAsync || m.secs[sectionAsync].cursor != 0 {
		t.Fatalf("down past proto list should enter asyncapi list, focus=%d", m.focus)
	}
	m = key(m, "e")
	m = typeText(m, "x")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.secs[sectionAsync].paths[0] != "/ax" || m.secs[sectionProto].paths[0] != "/p" {
		t.Fatalf("edit went to wrong list: %v %v", m.secs[0].paths, m.secs[1].paths)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyUp})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.focus != sectionProto || m.secs[sectionProto].cursor != 0 {
		t.Fatalf("up past asyncapi list should return to proto list, focus=%d", m.focus)
	}
}

func TestSchemaSourcesDownIntoEmptyAsyncList(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{"/p"}})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.focus != sectionAsync || !strings.Contains(m.View(), "> (none configured)") {
		t.Fatalf("empty asyncapi list should be reachable and show the cursor, focus=%d", m.focus)
	}
}

// writeIncludeFixture makes a root whose message imports acme/common.proto,
// which only exists under a separate include directory.
func writeIncludeFixture(t *testing.T) (root, inc string) {
	t.Helper()
	root, inc = t.TempDir(), t.TempDir()
	_ = os.MkdirAll(filepath.Join(inc, "acme"), 0o755)
	if err := os.WriteFile(filepath.Join(inc, "acme", "common.proto"), []byte("syntax = \"proto3\";\npackage acme;\nmessage Common { string id = 1; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "order.proto"), []byte("syntax = \"proto3\";\npackage shop;\nimport \"acme/common.proto\";\nmessage Order { acme.Common c = 1; }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, inc
}

func TestSchemaSourcesIncludePathsResolveButAreNotListed(t *testing.T) {
	ctx := context.Background()
	root, inc := writeIncludeFixture(t)
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "i.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	msg := applySchemaSourcesCmd(ctx, db, schemaPaths{proto: []string{root}, include: []string{inc}}, true)().(schemaSourcesLoadedMsg)
	if msg.protoErr != nil || len(msg.protoFailed) > 0 || !msg.saved {
		t.Fatalf("%+v", msg)
	}
	if _, ok := msg.messages.Get("shop.Order"); !ok {
		t.Fatal("shop.Order should load via the include path")
	}
	if _, ok := msg.messages.Get("acme.Common"); ok {
		t.Fatal("include-path messages must not be listed")
	}
	got, found, err := database.ReadProtoIncludePaths(ctx, db)
	if err != nil || !found || len(got) != 1 || got[0] != inc {
		t.Fatalf("include paths not persisted: %v %v %v", got, found, err)
	}

	// Without the include path the import fails.
	msg = applySchemaSourcesCmd(ctx, nil, schemaPaths{proto: []string{root}}, false)().(schemaSourcesLoadedMsg)
	if len(msg.protoFailed) == 0 {
		t.Fatal("expected an unresolved import without the include path")
	}
}

func TestSchemaSourcesIncludeSectionEditing(t *testing.T) {
	m := newProtoSettingsModel(context.Background(), Deps{ProtoImportPaths: []string{t.TempDir()}, ProtoIncludePaths: []string{"/inc"}})
	if v := m.View(); !strings.Contains(v, "PROTO INCLUDE PATHS") || !strings.Contains(v, "/inc") {
		t.Fatalf("include section not rendered:\n%s", v)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab})
	m = key(m, "a")
	m = typeText(m, "/inc2")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if got := m.secs[sectionInclude].paths; len(got) != 2 || got[1] != "/inc2" || !m.dirty() {
		t.Fatalf("add to include: %v", got)
	}
	m = key(m, "=")
	if m.noteIsOK || len(m.secs[sectionAsync].paths) != 0 {
		t.Fatalf("= must not copy include paths, note=%q", m.note)
	}
	// Include paths are validated as directories on apply.
	m, cmd := m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	if cmd != nil || m.noteIsOK || !strings.Contains(m.note, "/inc") {
		t.Fatalf("expected directory validation error, note=%q", m.note)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift})
	if m.focus != sectionProto {
		t.Fatalf("shift+tab should go back to proto, focus=%d", m.focus)
	}
}

package tui

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func duiFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func duiServing(port int) bool {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get("http://localhost:" + strconv.Itoa(port) + "/")
	if err != nil {
		return false
	}
	_ = r.Body.Close()
	return true
}

func TestDuckDBUIWithoutDB(t *testing.T) {
	m := newDuckDBUISettingsModel(context.Background(), Deps{})
	m, cmd := m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	if cmd != nil {
		t.Fatal("nothing to apply without a database")
	}
	if v := ansi.Strip(m.View()); !strings.Contains(v, "needs a database file") {
		t.Fatalf("expected explanation, got:\n%s", v)
	}
	if m.menuSummary() != "" {
		t.Fatal("no menu status without a database")
	}
}

func TestDuckDBUIPortValidation(t *testing.T) {
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	m := newDuckDBUISettingsModel(context.Background(), Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Port: 4213}})
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyDown})
	m, _ = m.update(tea.KeyPressMsg{Code: 'e', Text: "e"})
	if !m.editing {
		t.Fatal("e on Port should edit")
	}
	m.input.SetValue("80")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if !m.editing || m.noteIsOK || m.staged.Port != 4213 {
		t.Fatalf("invalid port must be rejected and keep the editor open: %+v note=%q", m.staged, m.note)
	}
	m.input.SetValue("5555")
	m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.editing || m.staged.Port != 5555 || !m.dirty() {
		t.Fatalf("valid port should be staged: %+v", m.staged)
	}
	m, _ = m.update(tea.KeyPressMsg{Code: 'u', Text: "u"})
	if m.dirty() {
		t.Fatal("u should undo")
	}
}

func TestModelDuckDBUIApplyStartsStopsAndSaves(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = database.StopDuckDBUI(ctx, db)
		_ = db.Close()
	}()
	port := duiFreePort(t)

	mod := New(ctx, Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Port: database.DefaultDuckDBUIPort}})
	if mod.menu.duckDBUISummary != "(off)" {
		t.Fatalf("menu should show off, got %q", mod.menu.duckDBUISummary)
	}
	mod.page = pageDuckDBUISettings
	send := func(k tea.KeyPressMsg) tea.Cmd {
		n, cmd := mod.Update(k)
		mod = n.(Model)
		return cmd
	}
	send(tea.KeyPressMsg{Code: ' ', Text: " "}) // enable
	send(tea.KeyPressMsg{Code: tea.KeyDown})
	send(tea.KeyPressMsg{Code: 'e', Text: "e"})
	mod.duckDBUI.input.SetValue(strconv.Itoa(port))
	send(tea.KeyPressMsg{Code: tea.KeyEnter})
	cmd := send(tea.KeyPressMsg{Code: 's', Text: "s"})
	if cmd == nil {
		t.Fatalf("expected apply cmd, note=%q", mod.duckDBUI.note)
	}
	n, _ := mod.Update(cmd())
	mod = n.(Model)

	if !mod.deps.DuckDBUIRunning || !duiServing(port) {
		t.Fatalf("ui should be running on %d, note=%q", port, mod.duckDBUI.note)
	}
	if want := "(running :" + strconv.Itoa(port) + ")"; mod.menu.duckDBUISummary != want {
		t.Fatalf("menu summary %q, want %q", mod.menu.duckDBUISummary, want)
	}
	saved, found, err := database.ReadDuckDBUISettings(ctx, db)
	if err != nil || !found || !saved.Enabled || saved.Port != port {
		t.Fatalf("not saved: %+v found=%v err=%v", saved, found, err)
	}

	// Disable: server stops, "off" is saved.
	send(tea.KeyPressMsg{Code: tea.KeyUp})
	send(tea.KeyPressMsg{Code: ' ', Text: " "})
	cmd = send(tea.KeyPressMsg{Code: 's', Text: "s"})
	n, _ = mod.Update(cmd())
	mod = n.(Model)
	if mod.deps.DuckDBUIRunning || duiServing(port) {
		t.Fatal("ui should be stopped")
	}
	if saved, _, _ = database.ReadDuckDBUISettings(ctx, db); saved.Enabled {
		t.Fatal("disabled state should be saved")
	}

	// esc returns to the menu.
	send(tea.KeyPressMsg{Code: tea.KeyEsc})
	if mod.page != pageMenu {
		t.Fatal("esc should return to the menu")
	}
}

func TestDuckDBUIApplyPortInUseKeepsSettings(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	busy := l.Addr().(*net.TCPAddr).Port

	m := newDuckDBUISettingsModel(ctx, Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Port: busy}})
	m, _ = m.update(tea.KeyPressMsg{Code: ' ', Text: " "})
	m, cmd := m.update(tea.KeyPressMsg{Code: 's', Text: "s"})
	m, _ = m.update(cmd())
	if m.running || m.noteIsOK || !strings.Contains(m.note, "in use") || !m.dirty() {
		t.Fatalf("expected port-in-use error with edits kept, note=%q", m.note)
	}
	if _, found, _ := database.ReadDuckDBUISettings(ctx, db); found {
		t.Fatal("failed apply must not save")
	}
}

package tui

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestBrowserCommand(t *testing.T) {
	for goos, want := range map[string]string{"darwin": "open", "linux": "xdg-open", "windows": "rundll32"} {
		name, args, err := browserCommand(goos, "http://localhost:1/")
		if err != nil || name != want || args[len(args)-1] != "http://localhost:1/" {
			t.Errorf("%s: got %s %v %v", goos, name, args, err)
		}
	}
	if _, _, err := browserCommand("plan9", "x"); err == nil {
		t.Error("unsupported OS should error")
	}
}

// fakeBrowser records opened URLs instead of launching a browser.
func fakeBrowser(t *testing.T, err error) *[]string {
	var opened []string
	prev := openBrowser
	openBrowser = func(url string) error { opened = append(opened, url); return err }
	t.Cleanup(func() { openBrowser = prev })
	return &opened
}

func run(mod Model, msg tea.Msg) Model {
	for msg != nil {
		n, cmd := mod.Update(msg)
		mod = n.(Model)
		if cmd == nil {
			return mod
		}
		msg = cmd()
	}
	return mod
}

func TestDuckDBUIScreenOpensBrowser(t *testing.T) {
	opened := fakeBrowser(t, nil)
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Not running: no browser, explain why.
	m := newDuckDBUISettingsModel(context.Background(), Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Port: 4213}})
	m, cmd := m.update(tea.KeyPressMsg{Code: 'o', Text: "o"})
	if cmd != nil || len(*opened) != 0 || m.noteIsOK {
		t.Fatalf("should not open when stopped, note=%q", m.note)
	}

	m.running, m.applied = true, database.DuckDBUISettings{Enabled: true, Port: 4299}
	m, cmd = m.update(tea.KeyPressMsg{Code: 'o', Text: "o"})
	m, _ = m.update(cmd())
	if len(*opened) != 1 || (*opened)[0] != "http://localhost:4299/" || !m.noteIsOK {
		t.Fatalf("opened=%v note=%q", *opened, m.note)
	}
}

func TestMenuOpensDuckDBUI(t *testing.T) {
	opened := fakeBrowser(t, nil)
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mod := New(context.Background(), Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Enabled: true, Port: 4300}})
	mod.page = pageMenu
	for mod.menu.selected().target != menuTargetDuckDBUI {
		mod = run(mod, tea.KeyPressMsg{Code: tea.KeyDown})
	}

	// Stopped: a note, no browser.
	mod = run(mod, tea.KeyPressMsg{Code: 'o', Text: "o"})
	if len(*opened) != 0 || !strings.Contains(mod.menu.View(), "not running") {
		t.Fatalf("should explain the UI is stopped; opened=%v", *opened)
	}

	mod.deps.DuckDBUIRunning = true
	mod = run(mod, tea.KeyPressMsg{Code: 'o', Text: "o"})
	if len(*opened) != 1 || (*opened)[0] != "http://localhost:4300/" || !strings.Contains(mod.menu.View(), "opened http://localhost:4300/") {
		t.Fatalf("opened=%v note=%q", *opened, mod.menu.note)
	}
	if mod.page != pageMenu {
		t.Fatal("o must not leave the menu")
	}

	// o on another item does nothing.
	mod = run(mod, tea.KeyPressMsg{Code: tea.KeyUp})
	mod = run(mod, tea.KeyPressMsg{Code: 'o', Text: "o"})
	if len(*opened) != 1 {
		t.Fatal("o should only act on the duckdb ui item")
	}
}

func TestOpenBrowserFailureShowsURL(t *testing.T) {
	fakeBrowser(t, errors.New("no browser"))
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	m := newDuckDBUISettingsModel(context.Background(), Deps{DB: db, DuckDBUI: database.DuckDBUISettings{Enabled: true, Port: 4301}, DuckDBUIRunning: true})
	m, cmd := m.update(tea.KeyPressMsg{Code: 'o', Text: "o"})
	m, _ = m.update(cmd())
	if m.noteIsOK || !strings.Contains(m.note, "http://localhost:4301/ manually") {
		t.Fatalf("note=%q", m.note)
	}
}

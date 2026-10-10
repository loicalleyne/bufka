package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
)

const (
	duiFieldEnabled = iota
	duiFieldPort
	duiFieldCount
)

// duckDBUISettingsModel (settings > duckdb ui) turns DuckDB's built-in UI
// server on/off and sets its localhost port. Edits are staged and take effect
// with "s": the server is started, restarted on the new port, or stopped, and
// the settings are saved to the database. Without a database there is
// nothing for the UI to show, so the screen only explains that.
type duckDBUISettingsModel struct {
	ctx context.Context
	dep Deps

	staged  database.DuckDBUISettings
	applied database.DuckDBUISettings
	running bool
	cursor  int

	input   textinput.Model
	editing bool
	loading bool

	note     string
	noteIsOK bool
}

// duckDBUIAppliedMsg reports the outcome of applying settings.
type duckDBUIAppliedMsg struct {
	settings database.DuckDBUISettings
	running  bool
	err      error // starting/stopping failed; nothing was saved
	saveErr  error
}

func newDuckDBUISettingsModel(ctx context.Context, deps Deps) duckDBUISettingsModel {
	ti := textinput.New()
	ti.Prompt = "> "
	m := duckDBUISettingsModel{
		ctx:     ctx,
		dep:     deps,
		staged:  deps.DuckDBUI,
		applied: deps.DuckDBUI,
		running: deps.DuckDBUIRunning,
		input:   ti,
	}
	if deps.DuckDBUIErr != "" {
		m.note, m.noteIsOK = "not started at launch: "+deps.DuckDBUIErr, false
	}
	return m
}

func (m duckDBUISettingsModel) capturingInput() bool { return m.editing }

func (m duckDBUISettingsModel) dirty() bool { return m.staged != m.applied }

func (m duckDBUISettingsModel) update(msg tea.Msg) (duckDBUISettingsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case duckDBUIAppliedMsg:
		m.loading = false
		if msg.err != nil {
			m.running = msg.running
			m.note, m.noteIsOK = "not applied: "+msg.err.Error(), false
			return m, nil
		}
		m.applied, m.staged, m.running = msg.settings, msg.settings, msg.running
		m.note, m.noteIsOK = "stopped", true
		if msg.running {
			m.note = "running at " + msg.settings.URL()
		}
		if msg.saveErr != nil {
			m.note += "; NOT saved to database: " + msg.saveErr.Error()
			m.noteIsOK = false
		} else {
			m.note += "; saved"
		}
		return m, nil

	case browserOpenedMsg:
		if msg.err != nil {
			m.note, m.noteIsOK = msg.err.Error()+" — open "+msg.url+" manually", false
		} else {
			m.note, m.noteIsOK = "opened "+msg.url+" in your browser", true
		}
		return m, nil

	case tea.KeyPressMsg:
		if m.dep.DB == nil {
			return m, nil
		}
		if m.editing {
			switch msg.String() {
			case "esc":
				m.editing = false
				m.input.Blur()
				m.note = ""
				return m, nil
			case "enter":
				p, err := database.ValidateDuckDBUIPort(m.input.Value())
				if err != nil {
					m.note, m.noteIsOK = "port: "+err.Error(), false
					return m, nil
				}
				m.staged.Port = p
				m.editing = false
				m.input.Blur()
				m.note = ""
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}

		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < duiFieldCount-1 {
				m.cursor++
			}
		case "space", " ":
			if m.cursor == duiFieldEnabled {
				m.staged.Enabled = !m.staged.Enabled
				m.note = ""
			}
		case "e", "enter":
			if m.cursor == duiFieldEnabled {
				m.staged.Enabled = !m.staged.Enabled
				m.note = ""
				return m, nil
			}
			m.editing = true
			m.note = ""
			m.input.SetValue(strconv.Itoa(m.staged.Port))
			m.input.CursorEnd()
			return m, m.input.Focus()
		case "o":
			if !m.running {
				m.note, m.noteIsOK = "the DuckDB UI is not running: enable it and press s first", false
				return m, nil
			}
			return m, openBrowserCmd(m.applied.URL())
		case "u":
			m.staged = m.applied
			m.note = ""
		case "s":
			if m.loading {
				return m, nil
			}
			if !m.dirty() {
				m.note, m.noteIsOK = "no changes to apply", true
				return m, nil
			}
			m.loading = true
			m.note, m.noteIsOK = "applying...", true
			return m, applyDuckDBUICmd(ctxOrBackground(m.ctx), m.dep, m.staged, m.running)
		}
	}
	return m, nil
}

// applyDuckDBUICmd starts (or restarts on the new port) or stops the UI
// server, then saves the settings. Settings are only saved when the server
// reached the requested state.
func applyDuckDBUICmd(ctx context.Context, deps Deps, s database.DuckDBUISettings, wasRunning bool) tea.Cmd {
	return func() tea.Msg {
		out := duckDBUIAppliedMsg{settings: s}
		if s.Enabled {
			if err := database.StartDuckDBUI(ctx, deps.DB, s.Port); err != nil {
				// StartDuckDBUI stops any running server before starting.
				out.err = err
				return out
			}
			out.running = true
		} else {
			if err := database.StopDuckDBUI(ctx, deps.DB); err != nil {
				out.err, out.running = err, wasRunning
				return out
			}
		}
		out.saveErr = database.WriteDuckDBUISettings(ctx, deps.DB, s)
		return out
	}
}

// menuSummary is the state shown next to the menu item.
func (m duckDBUISettingsModel) menuSummary() string {
	if m.dep.DB == nil {
		return ""
	}
	if m.running {
		return fmt.Sprintf("(running :%d)", m.applied.Port)
	}
	return "(off)"
}

func (m duckDBUISettingsModel) View() string {
	if m.dep.DB == nil {
		return borderStyle.Render(
			"The DuckDB UI browses bufka's DuckDB database.\n\n"+
				helpStyle.Render("Not available: needs a database file (DUCKDB_PATH is :memory:)."),
		) + "\n" + helpStyle.Render("esc back")
	}
	onOff := func(b bool) string {
		if b {
			return "on"
		}
		return "off"
	}
	rows := []struct{ label, value string }{
		{"Enabled", "[" + map[bool]string{true: "x", false: " "}[m.staged.Enabled] + "] " + onOff(m.staged.Enabled)},
		{"Port", strconv.Itoa(m.staged.Port)},
	}
	var b strings.Builder
	hdr := "DUCKDB UI"
	if m.dirty() {
		hdr += "  (unapplied changes)"
	}
	b.WriteString(hdr + "\n\n")
	for i, r := range rows {
		cur := "  "
		if i == m.cursor && !m.editing {
			cur = "> "
		}
		b.WriteString(fmt.Sprintf("%s%-9s %s\n", cur, r.label, r.value))
	}
	b.WriteString("\n")
	if m.running {
		b.WriteString("  status    " + tailRunningStyle.Render("running") + " at " + m.applied.URL() + "\n")
	} else {
		b.WriteString("  status    " + helpStyle.Render("stopped") + "\n")
	}
	if m.editing {
		b.WriteString("\nport: " + m.input.View() + "\n")
	}
	out := borderStyle.Render(strings.TrimRight(b.String(), "\n")) + "\n"
	if m.note != "" {
		if m.noteIsOK {
			out += statusStyle.Render(m.note) + "\n"
		} else {
			out += errorStyle.Render(m.note) + "\n"
		}
	}
	out += helpStyle.Render("The UI listens on localhost only and stops when bufka tui exits. Settings are saved in the database;") + "\n"
	out += helpStyle.Render("until then DUCKDB_UI_ENABLED (default false) and the port in DUCKDB_UI_ADDR (default 4213) are used.") + "\n"
	if m.editing {
		return out + helpStyle.Render("enter save · esc cancel")
	}
	return out + helpStyle.Render("↑/↓ select · space/enter toggle · e/enter edit port · s apply & save · o open in browser · u undo · esc back (discards unapplied)")
}

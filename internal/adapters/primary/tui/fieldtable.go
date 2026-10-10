package tui

import (
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/loicalleyne/bufka/pkg/schemafield"
)

var (
	fieldTableHeaderStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("250"))
	fieldTablePathStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
	fieldTableTypeStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("81"))
	fieldTableReqStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	fieldTableDescStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
)

const fieldTableMaxIndent = 6

// fieldTable renders []schemafield.FieldDef as a scrollable table with the
// columns PATH, TYPE, REQ and DESCRIPTION. PATH shows the leaf name indented
// by path depth; TYPE gets a "?" suffix when nullable; REQ is "*" when
// required; DESCRIPTION is followed by compact enum/format/example hints and
// truncated to the available width. The header row stays pinned while the
// body scrolls (up/down/pgup/pgdn, j/k).
//
// Usage: t := newFieldTable(fields); t = t.resize(w, h); t, cmd = t.update(msg);
// t.View(). h is the total height including the header row.
type fieldTable struct {
	fields []schemafield.FieldDef
	vp     viewport.Model
	width  int
	height int
}

// newFieldTable builds a table for fields. Call resize before View.
func newFieldTable(fields []schemafield.FieldDef) fieldTable {
	return fieldTable{fields: fields}
}

// Len reports the number of fields shown.
func (t fieldTable) Len() int { return len(t.fields) }

// resize sets the total width and height (header row included).
func (t fieldTable) resize(w, h int) fieldTable {
	t.width, t.height = w, h
	vh := h - 1
	if vh < 1 {
		vh = 1
	}
	t.vp = resizedViewport(t.vp, w, vh)
	t.vp.SetContent(t.renderBody())
	return t
}

// update forwards scroll keys to the body viewport.
func (t fieldTable) update(msg tea.Msg) (fieldTable, tea.Cmd) {
	var cmd tea.Cmd
	t.vp, cmd = t.vp.Update(msg)
	return t, cmd
}

// View renders the pinned header followed by the visible body rows.
func (t fieldTable) View() string {
	if t.width <= 0 {
		return ""
	}
	return t.renderHeader() + "\n" + t.vp.View()
}

func (t fieldTable) columns() (pathW, typeW, reqW, descW int) {
	reqW = 3
	pathW = t.width * 3 / 10
	if pathW < 12 {
		pathW = 12
	}
	typeW = t.width * 2 / 10
	if typeW < 8 {
		typeW = 8
	}
	descW = t.width - pathW - typeW - reqW - 3
	if descW < 8 {
		descW = 8
	}
	return
}

func (t fieldTable) renderHeader() string {
	p, ty, r, d := t.columns()
	return fieldTableHeaderStyle.Render(cellPad("PATH", p) + " " + cellPad("TYPE", ty) + " " + cellPad("REQ", r) + " " + cellPad("DESCRIPTION", d))
}

func (t fieldTable) renderBody() string {
	if t.width <= 0 {
		return ""
	}
	if len(t.fields) == 0 {
		return helpStyle.Render("(no fields)")
	}
	p, ty, r, d := t.columns()
	lines := make([]string, 0, len(t.fields))
	for _, f := range t.fields {
		req := ""
		if f.Required {
			req = "*"
		}
		lines = append(lines, strings.Join([]string{
			fieldTablePathStyle.Render(cellPad(fieldPathLabel(f.Path), p)),
			fieldTableTypeStyle.Render(cellPad(fieldTypeLabel(f), ty)),
			fieldTableReqStyle.Render(cellPad(req, r)),
			fieldTableDescStyle.Render(cellPad(fieldDescLabel(f), d)),
		}, " "))
	}
	return strings.Join(lines, "\n")
}

// fieldPathLabel returns the leaf of a dotted path indented by its depth.
func fieldPathLabel(path string) string {
	depth := strings.Count(path, ".")
	leaf := path
	if i := strings.LastIndex(path, "."); i >= 0 {
		leaf = path[i+1:]
	}
	if depth > fieldTableMaxIndent {
		depth = fieldTableMaxIndent
	}
	return strings.Repeat("  ", depth) + leaf
}

func fieldTypeLabel(f schemafield.FieldDef) string {
	if f.Nullable {
		return f.Type + "?"
	}
	return f.Type
}

// fieldDescLabel is the one-line description plus enum/format/example hints.
func fieldDescLabel(f schemafield.FieldDef) string {
	parts := []string{}
	if d := strings.Join(strings.Fields(f.Description), " "); d != "" {
		parts = append(parts, d)
	}
	if len(f.Enum) > 0 {
		vals := f.Enum
		suffix := ""
		if len(vals) > 4 {
			vals, suffix = vals[:4], "|…"
		}
		parts = append(parts, "{"+strings.Join(vals, "|")+suffix+"}")
	}
	if f.Format != "" {
		parts = append(parts, "("+f.Format+")")
	}
	if f.Example != "" {
		parts = append(parts, "e.g. "+strings.Join(strings.Fields(f.Example), " "))
	}
	return strings.Join(parts, " ")
}

// cellTruncate cuts s to at most w display cells, ending with "…" if cut.
func cellTruncate(s string, w int) string {
	if w <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	var b strings.Builder
	used := 0
	for _, r := range s {
		rw := lipgloss.Width(string(r))
		if used+rw > w-1 {
			break
		}
		b.WriteRune(r)
		used += rw
	}
	b.WriteString("…")
	return b.String()
}

// cellPad truncates s to w cells and right-pads it with spaces.
func cellPad(s string, w int) string {
	s = cellTruncate(s, w)
	if pad := w - lipgloss.Width(s); pad > 0 {
		s += strings.Repeat(" ", pad)
	}
	return s
}

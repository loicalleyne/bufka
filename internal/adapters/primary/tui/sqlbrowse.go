package tui

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
)

// sqlBrowseModel shows a query result in a scrollable table (.browse);
// enter shows the selected row as column = value lines.
type sqlBrowseModel struct {
	result        *format.Table
	query         string
	table         table.Model
	width, height int
	// detail is the selected row's full text while open.
	detail     string
	showDetail bool
	// colOffset is the first column shown (left/right scroll).
	colOffset int
}

// sqlBrowseMaxCell caps a column's width in the browser.
const sqlBrowseMaxCell = 40

func newSQLBrowseModel(result *format.Table, query string, w, h int) sqlBrowseModel {
	m := sqlBrowseModel{result: result, query: query}
	m.table = table.New(table.WithFocused(true))
	return m.resize(w, h)
}

func (m sqlBrowseModel) resize(w, h int) sqlBrowseModel {
	m.width, m.height = w, h
	m.rebuild()
	return m
}

// rebuild lays out the columns starting at colOffset that fit the width.
func (m *sqlBrowseModel) rebuild() {
	t := m.result
	var cols []table.Column
	used := 0
	idx := []int{}
	for i := m.colOffset; i < len(t.Columns); i++ {
		c := t.Columns[i]
		wd := ansi.StringWidth(c.Name)
		for _, row := range t.Head {
			wd = max(wd, ansi.StringWidth(cellOneLine(row[i])))
			if wd >= sqlBrowseMaxCell {
				wd = sqlBrowseMaxCell
				break
			}
		}
		wd = max(min(wd, sqlBrowseMaxCell), 3)
		if used+wd+2 > m.width && len(cols) > 0 {
			break
		}
		cols = append(cols, table.Column{Title: c.Name, Width: wd})
		idx = append(idx, i)
		used += wd + 2
	}
	rows := make([]table.Row, len(t.Head))
	for r, row := range t.Head {
		vals := make(table.Row, len(idx))
		for j, i := range idx {
			vals[j] = cellOneLine(row[i])
		}
		rows[r] = vals
	}
	cursor := m.table.Cursor()
	m.table.SetRows(nil)
	m.table.SetColumns(cols)
	m.table.SetRows(rows)
	m.table.SetWidth(m.width)
	m.table.SetHeight(max(m.height-3, 3))
	if cursor < len(rows) {
		m.table.SetCursor(cursor)
	}
}

func cellOneLine(c format.Cell) string {
	if c.Null {
		return "NULL"
	}
	return strings.NewReplacer("\n", "↵", "\t", " ").Replace(c.Text)
}

func (m sqlBrowseModel) update(msg tea.Msg) (sqlBrowseModel, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok {
		if m.showDetail {
			switch key.String() {
			case "enter", "esc", "q":
				m.showDetail = false
			}
			return m, nil
		}
		switch key.String() {
		case "enter":
			if r := m.table.Cursor(); r >= 0 && r < len(m.result.Head) {
				m.detail = m.rowDetail(r)
				m.showDetail = true
			}
			return m, nil
		case "right", "l":
			if m.colOffset < len(m.result.Columns)-1 {
				m.colOffset++
				m.rebuild()
			}
			return m, nil
		case "left", "h":
			if m.colOffset > 0 {
				m.colOffset--
				m.rebuild()
			}
			return m, nil
		}
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m sqlBrowseModel) rowDetail(r int) string {
	nameW := 0
	for _, c := range m.result.Columns {
		nameW = max(nameW, ansi.StringWidth(c.Name))
	}
	var b strings.Builder
	for i, c := range m.result.Columns {
		v := m.result.Head[r][i]
		text := v.Text
		if v.Null {
			text = "NULL"
		}
		fmt.Fprintf(&b, "%*s  %s\n", nameW, menuKeyStyle.Render(c.Name), text)
	}
	return b.String()
}

func (m sqlBrowseModel) View() string {
	if m.showDetail {
		return m.detail + "\n" + helpStyle.Render("enter/esc back to the table")
	}
	info := fmt.Sprintf("%d rows · columns %d–%d of %d", len(m.result.Head),
		m.colOffset+1, m.colOffset+len(m.table.Columns()), len(m.result.Columns))
	if m.result.Total > int64(len(m.result.Head)) {
		info = fmt.Sprintf("first %d of %d rows · columns %d–%d of %d", len(m.result.Head), m.result.Total,
			m.colOffset+1, m.colOffset+len(m.table.Columns()), len(m.result.Columns))
	}
	return m.table.View() + "\n" + helpStyle.Render(info+" · ↑/↓ rows · ←/→ columns · enter row detail · esc back to the REPL")
}

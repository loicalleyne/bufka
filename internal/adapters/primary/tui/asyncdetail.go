package tui

import (
	"path/filepath"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/loicalleyne/bufka/pkg/asyncapi"
)

// asyncDetailModel shows one AsyncAPI message: a header with the spec/topic
// metadata followed by its field table.
type asyncDetailModel struct {
	spec   asyncapi.TopicSpec
	msg    asyncapi.MessageSpec
	table  fieldTable
	width  int
	height int
	err    string
}

var asyncDetailLabelStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("63"))

// newAsyncDetailModel looks up message index msgIdx of topic in deps.AsyncAPI.
func newAsyncDetailModel(deps Deps, topic string, msgIdx int) asyncDetailModel {
	m := asyncDetailModel{}
	if deps.AsyncAPI == nil {
		m.err = "no AsyncAPI specs loaded"
		return m
	}
	spec, ok := deps.AsyncAPI.Lookup(topic)
	if !ok {
		m.err = "topic " + topic + " not found in AsyncAPI specs"
		return m
	}
	m.spec = spec
	if msgIdx >= 0 && msgIdx < len(spec.Messages) {
		m.msg = spec.Messages[msgIdx]
	}
	m.table = newFieldTable(m.msg.Fields)
	return m
}

// headerLines renders the metadata block, wrapped to width.
func (m asyncDetailModel) headerLines(width int) []string {
	type kv struct{ k, v string }
	rows := []kv{
		{"Spec", m.spec.SpecTitle},
		{"Topic", m.spec.Topic},
		{"Message", m.msg.Name},
		{"Event type", m.msg.EventType},
		{"Summary", m.msg.Summary},
		{"Description", m.msg.Description},
		{"Topic summary", m.spec.Summary},
		{"Topic description", m.spec.Description},
		{"Cleanup policy", m.spec.CleanupPolicy},
		{"Key", m.msg.KeyDescription},
		{"Spec file", m.spec.SpecFile},
	}
	pairs := make([][2]string, len(rows))
	for i, r := range rows {
		pairs[i] = [2]string{r.k, r.v}
	}
	return kvHeaderLines(pairs, width, 0)
}

// kvHeaderLines renders "Label: value" pairs wrapped to width, skipping empty
// values. maxLines > 0 caps the lines each value may take (cut with "…").
func kvHeaderLines(rows [][2]string, width, maxLines int) []string {
	var out []string
	for _, r := range rows {
		v := strings.TrimSpace(r[1])
		if v == "" {
			continue
		}
		line := asyncDetailLabelStyle.Render(r[0]+": ") + v
		lines := strings.Split(lipgloss.NewStyle().Width(width).Render(line), "\n")
		if maxLines > 0 && len(lines) > maxLines {
			lines = append(lines[:maxLines-1], strings.TrimRight(lines[maxLines-1], " ")+"…")
		}
		out = append(out, lines...)
	}
	return out
}

func (m asyncDetailModel) layout() (header string, tableH int) {
	lines := m.headerLines(m.width)
	maxH := (m.height - 2) / 2
	if maxH < 1 {
		maxH = 1
	}
	if len(lines) > maxH {
		lines = append(lines[:maxH-1], helpStyle.Render("…"))
	}
	tableH = m.height - 2 - len(lines)
	if tableH < 2 {
		tableH = 2
	}
	return strings.Join(lines, "\n"), tableH
}

func (m asyncDetailModel) resize(w, h int) asyncDetailModel {
	m.width, m.height = w, h
	_, th := m.layout()
	m.table = m.table.resize(w, th)
	return m
}

func (m asyncDetailModel) update(msg tea.Msg) (asyncDetailModel, tea.Cmd) {
	var cmd tea.Cmd
	m.table, cmd = m.table.update(msg)
	return m, cmd
}

func (m asyncDetailModel) title() string {
	name := m.msg.Name
	if name == "" {
		name = m.spec.Topic
	}
	return name
}

func (m asyncDetailModel) View() string {
	if m.err != "" {
		return helpStyle.Render(m.err) + "\n" + helpStyle.Render("esc back")
	}
	header, _ := m.layout()
	return header + "\n" + m.table.View() + "\n" + helpStyle.Render("↑/↓/pgup/pgdn scroll · esc back")
}

// asyncRowSource is the display form of a spec file path.
func asyncRowSource(file string) string {
	return filepath.Base(file)
}

package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// menuTarget identifies which screen a selected menu leaf opens.
type menuTarget int

const (
	menuTargetTopics menuTarget = iota
	menuTargetGroups
	menuTargetKafkaCache
	menuTargetKafkaSettings
	menuTargetSchemaCatalog
	menuTargetTopicMapping
	menuTargetSchemaSources
	menuTargetDuckDBUI
	menuTargetSQLRepl
)

// menuItem is one selectable leaf in the main menu.
type menuItem struct {
	label  string
	target menuTarget
	// summary is the one-line tooltip shown next to the selected item.
	summary string
	// details are extra lines for the tooltip panel: what the screen
	// needs and its main keys.
	details []string
}

var menuSections = []struct {
	heading string
	items   []menuItem
}{
	{
		heading: "kafka",
		items: []menuItem{
			{
				label: "topics", target: menuTargetTopics,
				summary: "Every topic on the cluster, with detected format, schema, message count and estimated size.",
				details: []string{"Needs: Kafka connection (DuckDB adds format, schema and size).", "S sample all · E estimate all · A both · / filter", "enter: topic detail (sample, schema, override)"},
			},
			{
				label: "consumer groups", target: menuTargetGroups,
				summary: "Consumer groups on the cluster, their state and members, and per-partition lag.",
				details: []string{"Needs: Kafka connection.", "enter: committed offsets, end offsets and lag per partition"},
			},
			{
				label: "cache", target: menuTargetKafkaCache,
				summary: "Topics synced to local DuckDB: messages consumed and decoded continuously so you can query them with SQL.",
				details: []string{"a add topic · p pause/resume · d drop · enter details", "Resuming a duration/count-limited sync from \"now\" re-runs it for that duration/count from the topic end."},
			},
		},
	},
	{
		heading: "sql",
		items: []menuItem{
			{
				label: "repl", target: menuTargetSQLRepl,
				summary: "A DuckDB-CLI-style SQL prompt on bufka's database: query cached topics and samples, with .mode output formats and dot-commands.",
				details: []string{".sqlrc sets a startup script (saved in the database); history in ~/.local/state/bufka.", "enter runs a statement ending in ; · alt+enter new line · tab complete", ".help commands · .topics cached topics · .tail TOPIC · .browse last result"},
			},
		},
	},
	{
		heading: "schemas",
		items: []menuItem{
			{
				label: "catalog", target: menuTargetSchemaCatalog,
				summary: "All schemas loaded from proto import paths and AsyncAPI specs, with the topics each one is used for.",
				details: []string{"tab: all / proto / asyncapi · / filter", "enter: field-by-field definition (types, required, enums, docs)"},
			},
			{
				label: "topic mapping", target: menuTargetTopicMapping,
				summary: "Which schema each topic is decoded with, how it was chosen, and which mappings need attention.",
				details: []string{"Includes spec topics not yet sampled.", "tab: all / problems / ok · s sort · enter: topic detail to fix"},
			},
		},
	},
	{
		heading: "settings",
		items: []menuItem{
			{
				label: "kafka connection", target: menuTargetKafkaSettings,
				summary: "Brokers, SASL/TLS and consumer group settings used by this session.",
				details: []string{"Empty fields fall back to the environment.", "Saved in the database."},
			},
			{
				label: "schema sources", target: menuTargetSchemaSources,
				summary: "Where schemas are loaded from: protobuf roots, import-only include paths, and AsyncAPI spec files or directories.",
				details: []string{"Defaults: PROTO_IMPORT_PATHS, PROTO_INCLUDE_PATHS, ASYNCAPI_PATHS.", "tab list · a add · e edit · d delete · s apply (and save)"},
			},
			{
				label: "duckdb ui", target: menuTargetDuckDBUI,
				summary: "Turn DuckDB's built-in browser UI on or off and choose its localhost port, to explore cached and sampled data with SQL.",
				details: []string{"Off by default (DUCKDB_UI_ENABLED); saved settings win.", "space toggle · e edit port · s apply & save · o open in browser", "Press o here to open the running UI in your browser."},
			},
		},
	},
}

var (
	menuHeadingStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("63"))

	menuItemStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("250")).
			PaddingLeft(2)

	menuTipTitleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255"))
	menuTipStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("250"))
	menuKeyStyle      = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("63"))

	menuItemSelectedStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("255")).
				Background(lipgloss.Color("63")).
				PaddingLeft(2)
)

// menuModel is the main-menu page: the BufKa splash art followed by a
// navigable tree of kafka/schemas/settings sections.
type menuModel struct {
	width, height int
	cursor        int // index into the flattened list of leaf items
	cacheSummary  string
	// duckDBUISummary is the DuckDB UI state shown next to its item.
	duckDBUISummary string
	// note is a one-line result (e.g. of opening the browser), cleared on
	// the next key press.
	note     string
	noteIsOK bool
}

func newMenuModel() menuModel {
	return menuModel{}
}

func (m menuModel) Init() tea.Cmd {
	return nil
}

func (m menuModel) resize(w, h int) menuModel {
	m.width, m.height = w, h
	return m
}

// leafCount is the total number of selectable items across all sections.
func (m menuModel) leafCount() int {
	n := 0
	for _, s := range menuSections {
		n += len(s.items)
	}
	return n
}

// selected returns the menuItem at the current cursor position.
func (m menuModel) selected() menuItem {
	i := m.cursor
	for _, s := range menuSections {
		if i < len(s.items) {
			return s.items[i]
		}
		i -= len(s.items)
	}
	return menuItem{}
}

// menuSelectedMsg is emitted when the user picks a leaf item.
type menuSelectedMsg struct{ target menuTarget }

// menuOpenDuckDBUIMsg asks the root Model to open the DuckDB UI in the
// browser (o on the duckdb ui item).
type menuOpenDuckDBUIMsg struct{}

func (m menuModel) update(msg tea.Msg) (menuModel, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok {
		m.note = ""
		switch key.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < m.leafCount()-1 {
				m.cursor++
			}
		case "enter":
			target := m.selected().target
			return m, func() tea.Msg { return menuSelectedMsg{target: target} }
		case "o":
			if m.selected().target == menuTargetDuckDBUI {
				return m, func() tea.Msg { return menuOpenDuckDBUIMsg{} }
			}
		}
	}
	return m, nil
}

// menuHelp is the general help section under the menu: key bindings that
// work everywhere and a suggested first workflow.
var menuHelp = [][2]string{
	{"↑/↓ enter", "move and open"},
	{"esc", "back to the previous screen"},
	{"/", "filter lists"},
	{"q, ctrl+c", "quit"},
	{"", ""},
	{"1", "settings > schema sources: point bufka at your .proto / AsyncAPI files"},
	{"2", "kafka > topics: S samples every topic and detects its schema"},
	{"3", "schemas > topic mapping: review problems, override in the topic detail"},
	{"4", "kafka > cache: sync topics into DuckDB to query them with SQL"},
}

// menuPanelGap separates the menu from the tooltip panel.
const menuPanelGap = 2

func (m menuModel) menuList() string {
	var b strings.Builder
	i := 0
	for _, s := range menuSections {
		b.WriteString(menuHeadingStyle.Render("## " + s.heading))
		b.WriteString("\n")
		for _, item := range s.items {
			label := item.label
			if item.target == menuTargetKafkaCache && m.cacheSummary != "" {
				label = fmt.Sprintf("cache %s", m.cacheSummary)
			}
			if item.target == menuTargetDuckDBUI && m.duckDBUISummary != "" {
				label += " " + m.duckDBUISummary
			}
			line := "- " + label
			if i == m.cursor {
				b.WriteString(menuItemSelectedStyle.Render(line))
			} else {
				b.WriteString(menuItemStyle.Render(line))
			}
			b.WriteString("\n")
			i++
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// sectionOf returns the heading of the section containing the cursor.
func (m menuModel) sectionOf() string {
	i := m.cursor
	for _, s := range menuSections {
		if i < len(s.items) {
			return s.heading
		}
		i -= len(s.items)
	}
	return ""
}

// tooltip describes the selected item, wrapped to width.
func (m menuModel) tooltip(width int) string {
	it := m.selected()
	st := menuTipStyle.Width(width)
	lines := []string{
		menuTipTitleStyle.Render(m.sectionOf() + " › " + it.label),
		"",
		st.Render(it.summary),
	}
	if len(it.details) > 0 {
		lines = append(lines, "")
		for _, d := range it.details {
			lines = append(lines, helpStyle.Width(width).Render(d))
		}
	}
	return strings.Join(lines, "\n")
}

func (m menuModel) helpSection(width int) string {
	keyW := 0
	for _, h := range menuHelp {
		keyW = max(keyW, lipgloss.Width(h[0]))
	}
	lines := []string{menuHeadingStyle.Render("## help"), ""}
	for _, h := range menuHelp {
		if h[0] == "" {
			lines = append(lines, "", menuTipTitleStyle.Render("getting started"))
			continue
		}
		key := menuKeyStyle.Render(fmt.Sprintf("%-*s", keyW, h[0]))
		desc := menuTipStyle.Width(max(width-keyW-2, 10)).Render(h[1])
		lines = append(lines, lipgloss.JoinHorizontal(lipgloss.Top, key, "  ", desc))
	}
	return strings.Join(lines, "\n")
}

func (m menuModel) View() string {
	logo := splashLogoStyle.Render(bufkaLogo)
	list := m.menuList()
	listW := lipgloss.Width(list)
	box := borderStyle.Padding(0, 1)
	frame := box.GetHorizontalFrameSize()

	// Side by side when the menu, gap and a useful tooltip fit; stacked
	// otherwise. The tooltip and help share one column width.
	const tipMin, tipMax = 36, 64
	sideBySide := m.width == 0 || m.width >= listW+menuPanelGap+tipMin+2*frame
	tipW := tipMax
	if sideBySide && m.width > 0 {
		tipW = min(tipMax, m.width-listW-menuPanelGap-2*frame)
	} else if !sideBySide {
		tipW = max(min(tipMax, m.width-frame), 20)
	}

	tip := m.tooltip(tipW)
	var top string
	if sideBySide {
		// Equal heights so both boxes line up.
		h := max(lipgloss.Height(list), lipgloss.Height(tip))
		// Width and Height include the border and padding.
		vf := box.GetVerticalFrameSize()
		left := box.Height(h + vf).Render(list)
		right := box.Width(tipW + frame).Height(h + vf).Render(tip)
		top = lipgloss.JoinHorizontal(lipgloss.Top, left, strings.Repeat(" ", menuPanelGap), right)
	} else {
		top = lipgloss.JoinVertical(lipgloss.Left, box.Render(list), box.Width(tipW+frame).Render(tip))
	}

	helpW := lipgloss.Width(top) - frame
	help := box.Width(helpW + frame).Render(m.helpSection(helpW))
	hint := "↑/↓ navigate · enter open · q quit"
	if m.selected().target == menuTargetDuckDBUI {
		hint = "↑/↓ navigate · enter open · o open in browser · q quit"
	}
	keys := helpStyle.Render(hint)
	if m.note != "" {
		if m.noteIsOK {
			keys = statusStyle.Render(m.note) + "\n" + keys
		} else {
			keys = errorStyle.Render(m.note) + "\n" + keys
		}
	}

	parts := []string{logo, "", top, help, keys}
	if m.height > 0 && lipgloss.Height(lipgloss.JoinVertical(lipgloss.Center, parts...)) > m.height {
		parts = []string{logo, "", top, keys} // drop help before clipping
		if lipgloss.Height(lipgloss.JoinVertical(lipgloss.Center, parts...)) > m.height {
			parts = []string{top, keys}
		}
	}
	content := lipgloss.JoinVertical(lipgloss.Center, parts...)
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, content)
	}
	return content
}

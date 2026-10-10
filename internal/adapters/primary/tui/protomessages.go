package tui

import (
	"context"
	"sort"
	"strings"

	"charm.land/bubbles/v2/textinput"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
)

// protoMessageRow is one resolved protobuf message and the topic(s) whose
// kafka_topic_schemas row resolved to it.
type protoMessageRow struct {
	kind   string // catalogKindProto or catalogKindAsyncAPI
	name   string // display name: proto FQN, or AsyncAPI message name
	fqn    string // proto FQN (proto rows only)
	topics []string
	source string // source file shown in the SOURCE FILE column

	// asyncTopic/asyncIdx locate an AsyncAPI row's TopicSpec.Messages entry.
	asyncTopic string
	asyncIdx   int
}

const (
	catalogKindProto    = "proto"
	catalogKindAsyncAPI = "asyncapi"
)

// catalogKinds is the tab cycle order; "" means all kinds.
var catalogKinds = []string{"", catalogKindProto, catalogKindAsyncAPI}

// protoMessagesModel lists every resolved protobuf message's fully-qualified
// name alongside the Kafka topic(s) whose kafka_topic_schemas row resolved to it.
// The topics column wraps rather than truncates, since a message can match
// many topics. One row can be selected (cursor) and opened for its full
// schema definition, the list can be filtered by message FQN or matched
// topic name, and a separate toggle can hide messages with no resolved
// topics.
type protoMessagesModel struct {
	deps Deps
	all  []protoMessageRow
	rows []protoMessageRow

	empty    string
	filter   textinput.Model
	filterOn bool

	// resolvedOnly, when true, hides rows whose topics list is empty
	// (i.e. messages that did not resolve to any Kafka topic). Independent
	// of the text filter.
	resolvedOnly bool

	// kindFilter is "" (all), catalogKindProto or catalogKindAsyncAPI.
	kindFilter string

	cursor int
	vp     viewport.Model
	width  int
	height int
}

var (
	protoMsgHeaderStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("250"))
	protoMsgFQNStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
	protoMsgTopicsStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	protoMsgSelectedFQN  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255")).Background(lipgloss.Color("63"))
	protoMsgSelectedTopi = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(lipgloss.Color("63"))
)

func newProtoMessagesModel(deps Deps) protoMessagesModel {
	fi := textinput.New()
	fi.Placeholder = "filter by name, topic or file"
	m := protoMessagesModel{deps: deps, filter: fi}

	hasProto := deps.Messages != nil && deps.Messages.Len() > 0
	hasAsync := deps.AsyncAPI != nil && len(deps.AsyncAPI.Topics()) > 0
	if !hasProto && !hasAsync {
		m.empty = "no schemas loaded (set PROTO_IMPORT_PATHS or ASYNCAPI_PATHS)"
		return m
	}

	if hasProto {
		topicsByMessage := make(map[string][]string)
		if deps.DB != nil {
			if schemas, err := database.ReadTopicSchemas(context.Background(), deps.DB); err == nil {
				for topic, entry := range schemas {
					if entry.MessageName == "" {
						continue
					}
					topicsByMessage[entry.MessageName] = append(topicsByMessage[entry.MessageName], topic)
				}
			}
		}
		for _, topics := range topicsByMessage {
			sort.Strings(topics)
		}

		fqns := append([]string{}, deps.Messages.Keys()...)
		sort.Strings(fqns)
		for _, fqn := range fqns {
			info, _ := deps.Messages.Get(fqn)
			m.all = append(m.all, protoMessageRow{
				kind: catalogKindProto, name: fqn, fqn: fqn,
				topics: topicsByMessage[fqn], source: info.File,
			})
		}
	}

	if hasAsync {
		for _, ts := range deps.AsyncAPI.Topics() {
			msgs := ts.Messages
			if len(msgs) == 0 {
				msgs = []asyncapi.MessageSpec{{}}
			}
			for i, ms := range msgs {
				name := ms.Name
				if name == "" {
					name = ts.Topic
				}
				topic := ts.Topic
				if ms.EventType != "" {
					topic += " (" + ms.EventType + ")"
				}
				m.all = append(m.all, protoMessageRow{
					kind: catalogKindAsyncAPI, name: name, topics: []string{topic},
					source: asyncRowSource(ts.SpecFile), asyncTopic: ts.Topic, asyncIdx: i,
				})
			}
		}
	}
	// Stable sort by name so proto and asyncapi rows interleave; ties keep
	// proto first, then topic order.
	sort.SliceStable(m.all, func(i, j int) bool { return m.all[i].name < m.all[j].name })
	m.rows = m.all
	return m
}

func (m protoMessagesModel) Init() tea.Cmd {
	return nil
}

func (m protoMessagesModel) resize(w, h int) (protoMessagesModel, tea.Cmd) {
	m.width, m.height = w, h
	vh := h - 2
	if m.filterOn || m.filter.Value() != "" {
		vh--
	}
	if vh < 1 {
		vh = 1
	}
	m.vp = resizedViewport(m.vp, w, vh)
	m.vp.SetContent(m.render())
	return m, nil
}

// selected returns the fully-qualified name of the row under the cursor.
func (m protoMessagesModel) selected() (string, bool) {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return "", false
	}
	return m.rows[m.cursor].name, true
}

// selectedRow returns the row under the cursor.
func (m protoMessagesModel) selectedRow() (protoMessageRow, bool) {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return protoMessageRow{}, false
	}
	return m.rows[m.cursor], true
}

func (m protoMessagesModel) filtering() bool {
	return m.filterOn
}

// applyFilter recomputes m.rows from m.all using the current filter text and
// the resolvedOnly toggle: filter text matches against the message FQN or
// any topic it resolved to, and resolvedOnly (when on) additionally drops
// any message with no resolved topics. Resets the cursor and viewport
// content/size for the new row count.
func (m *protoMessagesModel) applyFilter() {
	q := strings.ToLower(strings.TrimSpace(m.filter.Value()))

	rows := make([]protoMessageRow, 0, len(m.all))
	for _, r := range m.all {
		if m.kindFilter != "" && r.kind != m.kindFilter {
			continue
		}
		if m.resolvedOnly && len(r.topics) == 0 {
			continue
		}
		if q == "" {
			rows = append(rows, r)
			continue
		}
		if strings.Contains(strings.ToLower(r.name), q) || strings.Contains(strings.ToLower(r.source), q) {
			rows = append(rows, r)
			continue
		}
		for _, t := range r.topics {
			if strings.Contains(strings.ToLower(t), q) {
				rows = append(rows, r)
				break
			}
		}
	}
	m.rows = rows

	if m.cursor >= len(m.rows) {
		m.cursor = len(m.rows) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	vh := m.height - 2
	if m.filterOn || m.filter.Value() != "" {
		vh--
	}
	if vh < 1 {
		vh = 1
	}
	m.vp = resizedViewport(m.vp, m.width, vh)
	m.vp.SetContent(m.render())
}

// protoMessageSelectedMsg is emitted when the user opens a proto message's schema.
type protoMessageSelectedMsg struct{ fqn string }

// asyncMessageSelectedMsg is emitted when the user opens an AsyncAPI message.
type asyncMessageSelectedMsg struct {
	topic string
	idx   int
}

func (m protoMessagesModel) update(msg tea.Msg) (protoMessagesModel, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok {
		if m.filterOn {
			switch key.String() {
			case "esc", "enter":
				m.filterOn = false
				m.filter.Blur()
				m.applyFilter()
				return m, nil
			}
			var cmd tea.Cmd
			m.filter, cmd = m.filter.Update(msg)
			m.applyFilter()
			return m, cmd
		}

		switch key.String() {
		case "/":
			m.filterOn = true
			cmd := m.filter.Focus()
			m.applyFilter()
			return m, cmd
		case "tab":
			for i, k := range catalogKinds {
				if k == m.kindFilter {
					m.kindFilter = catalogKinds[(i+1)%len(catalogKinds)]
					break
				}
			}
			m.applyFilter()
			return m, nil
		case "r":
			m.resolvedOnly = !m.resolvedOnly
			m.applyFilter()
			return m, nil
		case "esc":
			if m.filter.Value() != "" {
				m.filter.SetValue("")
				m.applyFilter()
				return m, nil
			}
		}

		if len(m.rows) > 0 {
			switch key.String() {
			case "up", "k":
				if m.cursor > 0 {
					m.cursor--
					m.vp.SetContent(m.render())
					m.ensureCursorVisible()
				}
				return m, nil
			case "down", "j":
				if m.cursor < len(m.rows)-1 {
					m.cursor++
					m.vp.SetContent(m.render())
					m.ensureCursorVisible()
				}
				return m, nil
			case "enter":
				if row, ok := m.selectedRow(); ok {
					if row.kind == catalogKindAsyncAPI {
						return m, func() tea.Msg { return asyncMessageSelectedMsg{topic: row.asyncTopic, idx: row.asyncIdx} }
					}
					return m, func() tea.Msg { return protoMessageSelectedMsg{fqn: row.fqn} }
				}
				return m, nil
			}
		}
	}
	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

// ensureCursorVisible scrolls the viewport so the selected row (one content
// line below the header) stays on screen.
func (m *protoMessagesModel) ensureCursorVisible() {
	line := m.cursor + 1 // +1 for the header line
	if line < m.vp.YOffset() {
		m.vp.SetYOffset(line)
	} else if max := m.vp.YOffset() + m.vp.Height() - 1; line > max {
		m.vp.SetYOffset(line - m.vp.Height() + 1)
	}
}

// render builds the full (unwrapped-by-viewport, column-wrapped) content:
// a fixed-width FQN column and a word-wrapped TOPICS column that grows to
// fit however many topics matched, so nothing is truncated.
func (m protoMessagesModel) render() string {
	if m.width <= 0 {
		return ""
	}
	nameW := m.width * 3 / 10
	if nameW < 16 {
		nameW = 16
	}
	kindW := 8
	topicsW := m.width * 3 / 10
	if topicsW < 10 {
		topicsW = 10
	}
	sourceW := m.width - nameW - kindW - topicsW - 3
	if sourceW < 10 {
		sourceW = 10
	}
	join := func(ns, ks, ts, ss lipgloss.Style, n, k, t, src string) string {
		return lipgloss.JoinHorizontal(lipgloss.Top,
			ns.Width(nameW).Render(n), " ",
			ks.Width(kindW).Render(k), " ",
			ts.Width(topicsW).Render(t), " ",
			ss.Width(sourceW).Render(src),
		)
	}

	var b strings.Builder
	b.WriteString(join(protoMsgHeaderStyle, protoMsgHeaderStyle, protoMsgHeaderStyle, protoMsgHeaderStyle,
		"NAME", "KIND", "TOPIC(S)", "SOURCE FILE"))
	b.WriteString("\n")

	if len(m.rows) == 0 {
		msg := "no messages match the filter"
		if m.resolvedOnly && strings.TrimSpace(m.filter.Value()) == "" {
			msg = "no messages have a resolved topic"
		}
		b.WriteString(helpStyle.Render(msg))
		return strings.TrimRight(b.String(), "\n")
	}

	for i, row := range m.rows {
		topics := "-"
		if len(row.topics) > 0 {
			topics = strings.Join(row.topics, ", ")
		}
		src := row.source
		if src == "" {
			src = "-"
		}
		ns, ts := protoMsgFQNStyle, protoMsgTopicsStyle
		if i == m.cursor {
			ns, ts = protoMsgSelectedFQN, protoMsgSelectedTopi
		}
		b.WriteString(join(ns, ts, ts, ts, row.name, row.kind, topics, src))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m protoMessagesModel) View() string {
	if m.empty != "" {
		return helpStyle.Render(m.empty) + "\n" + helpStyle.Render("esc back to menu")
	}
	var b strings.Builder
	if m.filterOn || m.filter.Value() != "" {
		b.WriteString(m.filter.View())
		b.WriteString("\n")
	}
	b.WriteString(m.vp.View())
	b.WriteString("\n")
	help := "↑/↓ navigate · / filter · tab kind · r toggle resolved-only · enter open · esc back to menu"
	if m.resolvedOnly {
		help = "[resolved-only] " + help
	}
	help = "[kind: " + m.kindLabel() + "] " + help
	b.WriteString(helpStyle.Render(help))
	return b.String()
}

// kindLabel names the active kind filter.
func (m protoMessagesModel) kindLabel() string {
	if m.kindFilter == "" {
		return "all"
	}
	return m.kindFilter
}

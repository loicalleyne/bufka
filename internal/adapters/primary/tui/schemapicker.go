package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
)

// schemaPickerModel lets the user override the protobuf message a topic is
// decoded as. The left pane lists every message that decodes the topic's
// sampled values cleanly (topicsample.Rank, best first); the right pane
// previews the samples decoded with the highlighted message. Enter saves the
// choice to kafka_topic_schemas as an override that later re-sampling keeps.
type schemaPickerModel struct {
	deps  Deps
	topic string

	width, height int

	loading bool
	loadErr error

	// rows are the sampled messages; their Value is what gets re-decoded.
	rows       []database.SampleRow
	candidates []topicsample.Candidate
	current    string
	cursor     int

	listVP    viewport.Model
	previewVP viewport.Model
}

var (
	pickerItemStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
	pickerSelectedStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255")).Background(lipgloss.Color("63"))
	pickerCurrentStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
)

func newSchemaPickerModel(deps Deps, topic string) schemaPickerModel {
	m := schemaPickerModel{deps: deps, topic: topic, loading: true}
	if deps.DB != nil {
		if schemas, err := database.ReadTopicSchemas(context.Background(), deps.DB); err == nil {
			m.current = schemas[topic].MessageName
		}
	}
	return m
}

type schemaPickerLoadedMsg struct {
	topic      string
	rows       []database.SampleRow
	candidates []topicsample.Candidate
	err        error
}

// schemaOverriddenMsg reports that topic's message was overridden.
type schemaOverriddenMsg struct {
	topic, message string
	err            error
}

func (m schemaPickerModel) Init() tea.Cmd {
	deps, topic := m.deps, m.topic
	return func() tea.Msg {
		rows, err := pickerSamples(context.Background(), deps, topic)
		if err != nil {
			return schemaPickerLoadedMsg{topic: topic, err: err}
		}
		values := make([][]byte, len(rows))
		for i, r := range rows {
			values[i] = r.Value
		}
		return schemaPickerLoadedMsg{topic: topic, rows: rows, candidates: topicsample.Rank(topic, values, deps.Messages)}
	}
}

// pickerSamples returns the topic's stored _sample rows, else live-samples it.
func pickerSamples(ctx context.Context, deps Deps, topic string) ([]database.SampleRow, error) {
	table, err := kafkaingest.SampleTableName(topic)
	if err != nil {
		return nil, err
	}
	if deps.DB != nil {
		if rows, ok, err := database.ReadSampleTable(ctx, deps.DB, table); err == nil && ok && len(rows) > 0 {
			return rows, nil
		}
	}
	timeout := deps.SampleTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	count := deps.SampleCount
	if count <= 0 {
		count = 50
	}
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
	if err != nil && err != kafka.ErrTopicEmpty {
		return nil, err
	}
	rows := make([]database.SampleRow, len(records))
	for i, r := range records {
		rows[i] = database.SampleRow{
			Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp,
			Key: r.Key, Value: r.Value,
		}
	}
	return rows, nil
}

func (m schemaPickerModel) paneWidths() (int, int) {
	left := m.width * 2 / 5
	if left < 24 {
		left = 24
	}
	right := m.width - left - 1
	if right < 10 {
		right = 10
	}
	return left, right
}

func (m schemaPickerModel) resize(w, h int) schemaPickerModel {
	m.width, m.height = w, h
	vh := h - 2
	if vh < 1 {
		vh = 1
	}
	lw, rw := m.paneWidths()
	m.listVP = resizedViewport(m.listVP, lw, vh)
	m.previewVP = resizedViewport(m.previewVP, rw, vh)
	m.refresh()
	return m
}

func (m *schemaPickerModel) refresh() {
	m.listVP.SetContent(m.renderList())
	m.previewVP.SetContent(m.renderPreview())
	m.previewVP.GotoTop()
	// Keep the cursor visible (header line + one line per candidate).
	line := m.cursor + 1
	if line < m.listVP.YOffset() {
		m.listVP.SetYOffset(line)
	} else if last := m.listVP.YOffset() + m.listVP.Height() - 1; line > last {
		m.listVP.SetYOffset(line - m.listVP.Height() + 1)
	}
}

func (m schemaPickerModel) selected() (topicsample.Candidate, bool) {
	if m.cursor < 0 || m.cursor >= len(m.candidates) {
		return topicsample.Candidate{}, false
	}
	return m.candidates[m.cursor], true
}

func (m schemaPickerModel) update(msg tea.Msg) (schemaPickerModel, tea.Cmd) {
	switch msg := msg.(type) {
	case schemaPickerLoadedMsg:
		if msg.topic != m.topic {
			return m, nil
		}
		m.loading = false
		m.loadErr = msg.err
		m.rows, m.candidates = msg.rows, msg.candidates
		m.cursor = 0
		for i, c := range m.candidates {
			if c.Name == m.current {
				m.cursor = i
				break
			}
		}
		m.refresh()
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		return m, setStatus(fmt.Sprintf("%s: %d matching messages over %d samples", m.topic, len(m.candidates), len(m.rows)))

	case schemaOverriddenMsg:
		if msg.err == nil && msg.topic == m.topic {
			m.current = msg.message
			m.listVP.SetContent(m.renderList())
		}
		return m, nil

	case tea.KeyPressMsg:
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
				m.refresh()
			}
			return m, nil
		case "down", "j":
			if m.cursor < len(m.candidates)-1 {
				m.cursor++
				m.refresh()
			}
			return m, nil
		case "enter":
			if c, ok := m.selected(); ok {
				return m, applySchemaOverrideCmd(m.deps, m.topic, c, m.rows)
			}
			return m, nil
		case "pgdown", "pgup", "J", "K", "ctrl+d", "ctrl+u":
			// Scroll the preview pane.
			switch msg.String() {
			case "J":
				m.previewVP.ScrollDown(1)
			case "K":
				m.previewVP.ScrollUp(1)
			case "pgdown", "ctrl+d":
				m.previewVP.HalfPageDown()
			case "pgup", "ctrl+u":
				m.previewVP.HalfPageUp()
			}
			return m, nil
		}
	}
	return m, nil
}

// applySchemaOverrideCmd records c as topic's message in kafka_topic_schemas
// (marked as a user override) and rewrites the stored _sample table decoded with it.
func applySchemaOverrideCmd(deps Deps, topic string, c topicsample.Candidate, rows []database.SampleRow) tea.Cmd {
	rows = append([]database.SampleRow(nil), rows...)
	return func() tea.Msg {
		values := make([][]byte, len(rows))
		for i, r := range rows {
			values[i] = r.Value
		}
		decoded, matched := topicsample.DecodeWith(values, c.Info, c.CutConfluencePrefix)
		for i := range rows {
			d := decoded[i]
			rows[i].DetectedFormat = topicsample.FormatProtobuf
			rows[i].MessageName = c.Name
			rows[i].CutConfluencePrefix = c.CutConfluencePrefix
			rows[i].ConfluentHeaderLen = int32(d.HeaderLen)
			rows[i].ConfluentSchemaID = d.SchemaID
			rows[i].ConfluentMessageIndexes = d.MessageIndexes
			rows[i].DecodeOK = d.DecodeOK
			rows[i].DecodedJSON = d.Decoded
		}
		ctx := context.Background()
		table, err := kafkaingest.SampleTableName(topic)
		if err != nil {
			return schemaOverriddenMsg{topic: topic, err: err}
		}
		if deps.DB == nil {
			return schemaOverriddenMsg{topic: topic, err: errors.New("saving a schema override needs DuckDB")}
		}
		if len(rows) > 0 {
			if err := database.WriteSampleTable(ctx, deps.DB, table, rows); err != nil {
				return errMsg{err}
			}
		} else {
			table = ""
		}
		{
			schemaRow := database.TopicSchemaRow{
				Topic:               topic,
				Table:               table,
				Format:              topicsample.FormatProtobuf,
				MessageName:         c.Name,
				ProtoFile:           c.Info.File,
				ImportRoot:          c.Info.Root,
				CutConfluencePrefix: c.CutConfluencePrefix,
				MessageOverride:     true,
				ResolvedBy:          topicsample.ResolvedByOverride,
				SampleCount:         int32(len(rows)),
				MatchedCount:        int32(matched),
				UpdatedAt:           time.Now().UTC(),
			}
			if err := database.UpsertTopicSchemas(ctx, deps.DB, []database.TopicSchemaRow{schemaRow}); err != nil {
				return schemaOverriddenMsg{topic: topic, err: err}
			}
		}
		return schemaOverriddenMsg{topic: topic, message: c.Name}
	}
}

func (m schemaPickerModel) renderList() string {
	lw, _ := m.paneWidths()
	var b strings.Builder
	b.WriteString(protoMsgHeaderStyle.Render(fmt.Sprintf("MATCHING MESSAGES (%d)", len(m.candidates))))
	b.WriteString("\n")
	switch {
	case m.loading:
		b.WriteString(helpStyle.Render("sampling..."))
	case m.loadErr != nil:
		b.WriteString(errorStyle.Render(m.loadErr.Error()))
	case len(m.rows) == 0:
		b.WriteString(helpStyle.Render("no sampled messages"))
	case len(m.candidates) == 0:
		b.WriteString(helpStyle.Render("no loaded message decodes every sample"))
	}
	for i, c := range m.candidates {
		mark := "  "
		if c.Name == m.current {
			mark = "● "
		}
		label := mark + c.Name
		if c.CutConfluencePrefix {
			label += " (cut)"
		}
		style := pickerItemStyle
		if i == m.cursor {
			style = pickerSelectedStyle
		} else if c.Name == m.current {
			style = pickerCurrentStyle
		}
		b.WriteString(style.Width(lw).MaxWidth(lw).Render(label))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m schemaPickerModel) renderPreview() string {
	c, ok := m.selected()
	if !ok {
		return ""
	}
	_, rw := m.paneWidths()
	values := make([][]byte, len(m.rows))
	for i, r := range m.rows {
		values[i] = r.Value
	}
	decoded, matched := topicsample.DecodeWith(values, c.Info, c.CutConfluencePrefix)
	var b strings.Builder
	fmt.Fprintf(&b, "%s  file=%s  decoded=%d/%d\n\n", c.Name, c.Info.File, matched, len(m.rows))
	for i, r := range m.rows {
		fmt.Fprintf(&b, "[p%d@%d] %s\n", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339))
		writeDecodedRow(&b, decoded, i, r.Value)
	}
	return lipgloss.NewStyle().Width(rw).Render(b.String())
}

func (m schemaPickerModel) View() string {
	body := lipgloss.JoinHorizontal(lipgloss.Top, m.listVP.View(), " ", m.previewVP.View())
	help := "↑/↓ select · J/K pgup/pgdn scroll preview · enter use for topic · esc back   (● = current)"
	return body + "\n" + helpStyle.Render(help)
}

package tui

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
)

// topicRow is one row of the topic list, merging live topic names with
// stored schema detection and stored size estimates.
type topicRow struct {
	Topic    string
	Format   string
	Schema   string
	Messages int64
	Estimate int64
}

type topicListModel struct {
	deps Deps

	table    table.Model
	spinner  spinner.Model
	filter   textinput.Model
	filterOn bool

	loading bool
	all     []topicRow
	err     error

	batchRunning bool
	batchKind    batchKind
	batchDone    int
	batchTotal   int
	batchTopic   string
	batchCancel  context.CancelFunc
	batchSub     chan interface{}
	batchBar     progress.Model
}

// topicsLoadedMsg carries the result of the background topic list load.
type topicsLoadedMsg struct {
	rows []topicRow
	err  error
}

func newTopicListModel(deps Deps) topicListModel {
	cols := []table.Column{
		{Title: "TOPIC", Width: 32},
		{Title: "FORMAT", Width: 10},
		{Title: "SCHEMA", Width: 32},
		{Title: "MESSAGES", Width: 12},
		{Title: "EST. SIZE", Width: 12},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true))
	s := spinner.New()
	fi := textinput.New()
	fi.Placeholder = "filter topics"
	return topicListModel{deps: deps, table: t, spinner: s, filter: fi, loading: true, batchBar: progress.New(progress.WithDefaultBlend())}
}

func (m topicListModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, loadTopicsCmd(m.deps))
}

// loadTopicsCmd lists topics and merges stored schemas + estimates, the
// same data kafka list-topics / show-sizes / show-schema read.
func loadTopicsCmd(deps Deps) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		names, err := kafka.ListTopics(ctx, deps.KafkaConf)
		if err != nil {
			return topicsLoadedMsg{err: err}
		}

		var estimates map[string]database.EstimateRow
		var schemas map[string]database.TopicSchemaRow
		if deps.DB != nil {
			if rows, err := database.ReadEstimateTable(ctx, deps.DB); err == nil {
				estimates = make(map[string]database.EstimateRow, len(rows))
				for _, r := range rows {
					estimates[r.Topic] = r
				}
			}
			if sc, err := database.ReadTopicSchemas(ctx, deps.DB); err == nil {
				schemas = sc
			}
		}

		rows := make([]topicRow, 0, len(names))
		for _, name := range names {
			row := topicRow{Topic: name}
			if schemas != nil {
				if s, ok := schemas[name]; ok {
					row.Format = s.Format
					row.Schema = s.MessageName
				}
			}
			if est, ok := estimates[name]; ok {
				row.Messages = est.TotalMessages
				row.Estimate = est.EstimatedBytes
			}
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Topic < rows[j].Topic })
		return topicsLoadedMsg{rows: rows}
	}
}

func (m topicListModel) filtering() bool {
	return m.filterOn
}

// visibleTopics returns the topic names currently displayed in the table (or
// all topics if the table has none selected/filtered).
func (m topicListModel) visibleTopics() []string {
	rows := m.table.Rows()
	if len(rows) == 0 {
		return nil
	}
	topics := make([]string, 0, len(rows))
	for _, r := range rows {
		if len(r) > 0 {
			topics = append(topics, r[0])
		}
	}
	return topics
}

func (m topicListModel) resize(w, h int) (topicListModel, tea.Cmd) {
	m.table.SetWidth(w)
	th := h - 4
	if th < 1 {
		th = 1
	}
	m.table.SetHeight(th)
	cols := m.table.Columns()
	// fixed columns: FORMAT (10), MESSAGES (12), EST. SIZE (12) + 10 for table borders & padding
	const fixedCols = 10 + 12 + 12 + 10
	if len(cols) == 5 && w > fixedCols+20 {
		remaining := w - fixedCols

		// Find the longest topic and schema names to optimize space allocation
		maxTopic := 20
		maxSchema := 15
		for _, r := range m.all {
			if len(r.Topic) > maxTopic {
				maxTopic = len(r.Topic)
			}
			if len(r.Schema) > maxSchema {
				maxSchema = len(r.Schema)
			}
		}

		// If available width is ample, allow TOPIC to take its full untruncated width
		topicW := remaining * 50 / 100
		if maxTopic < topicW {
			topicW = maxTopic
		} else if maxTopic < remaining-maxSchema {
			topicW = maxTopic
		}

		cols[0].Width = topicW
		cols[2].Width = remaining - topicW
		m.table.SetColumns(cols)
	}
	barWidth := w - 4
	if barWidth < 10 {
		barWidth = 10
	}
	m.batchBar.SetWidth(barWidth)
	return m, nil
}

func (m topicListModel) selectedTopic() (string, bool) {
	row := m.table.SelectedRow()
	if len(row) == 0 {
		return "", false
	}
	return row[0], true
}

func (m *topicListModel) applyFilter() {
	q := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	rows := make([]table.Row, 0, len(m.all))
	for _, r := range m.all {
		if q != "" && !strings.Contains(strings.ToLower(r.Topic), q) {
			continue
		}
		rows = append(rows, table.Row{
			r.Topic,
			formatOrDash(r.Format),
			schemaOrDash(r.Schema),
			countOrDash(r.Messages),
			sizeOrDash(r.Estimate),
		})
	}
	m.table.SetRows(rows)
}

func formatOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func schemaOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func countOrDash(n int64) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", n)
}

func sizeOrDash(n int64) string {
	if n == 0 {
		return "-"
	}
	return humanBytes(n)
}

func waitBatchCmd(sub chan interface{}) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-sub
		if !ok {
			return nil
		}
		return msg
	}
}

func (m topicListModel) startBatch(ctx context.Context, kind batchKind) (topicListModel, tea.Cmd) {
	if m.batchRunning {
		return m, nil
	}
	topics := m.visibleTopics()
	if len(topics) == 0 {
		return m, setStatus("no topics to process")
	}
	batchCtx, cancel := context.WithCancel(ctx)
	m.batchCancel = cancel
	m.batchRunning = true
	m.batchKind = kind
	m.batchDone = 0
	m.batchTotal = len(topics)
	m.batchTopic = topics[0]
	m.batchSub = make(chan interface{}, 32)

	go runBatchProcess(batchCtx, m.deps, topics, kind, m.batchSub)

	return m, tea.Batch(
		m.spinner.Tick,
		waitBatchCmd(m.batchSub),
		setStatus(fmt.Sprintf("%s %d topics (esc to cancel)...", kind, len(topics))),
	)
}

func (m topicListModel) update(ctx context.Context, msg tea.Msg) (topicListModel, tea.Cmd) {
	switch msg := msg.(type) {
	case batchProgressMsg:
		m.batchRunning = true
		m.batchKind = msg.kind
		m.batchDone = msg.done
		m.batchTotal = msg.total
		m.batchTopic = msg.currentTopic
		status := fmt.Sprintf("%s [%d/%d] %s (esc to cancel)...", msg.kind, msg.done+1, msg.total, msg.currentTopic)
		return m, tea.Batch(waitBatchCmd(m.batchSub), setStatus(status))

	case batchFinishedMsg:
		m.batchRunning = false
		m.batchCancel = nil
		m.batchSub = nil
		status := fmt.Sprintf("%s finished: %d/%d topics processed", msg.kind, msg.processed, msg.total)
		if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
			status = fmt.Sprintf("%s finished with error: %v (%d/%d topics)", msg.kind, msg.err, msg.processed, msg.total)
		} else if errors.Is(msg.err, context.Canceled) {
			status = fmt.Sprintf("%s canceled (%d/%d topics processed)", msg.kind, msg.processed, msg.total)
		}
		return m, tea.Batch(loadTopicsCmd(m.deps), setStatus(status))

	case topicsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, setErr(msg.err)
		}
		m.err = nil
		m.all = msg.rows
		if m.table.Width() > 0 {
			m, _ = m.resize(m.table.Width(), m.table.Height()+4)
		}
		m.applyFilter()
		return m, setStatus(fmt.Sprintf("%d topics", len(m.all)))

	case spinner.TickMsg:
		if m.loading || m.batchRunning {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil

	case tea.KeyPressMsg:
		if m.filterOn {
			switch msg.String() {
			case "esc":
				m.filterOn = false
				m.filter.Blur()
				return m, nil
			case "enter":
				m.filterOn = false
				m.filter.Blur()
				return m, nil
			}
			var cmd tea.Cmd
			m.filter, cmd = m.filter.Update(msg)
			m.applyFilter()
			return m, cmd
		}
		switch msg.String() {
		case "esc":
			if m.batchRunning && m.batchCancel != nil {
				m.batchCancel()
				return m, setStatus("canceling batch...")
			}
			if m.filter.Value() != "" {
				m.filter.SetValue("")
				m.applyFilter()
				return m, nil
			}
		case "/":
			if !m.batchRunning {
				m.filterOn = true
				return m, m.filter.Focus()
			}
		case "S":
			if !m.batchRunning {
				return m.startBatch(ctx, batchSample)
			}
		case "E":
			if !m.batchRunning {
				return m.startBatch(ctx, batchEstimate)
			}
		case "A":
			if !m.batchRunning {
				return m.startBatch(ctx, batchBoth)
			}
		case "r", "R":
			if !m.batchRunning && !m.loading {
				m.loading = true
				return m, tea.Batch(m.spinner.Tick, loadTopicsCmd(m.deps), setStatus("refreshing topics..."))
			}
		}
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m topicListModel) View() string {
	var b strings.Builder
	if m.filterOn || m.filter.Value() != "" {
		b.WriteString(m.filter.View())
		b.WriteString("\n")
	}
	if m.loading {
		b.WriteString(m.spinner.View())
		b.WriteString(" loading topics...\n")
	}
	if m.batchRunning {
		b.WriteString(m.spinner.View())
		fmt.Fprintf(&b, " [%d/%d] %s: %s (esc to cancel)\n", m.batchDone+1, m.batchTotal, m.batchKind, m.batchTopic)
		percent := 0.0
		if m.batchTotal > 0 {
			percent = float64(m.batchDone) / float64(m.batchTotal)
		}
		b.WriteString(m.batchBar.ViewAs(percent))
		b.WriteString("\n")
	}
	b.WriteString(m.table.View())
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓ navigate · / filter · enter open · m message schema · S sample all · E estimate all · A both · r refresh · g groups · q quit"))
	return b.String()
}

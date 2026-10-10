package tui

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
)

// Topic mapping STATUS values.
const (
	mapStatusOK         = "ok"
	mapStatusNoSchema   = "no schema"
	mapStatusNotSampled = "not sampled"
	mapStatusReview     = "review"
)

// mappingRow is one row of the topic mapping screen: a kafka_topic_schemas
// row or a spec-only topic taken from the AsyncAPI index.
type mappingRow struct {
	Topic      string
	Format     string
	Schema     string
	ResolvedBy string
	Warnings   []string
	HasSpec    bool
	SpecOnly   bool
	Review     bool // no tie/review flag is persisted today; always false
}

// mappingStatus computes the STATUS cell, in priority order: warnings,
// review, not sampled, no schema, ok.
func mappingStatus(r mappingRow) string {
	switch {
	case len(r.Warnings) > 0:
		return fmt.Sprintf("warnings (%d)", len(r.Warnings))
	case r.Review:
		return mapStatusReview
	case r.SpecOnly:
		return mapStatusNotSampled
	case r.Schema == "" && !r.HasSpec:
		return mapStatusNoSchema
	}
	return mapStatusOK
}

// mappingIsProblem reports whether a status belongs in the "problems" filter.
func mappingIsProblem(status string) bool {
	return strings.HasPrefix(status, "warnings") || status == mapStatusReview || status == mapStatusNoSchema
}

// mappingRows builds the screen rows from DB rows plus AsyncAPI topics that
// have no DB row yet. Sorted by topic.
func mappingRows(schemas map[string]database.TopicSchemaRow, specTopics []string, specFiles map[string]string) []mappingRow {
	rows := make([]mappingRow, 0, len(schemas)+len(specTopics))
	for _, s := range schemas {
		r := mappingRow{
			Topic:      s.Topic,
			Format:     s.Format,
			Schema:     s.MessageName,
			ResolvedBy: s.ResolvedBy,
			Warnings:   s.Warnings,
			HasSpec:    s.SpecFile != "",
		}
		if r.Schema == "" {
			if s.SpecMessage != "" {
				r.Schema = s.SpecMessage
			} else if s.SpecFile != "" {
				r.Schema = filepath.Base(s.SpecFile)
			}
		}
		if s.MessageOverride && r.ResolvedBy == "" {
			r.ResolvedBy = topicsample.ResolvedByOverride
		}
		rows = append(rows, r)
	}
	for _, t := range specTopics {
		if _, ok := schemas[t]; ok {
			continue
		}
		r := mappingRow{
			Topic:      t,
			Format:     "json (spec)",
			ResolvedBy: topicsample.ResolvedByAsyncAPI,
			HasSpec:    true,
			SpecOnly:   true,
		}
		if f := specFiles[t]; f != "" {
			r.Schema = filepath.Base(f)
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Topic < rows[j].Topic })
	return rows
}

type mappingSort int

const (
	mapSortTopic mappingSort = iota
	mapSortStatus
	mapSortResolved
)

func (s mappingSort) label() string {
	return [...]string{"topic", "status", "resolved-by"}[s]
}

type mappingFilter int

const (
	mapFilterAll mappingFilter = iota
	mapFilterProblems
	mapFilterOK
)

func (f mappingFilter) label() string {
	return [...]string{"all", "problems", "ok"}[f]
}

// openTopicMsg asks the root Model to open the topic detail screen; esc from
// the detail returns to the page in from.
type openTopicMsg struct {
	topic string
	from  page
}

type topicMappingLoadedMsg struct {
	rows []mappingRow
	err  error
}

type topicMappingModel struct {
	deps          Deps
	width, height int

	table    table.Model
	filter   textinput.Model
	filterOn bool
	status   mappingFilter
	sortBy   mappingSort

	loading bool
	all     []mappingRow
	err     error
}

func newTopicMappingModel(deps Deps) topicMappingModel {
	cols := []table.Column{
		{Title: "TOPIC", Width: 30},
		{Title: "FORMAT", Width: 12},
		{Title: "SCHEMA", Width: 30},
		{Title: "RESOLVED BY", Width: 14},
		{Title: "STATUS", Width: 14},
	}
	fi := textinput.New()
	fi.Placeholder = "filter by topic or schema"
	m := topicMappingModel{
		deps:   deps,
		table:  table.New(table.WithColumns(cols), table.WithFocused(true)),
		filter: fi,
	}
	if deps.DB == nil {
		m.err = fmt.Errorf("no database (DUCKDB_PATH is :memory:)")
	} else {
		m.loading = true
	}
	return m
}

func (m topicMappingModel) Init() tea.Cmd {
	if m.deps.DB == nil {
		return nil
	}
	return loadTopicMappingCmd(m.deps)
}

func loadTopicMappingCmd(deps Deps) tea.Cmd {
	return func() tea.Msg {
		if deps.DB == nil {
			return topicMappingLoadedMsg{err: fmt.Errorf("no database (DUCKDB_PATH is :memory:)")}
		}
		schemas, err := database.ReadTopicSchemas(context.Background(), deps.DB)
		if err != nil {
			return topicMappingLoadedMsg{err: err}
		}
		var topics []string
		files := map[string]string{}
		if deps.AsyncAPI != nil {
			for _, t := range deps.AsyncAPI.Topics() {
				topics = append(topics, t.Topic)
				files[t.Topic] = t.SpecFile
			}
		}
		return topicMappingLoadedMsg{rows: mappingRows(schemas, topics, files)}
	}
}

func (m topicMappingModel) resize(w, h int) topicMappingModel {
	m.width, m.height = w, h
	m.table.SetWidth(w)
	m.layoutColumns()
	th := h - 6
	if th < 1 {
		th = 1
	}
	m.table.SetHeight(th)
	return m
}

func (m topicMappingModel) filtering() bool { return m.filterOn }

// visible returns the filtered, sorted rows.
func (m topicMappingModel) visible() []mappingRow {
	q := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	out := make([]mappingRow, 0, len(m.all))
	for _, r := range m.all {
		if q != "" && !strings.Contains(strings.ToLower(r.Topic), q) && !strings.Contains(strings.ToLower(r.Schema), q) {
			continue
		}
		st := mappingStatus(r)
		switch m.status {
		case mapFilterProblems:
			if !mappingIsProblem(st) {
				continue
			}
		case mapFilterOK:
			if st != mapStatusOK {
				continue
			}
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		switch m.sortBy {
		case mapSortStatus:
			si, sj := mappingStatus(out[i]), mappingStatus(out[j])
			if si != sj {
				return si < sj
			}
		case mapSortResolved:
			if out[i].ResolvedBy != out[j].ResolvedBy {
				return out[i].ResolvedBy < out[j].ResolvedBy
			}
		}
		return out[i].Topic < out[j].Topic
	})
	return out
}

func (m *topicMappingModel) applyFilter() {
	vis := m.visible()
	rows := make([]table.Row, 0, len(vis))
	for _, r := range vis {
		rows = append(rows, table.Row{r.Topic, formatOrDash(r.Format), r.Schema, r.ResolvedBy, mappingStatus(r)})
	}
	m.table.SetRows(rows)
	m.layoutColumns()
}

// Fixed widths of the short columns (FORMAT, RESOLVED BY, STATUS), and the
// per-cell horizontal padding of the default table style.
const (
	mappingFormatW   = 12
	mappingResolvedW = 14
	mappingStatusW   = 14
	mappingCellPad   = 2
	mappingMinNameW  = 10
)

// layoutColumns sizes TOPIC and SCHEMA to the terminal width: they get
// whatever the fixed columns leave, each up to its longest value, with any
// shortfall split in proportion to need so neither is starved.
func (m *topicMappingModel) layoutColumns() {
	if m.width <= 0 {
		return
	}
	needTopic, needSchema := len("TOPIC"), len("SCHEMA")
	for _, r := range m.all {
		needTopic = max(needTopic, lipgloss.Width(r.Topic))
		needSchema = max(needSchema, lipgloss.Width(r.Schema))
	}
	fixed := mappingFormatW + mappingResolvedW + mappingStatusW + 5*mappingCellPad
	avail := max(m.width-fixed, 2*mappingMinNameW)

	topicW, schemaW := needTopic, needSchema
	if topicW+schemaW > avail {
		topicW = max(avail*needTopic/(needTopic+needSchema), mappingMinNameW)
		// Do not give a column more than it needs; pass the rest on.
		topicW = min(topicW, needTopic)
		schemaW = max(avail-topicW, mappingMinNameW)
		if schemaW > needSchema {
			schemaW = needSchema
			topicW = avail - schemaW
		}
	} else {
		// Spare room goes to SCHEMA so the row fills the width.
		schemaW = avail - topicW
	}
	m.table.SetColumns([]table.Column{
		{Title: "TOPIC", Width: topicW},
		{Title: "FORMAT", Width: mappingFormatW},
		{Title: "SCHEMA", Width: schemaW},
		{Title: "RESOLVED BY", Width: mappingResolvedW},
		{Title: "STATUS", Width: mappingStatusW},
	})
}

func (m topicMappingModel) selectedTopic() (string, bool) {
	row := m.table.SelectedRow()
	if len(row) == 0 {
		return "", false
	}
	return row[0], true
}

func (m topicMappingModel) summary() string {
	var ok, warn, noSchema, review, notSampled int
	for _, r := range m.all {
		st := mappingStatus(r)
		switch {
		case st == mapStatusOK:
			ok++
		case strings.HasPrefix(st, "warnings"):
			warn++
		case st == mapStatusNoSchema:
			noSchema++
		case st == mapStatusReview:
			review++
		case st == mapStatusNotSampled:
			notSampled++
		}
	}
	parts := []string{
		fmt.Sprintf("%d topics", len(m.all)),
		fmt.Sprintf("%d ok", ok),
		fmt.Sprintf("%d warnings", warn),
		fmt.Sprintf("%d no schema", noSchema),
	}
	if review > 0 {
		parts = append(parts, fmt.Sprintf("%d review", review))
	}
	if notSampled > 0 {
		parts = append(parts, fmt.Sprintf("%d not sampled", notSampled))
	}
	return strings.Join(parts, " · ")
}

func (m topicMappingModel) update(msg tea.Msg) (topicMappingModel, tea.Cmd) {
	switch msg := msg.(type) {
	case topicMappingLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.err = nil
		m.all = msg.rows
		m.applyFilter()
		return m, setStatus(m.summary())
	case tea.KeyPressMsg:
		if m.filterOn {
			switch msg.String() {
			case "esc", "enter":
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
		case "/":
			m.filterOn = true
			return m, m.filter.Focus()
		case "tab":
			m.status = (m.status + 1) % 3
			m.applyFilter()
			return m, nil
		case "s":
			m.sortBy = (m.sortBy + 1) % 3
			m.applyFilter()
			return m, nil
		case "r", "ctrl+r":
			if m.deps.DB != nil && !m.loading {
				m.loading = true
				return m, tea.Batch(loadTopicMappingCmd(m.deps), setStatus("refreshing topic mapping..."))
			}
			return m, nil
		case "enter":
			if topic, ok := m.selectedTopic(); ok {
				return m, func() tea.Msg { return openTopicMsg{topic: topic, from: pageTopicMapping} }
			}
			return m, nil
		}
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func mappingStatusStyle(st string) lipgloss.Style {
	switch {
	case st == mapStatusOK:
		return tailRunningStyle
	case strings.HasPrefix(st, "warnings"), st == mapStatusReview:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Bold(true)
	case st == mapStatusNoSchema:
		return lagHighStyle
	}
	return helpStyle
}

func (m topicMappingModel) View() string {
	var b strings.Builder
	b.WriteString(menuHeadingStyle.Render("Topic mapping"))
	b.WriteString("\n")
	if m.deps.DB == nil {
		b.WriteString(errorStyle.Render(m.err.Error()))
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("esc back"))
		return b.String()
	}
	if m.err != nil {
		b.WriteString(errorStyle.Render(m.err.Error()))
		b.WriteString("\n")
	}
	if m.loading {
		b.WriteString("loading topic mapping...\n")
	} else {
		fmt.Fprintf(&b, "%s  [filter: %s · sort: %s]\n", m.summary(), m.status.label(), m.sortBy.label())
	}
	if m.filterOn || m.filter.Value() != "" {
		b.WriteString(m.filter.View())
		b.WriteString("\n")
	}
	b.WriteString(m.table.View())
	if sel := m.table.SelectedRow(); len(sel) == 5 {
		b.WriteString("\n")
		b.WriteString(mappingStatusStyle(sel[4]).Render(sel[0] + " — " + sel[4]))
	}
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓ navigate · / filter · tab status filter · s sort · enter open · r refresh · esc back"))
	return b.String()
}

func (m Model) updateTopicMapping(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && !m.topicMapping.filtering() && key.String() == "esc" {
		m.page = pageMenu
		return m, nil
	}
	var cmd tea.Cmd
	m.topicMapping, cmd = m.topicMapping.update(msg)
	return m, cmd
}

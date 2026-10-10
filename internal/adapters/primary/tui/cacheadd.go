package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
)

type wizardStep int

const (
	stepSelectTopic wizardStep = iota
	stepStartOffset
	stepLimitType
	stepLimitValue
	stepReview
)

type cacheAddModel struct {
	deps Deps
	step wizardStep

	topicTable table.Model
	all        []eligibleTopic // every eligible topic, sorted by name
	visible    []eligibleTopic // all, narrowed by the filter (table rows)
	filter     textinput.Model
	filterOn   bool

	selectedTopic cacheTopic
	topicName     string

	startFrom  string // "beginning" or "now"
	limitType  string // "indefinitely", "duration", "count"
	limitInput textinput.Model
	limitValue string

	estBytes int64

	width, height int
	err           error
}

func newCacheAddModel(deps Deps) cacheAddModel {
	ti := textinput.New()
	ti.Placeholder = "e.g. 2h, 1d, 50000"

	fi := textinput.New()
	fi.Placeholder = "filter by topic or schema"

	cols := []table.Column{
		{Title: "TOPIC", Width: 32},
		{Title: "FORMAT", Width: 10},
		{Title: "MESSAGES", Width: 12},
		{Title: "EST. SIZE", Width: 12},
		{Title: "SCHEMA", Width: 36},
	}
	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
	)

	return cacheAddModel{
		deps:       deps,
		step:       stepSelectTopic,
		topicTable: t,
		filter:     fi,
		limitInput: ti,
		startFrom:  "beginning",
		limitType:  "indefinitely",
	}
}

// cacheTopic is the schema a topic is cached with.
type cacheTopic struct {
	Format              string
	MessageName         string
	ProtoFile           string
	ImportRoot          string
	CutConfluencePrefix bool
}

// eligibleTopic is one sampled topic that can be cached (JSON, or
// protobuf with a resolved message), with its stored size estimate.
type eligibleTopic struct {
	name     string
	topic    cacheTopic
	messages int64
	estimate int64
}

func (m cacheAddModel) Init() tea.Cmd {
	deps := m.deps
	return func() tea.Msg {
		var estimates map[string]database.EstimateRow
		if deps.DB != nil {
			if rows, err := database.ReadEstimateTable(context.Background(), deps.DB); err == nil {
				estimates = make(map[string]database.EstimateRow, len(rows))
				for _, r := range rows {
					estimates[r.Topic] = r
				}
			}
		}

		var eligible []eligibleTopic
		if deps.DB != nil {
			schemas, _ := database.ReadTopicSchemas(context.Background(), deps.DB)
			if sums, err := database.ReadAllSampleSummaries(context.Background(), deps.DB); err == nil {
				for name, s := range sums {
					if s.Format != "json" && !(s.Format == "protobuf" && s.MessageName != "") {
						continue
					}
					t := cacheTopic{
						Format:              s.Format,
						MessageName:         s.MessageName,
						CutConfluencePrefix: s.CutConfluencePrefix,
					}
					if sc, ok := schemas[name]; ok {
						t.ProtoFile = sc.ProtoFile
						t.ImportRoot = sc.ImportRoot
					}
					e := eligibleTopic{name: name, topic: t}
					if est, ok := estimates[name]; ok {
						e.messages = est.TotalMessages
						e.estimate = est.EstimatedBytes
					}
					eligible = append(eligible, e)
				}
			}
		}
		sort.Slice(eligible, func(i, j int) bool { return eligible[i].name < eligible[j].name })
		return eligibleTopicsMsg{eligible: eligible}
	}
}

type eligibleTopicsMsg struct {
	eligible []eligibleTopic
}

// applyFilter narrows m.all to the topics whose name or schema contains the
// filter text and refreshes the table rows.
func (m *cacheAddModel) applyFilter() {
	q := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	m.visible = m.visible[:0:0]
	rows := make([]table.Row, 0, len(m.all))
	for _, e := range m.all {
		if q != "" && !strings.Contains(strings.ToLower(e.name), q) &&
			!strings.Contains(strings.ToLower(e.topic.MessageName), q) {
			continue
		}
		m.visible = append(m.visible, e)
		rows = append(rows, table.Row{
			e.name,
			e.topic.Format,
			countOrDash(e.messages),
			sizeOrDash(e.estimate),
			formatOrDash(e.topic.MessageName),
		})
	}
	m.topicTable.SetRows(rows)
	if c := m.topicTable.Cursor(); c >= len(rows) || c < 0 {
		m.topicTable.SetCursor(0)
	}
}

type cacheTopicAddedMsg struct {
	topic string
}

// fixedCacheAddWidth is the fixed width allocated to FORMAT, MESSAGES,
// EST. SIZE and table borders/padding.
const fixedCacheAddWidth = 10 + 12 + 12 + 10

func (m cacheAddModel) resize(w, h int) cacheAddModel {
	m.width, m.height = w, h
	m.topicTable.SetWidth(w)
	th := h - 7
	if th < 1 {
		th = 1
	}
	m.topicTable.SetHeight(th)

	cols := m.topicTable.Columns()
	if len(cols) == 5 && w > fixedCacheAddWidth+20 {
		remaining := w - fixedCacheAddWidth
		// Give ~45% to TOPIC and ~55% to SCHEMA
		cols[0].Width = remaining * 45 / 100
		cols[4].Width = remaining - cols[0].Width
		m.topicTable.SetColumns(cols)
	}
	return m
}

func (m cacheAddModel) update(ctx context.Context, msg tea.Msg) (cacheAddModel, tea.Cmd) {
	switch msg := msg.(type) {
	case eligibleTopicsMsg:
		m.all = msg.eligible
		m.applyFilter()
		return m, nil

	case tea.KeyPressMsg:
		if m.step == stepSelectTopic && m.filterOn {
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
			if m.step == stepSelectTopic {
				m.filterOn = true
				return m, m.filter.Focus()
			}
		case "esc":
			if m.step == stepSelectTopic && m.filter.Value() != "" {
				m.filter.SetValue("")
				m.applyFilter()
				return m, nil
			}
			if m.step > stepSelectTopic {
				m.step--
				return m, nil
			}
			return m, func() tea.Msg { return menuSelectedMsg{target: menuTargetKafkaCache} }

		case "enter":
			switch m.step {
			case stepSelectTopic:
				idx := m.topicTable.Cursor()
				if idx >= 0 && idx < len(m.visible) {
					e := m.visible[idx]
					m.selectedTopic = e.topic
					m.topicName = e.name
					m.estBytes = e.estimate
					m.step = stepStartOffset
					return m, nil
				}

			case stepStartOffset:
				m.step = stepLimitType
				return m, nil

			case stepLimitType:
				if m.limitType == "indefinitely" {
					m.step = stepReview
					return m, nil
				}
				m.step = stepLimitValue
				return m, m.limitInput.Focus()

			case stepLimitValue:
				m.limitValue = strings.TrimSpace(m.limitInput.Value())
				if m.limitValue != "" {
					m.step = stepReview
					m.limitInput.Blur()
					return m, nil
				}

			case stepReview:
				// Finalize and save
				row := database.CacheConfigRow{
					Topic:               m.topicName,
					Format:              m.selectedTopic.Format,
					MessageName:         m.selectedTopic.MessageName,
					ProtoFile:           m.selectedTopic.ProtoFile,
					ImportRoot:          m.selectedTopic.ImportRoot,
					CutConfluencePrefix: m.selectedTopic.CutConfluencePrefix,
					StartFrom:           m.startFrom,
					LimitType:           m.limitType,
					LimitValue:          m.limitValue,
					Active:              true,
					EstimatedSize:       m.estBytes,
					CreatedAt:           time.Now(),
				}
				topic := m.topicName
				return m, func() tea.Msg {
					if m.deps.DB != nil {
						if err := database.UpsertCacheConfig(context.Background(), m.deps.DB, row); err != nil {
							return errMsg{err: fmt.Errorf("save cache config: %w", err)}
						}
					}
					if m.deps.SyncManager != nil {
						_ = m.deps.SyncManager.Start(context.Background(), topic)
					}
					return cacheTopicAddedMsg{topic: topic}
				}
			}

		case "1":
			if m.step == stepStartOffset {
				m.startFrom = "beginning"
			} else if m.step == stepLimitType {
				m.limitType = "indefinitely"
			}
		case "2":
			if m.step == stepStartOffset {
				m.startFrom = "now"
			} else if m.step == stepLimitType {
				m.limitType = "duration"
			}
		case "3":
			if m.step == stepLimitType {
				m.limitType = "count"
			}
		}
	}

	if m.step == stepSelectTopic {
		var cmd tea.Cmd
		m.topicTable, cmd = m.topicTable.Update(msg)
		return m, cmd
	} else if m.step == stepLimitValue {
		var cmd tea.Cmd
		m.limitInput, cmd = m.limitInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m cacheAddModel) View() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Add Topic to Cache"))
	b.WriteString("\n\n")

	switch m.step {
	case stepSelectTopic:
		fmt.Fprintf(&b, "Select an eligible topic (JSON or resolved Protobuf) — %d/%d shown:\n\n", len(m.visible), len(m.all))
		if m.filterOn || m.filter.Value() != "" {
			b.WriteString(m.filter.View())
			b.WriteString("\n")
		}
		b.WriteString(m.topicTable.View())
		b.WriteString("\n" + helpStyle.Render("↑/↓ navigate · / filter · enter select · esc cancel"))

	case stepStartOffset:
		b.WriteString(fmt.Sprintf("Topic: %s\n\n", m.topicName))
		b.WriteString("Choose consumption start point:\n")
		sel1, sel2 := "[ ]", "[ ]"
		if m.startFrom == "beginning" {
			sel1 = "[*]"
		} else {
			sel2 = "[*]"
		}
		fmt.Fprintf(&b, "  1. %s Consume entire topic from beginning\n", sel1)
		fmt.Fprintf(&b, "  2. %s Starting from now (new messages only)\n\n", sel2)
		b.WriteString(helpStyle.Render("Press 1 or 2 · enter continue · esc back"))

	case stepLimitType:
		b.WriteString(fmt.Sprintf("Topic: %s\n\n", m.topicName))
		b.WriteString("Choose caching limit mode:\n")
		sel1, sel2, sel3 := "[ ]", "[ ]", "[ ]"
		switch m.limitType {
		case "indefinitely":
			sel1 = "[*]"
		case "duration":
			sel2 = "[*]"
		case "count":
			sel3 = "[*]"
		}
		fmt.Fprintf(&b, "  1. %s Sync indefinitely\n", sel1)
		fmt.Fprintf(&b, "  2. %s Sync for a duration (e.g. 2h, 30s, 1d, 2d13h56s)\n", sel2)
		fmt.Fprintf(&b, "  3. %s Sync for a set number of messages\n\n", sel3)
		b.WriteString(helpStyle.Render("Pausing and resuming (p) a duration/count sync re-runs it: another\nduration/count of messages, from the topic end when starting from now."))
		b.WriteString("\n")
		b.WriteString(helpStyle.Render("Press 1, 2, or 3 · enter continue · esc back"))

	case stepLimitValue:
		b.WriteString(fmt.Sprintf("Topic: %s\n\n", m.topicName))
		if m.limitType == "duration" {
			b.WriteString("Enter duration (e.g. 30s, 2h, 1d, 2d13h56s):\n\n")
		} else {
			b.WriteString("Enter message count (e.g. 50000):\n\n")
		}
		b.WriteString(m.limitInput.View())
		b.WriteString("\n\n" + helpStyle.Render("enter continue · esc back"))

	case stepReview:
		b.WriteString(fmt.Sprintf("Topic:          %s\n", m.topicName))
		b.WriteString(fmt.Sprintf("Format:         %s\n", m.selectedTopic.Format))
		if m.selectedTopic.MessageName != "" {
			b.WriteString(fmt.Sprintf("Schema:         %s\n", m.selectedTopic.MessageName))
		}
		b.WriteString(fmt.Sprintf("Start:          %s\n", m.startFrom))
		b.WriteString(fmt.Sprintf("Limit:          %s %s\n", m.limitType, m.limitValue))

		estStr := "unknown"
		if m.estBytes > 0 {
			estStr = humanBytes(m.estBytes)
		}
		b.WriteString(fmt.Sprintf("Estimated Size: %s\n\n", estStr))

		const limit1_5GB = int64(1.5 * 1024 * 1024 * 1024)
		if m.estBytes > limit1_5GB {
			b.WriteString(errorStyle.Render(fmt.Sprintf("⚠️  WARNING: Estimated cache storage (%s) exceeds 1.5 GB!", humanBytes(m.estBytes))))
			b.WriteString("\n\n")
		}

		b.WriteString("Press enter to finalize and begin caching, or esc to back.")
	}

	return b.String()
}

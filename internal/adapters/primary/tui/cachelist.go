package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
)

type cacheRow struct {
	topic         string
	active        bool
	cachedMsgs    int64
	format        string
	messageName   string
	estimatedDisk int64
	lastError     string
}

type cacheListModel struct {
	deps Deps

	table   table.Model
	spinner spinner.Model
	filter  textinput.Model

	filterOn bool
	loading  bool
	all      []cacheRow
	err      error

	confirmDrop   bool
	topicToDrop   string
	width, height int
}

func newCacheListModel(deps Deps) cacheListModel {
	cols := []table.Column{
		{Title: "TOPIC", Width: 26},
		{Title: "STATUS", Width: 10},
		{Title: "CACHED MSGS", Width: 14},
		{Title: "FORMAT", Width: 10},
		{Title: "SCHEMA", Width: 26},
		{Title: "EST. DISK", Width: 12},
	}
	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
	)
	s := spinner.New()
	ti := textinput.New()
	ti.Placeholder = "filter cached topics..."

	return cacheListModel{
		deps:    deps,
		table:   t,
		spinner: s,
		filter:  ti,
		loading: true,
	}
}

type cacheRowsLoadedMsg struct {
	rows []cacheRow
	err  error
}

func loadCacheRowsCmd(ctx context.Context, deps Deps) tea.Cmd {
	return func() tea.Msg {
		if deps.DB == nil {
			return cacheRowsLoadedMsg{err: fmt.Errorf("duckdb not configured")}
		}
		configs, err := database.ReadCacheConfigs(ctx, deps.DB)
		if err != nil {
			return cacheRowsLoadedMsg{err: err}
		}

		var rows []cacheRow
		for _, c := range configs {
			active := false
			lastErr := ""
			if deps.SyncManager != nil {
				active = deps.SyncManager.IsActive(c.Topic)
				lastErr = deps.SyncManager.Error(c.Topic)
			}
			tbl, _ := kafkaingest.TableName(c.Topic)
			cnt, disk, _ := database.GetTableStats(ctx, deps.DB, tbl)

			rows = append(rows, cacheRow{
				topic:         c.Topic,
				active:        active,
				cachedMsgs:    cnt,
				format:        c.Format,
				messageName:   c.MessageName,
				estimatedDisk: disk,
				lastError:     lastErr,
			})
		}
		return cacheRowsLoadedMsg{rows: rows}
	}
}

type cacheTickMsg time.Time

func cacheAutoRefreshCmd() tea.Cmd {
	return tea.Tick(5*time.Second, func(t time.Time) tea.Msg {
		return cacheTickMsg(t)
	})
}

func (m cacheListModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, loadCacheRowsCmd(context.Background(), m.deps), cacheAutoRefreshCmd())
}

// fixedCacheListWidth is the combined width of STATUS, CACHED MSGS, FORMAT, EST. DISK, and borders.
const fixedCacheListWidth = 10 + 14 + 10 + 12 + 10

func (m cacheListModel) resize(w, h int) (cacheListModel, tea.Cmd) {
	m.width, m.height = w, h
	th := h - 4
	if th < 1 {
		th = 1
	}
	m.table.SetWidth(w)
	m.table.SetHeight(th)

	cols := m.table.Columns()
	if len(cols) == 6 && w > fixedCacheListWidth+20 {
		remaining := w - fixedCacheListWidth
		// Share remaining width between TOPIC (45%) and SCHEMA (55%)
		cols[0].Width = remaining * 45 / 100
		cols[4].Width = remaining - cols[0].Width
		m.table.SetColumns(cols)
	}
	return m, nil
}

func (m *cacheListModel) applyFilter() {
	term := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	var filtered []table.Row
	for _, r := range m.all {
		if term != "" && !strings.Contains(strings.ToLower(r.topic), term) {
			continue
		}
		status := "PAUSED"
		if r.lastError != "" {
			status = "ERROR"
		} else if r.active {
			status = "ACTIVE"
		}
		schemaDisplay := r.messageName
		if schemaDisplay == "" {
			schemaDisplay = "-"
		}
		filtered = append(filtered, table.Row{
			r.topic,
			status,
			fmt.Sprintf("%d", r.cachedMsgs),
			r.format,
			schemaDisplay,
			humanBytes(r.estimatedDisk),
		})
	}
	m.table.SetRows(filtered)
}

func (m cacheListModel) selectedTopic() (string, bool) {
	row := m.table.SelectedRow()
	if len(row) == 0 {
		return "", false
	}
	return row[0], true
}

func (m cacheListModel) update(ctx context.Context, msg tea.Msg) (cacheListModel, tea.Cmd) {
	switch msg := msg.(type) {
	case cacheRowsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, setErr(msg.err)
		}
		m.all = msg.rows
		m.applyFilter()
		return m, setStatus(fmt.Sprintf("%d cached topics", len(m.all)))

	case cacheTickMsg:
		// Auto-refresh cache table row stats without triggering loading spinner
		return m, tea.Batch(loadCacheRowsCmd(ctx, m.deps), cacheAutoRefreshCmd())

	case spinner.TickMsg:
		if m.loading {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil

	case tea.KeyPressMsg:
		if m.confirmDrop {
			switch msg.String() {
			case "y", "Y":
				topic := m.topicToDrop
				m.confirmDrop = false
				m.topicToDrop = ""
				return m, func() tea.Msg {
					if m.deps.SyncManager != nil {
						_ = m.deps.SyncManager.Stop(topic)
					}
					tbl, _ := kafkaingest.TableName(topic)
					_ = database.DropTable(ctx, m.deps.DB, tbl)
					_ = database.CompactDB(ctx, m.deps.DB)
					_ = database.DeleteCacheConfig(ctx, m.deps.DB, topic)
					_ = database.DeleteCacheOffsets(ctx, m.deps.DB, topic)
					return loadCacheRowsCmd(ctx, m.deps)()
				}
			case "n", "N", "esc":
				m.confirmDrop = false
				m.topicToDrop = ""
				return m, setStatus("deletion canceled")
			}
			return m, nil
		}

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
		case "/":
			m.filterOn = true
			return m, m.filter.Focus()
		case "esc":
			if m.filter.Value() != "" {
				m.filter.SetValue("")
				m.applyFilter()
				return m, nil
			}
		case "r", "R":
			m.loading = true
			return m, tea.Batch(m.spinner.Tick, loadCacheRowsCmd(ctx, m.deps), setStatus("refreshing cache..."))
		case "d", "x":
			if topic, ok := m.selectedTopic(); ok {
				m.confirmDrop = true
				m.topicToDrop = topic
				return m, nil
			}
		case "p", " ":
			if topic, ok := m.selectedTopic(); ok && m.deps.SyncManager != nil {
				if m.deps.SyncManager.IsActive(topic) {
					_ = m.deps.SyncManager.Stop(topic)
					_ = database.UpsertCacheConfig(ctx, m.deps.DB, database.CacheConfigRow{
						Topic:  topic,
						Active: false,
					})
				} else {
					if err := m.deps.SyncManager.Start(ctx, topic); err != nil {
						return m, setStatus(fmt.Sprintf("resume %s: %v", topic, err))
					}
					_ = database.UpsertCacheConfig(ctx, m.deps.DB, database.CacheConfigRow{
						Topic:  topic,
						Active: true,
					})
				}
				return m, tea.Batch(loadCacheRowsCmd(ctx, m.deps), setStatus("updated sync state"))
			}
		}
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m cacheListModel) View() string {
	var b strings.Builder
	if m.confirmDrop {
		b.WriteString(errorStyle.Render(fmt.Sprintf("Are you sure? Please confirm dropping cache for %q and reclaiming disk space (y/n): ", m.topicToDrop)))
		b.WriteString("\n\n")
	}
	if m.filterOn || m.filter.Value() != "" {
		b.WriteString(m.filter.View())
		b.WriteString("\n")
	}
	if m.loading {
		b.WriteString(m.spinner.View())
		b.WriteString(" loading cache status...\n")
	}
	b.WriteString(m.table.View())
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓ navigate · / filter · a add · d/x drop · p pause/resume (re-runs duration/count syncs) · enter open · r refresh · esc back"))
	return b.String()
}

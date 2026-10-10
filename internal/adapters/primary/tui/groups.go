package tui

import (
	"context"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
)

type groupsModel struct {
	deps    Deps
	table   table.Model
	spinner spinner.Model
	loading bool
}

type groupsLoadedMsg struct {
	groups []kafka.ConsumerGroup
	err    error
}

func newGroupsModel(deps Deps) groupsModel {
	cols := []table.Column{
		{Title: "GROUP", Width: 40},
		{Title: "STATE", Width: 12},
		{Title: "MEMBERS", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true))
	return groupsModel{deps: deps, table: t, spinner: spinner.New(), loading: true}
}

func (m groupsModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, loadGroupsCmd(m.deps))
}

func loadGroupsCmd(deps Deps) tea.Cmd {
	return func() tea.Msg {
		groups, err := kafka.ListConsumerGroups(context.Background(), deps.KafkaConf)
		return groupsLoadedMsg{groups: groups, err: err}
	}
}

// fixedGroupsWidth is the combined width of STATE, MEMBERS, and table borders.
const fixedGroupsWidth = 12 + 10 + 6

func (m groupsModel) resize(w, h int) (groupsModel, tea.Cmd) {
	m.table.SetWidth(w)
	m.table.SetHeight(h)
	cols := m.table.Columns()
	if len(cols) == 3 && w > fixedGroupsWidth+10 {
		cols[0].Width = w - fixedGroupsWidth
		m.table.SetColumns(cols)
	}
	return m, nil
}

func (m groupsModel) selectedGroup() (string, bool) {
	row := m.table.SelectedRow()
	if len(row) == 0 {
		return "", false
	}
	return row[0], true
}

func (m groupsModel) update(ctx context.Context, msg tea.Msg) (groupsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case groupsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		rows := make([]table.Row, 0, len(msg.groups))
		for _, g := range msg.groups {
			rows = append(rows, table.Row{g.Name, g.State, fmt.Sprintf("%d", g.Members)})
		}
		m.table.SetRows(rows)
		return m, setStatus(fmt.Sprintf("%d consumer groups", len(msg.groups)))

	case spinner.TickMsg:
		if m.loading {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m groupsModel) View() string {
	var b strings.Builder
	if m.loading {
		b.WriteString(m.spinner.View())
		b.WriteString(" loading consumer groups...\n")
	}
	b.WriteString(m.table.View())
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓ navigate · enter open · esc back"))
	return b.String()
}

// groupDetailModel shows one consumer group's per-partition lag.
type groupDetailModel struct {
	deps    Deps
	group   string
	table   table.Model
	spinner spinner.Model
	loading bool
}

type groupDetailLoadedMsg struct {
	desc kafka.ConsumerGroupDescription
	err  error
}

func newGroupDetailModel(deps Deps, group string) groupDetailModel {
	cols := []table.Column{
		{Title: "TOPIC", Width: 30},
		{Title: "PARTITION", Width: 10},
		{Title: "COMMITTED", Width: 14},
		{Title: "END", Width: 14},
		{Title: "LAG", Width: 12},
		{Title: "CLIENT", Width: 20},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true))
	return groupDetailModel{deps: deps, group: group, table: t, spinner: spinner.New(), loading: true}
}

func (m groupDetailModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, loadGroupDetailCmd(m.deps, m.group))
}

func loadGroupDetailCmd(deps Deps, group string) tea.Cmd {
	return func() tea.Msg {
		desc, err := kafka.DescribeConsumerGroup(context.Background(), deps.KafkaConf, group)
		return groupDetailLoadedMsg{desc: desc, err: err}
	}
}

// fixedGroupDetailWidth is the combined width of every groupDetailModel
// column except TOPIC and CLIENT, which share whatever space remains.
const fixedGroupDetailWidth = 10 + 14 + 14 + 12 + 8

func (m groupDetailModel) resize(w, h int) (groupDetailModel, tea.Cmd) {
	m.table.SetWidth(w)
	m.table.SetHeight(h)
	cols := m.table.Columns()
	if len(cols) == 6 && w > fixedGroupDetailWidth+10 {
		remaining := w - fixedGroupDetailWidth
		cols[0].Width = remaining * 3 / 5
		cols[5].Width = remaining - cols[0].Width
		m.table.SetColumns(cols)
	}
	return m, nil
}

func (m groupDetailModel) update(ctx context.Context, msg tea.Msg) (groupDetailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case groupDetailLoadedMsg:
		m.loading = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		rows := make([]table.Row, 0, len(msg.desc.Partitions))
		var totalLag int64
		for _, p := range msg.desc.Partitions {
			lag := "-"
			if p.Lag >= 0 {
				lag = fmt.Sprintf("%d", p.Lag)
				totalLag += p.Lag
			}
			rows = append(rows, table.Row{
				p.Topic, fmt.Sprintf("%d", p.Partition),
				fmt.Sprintf("%d", p.Committed), fmt.Sprintf("%d", p.End), lag, p.ClientID,
			})
		}
		m.table.SetRows(rows)
		return m, setStatus(fmt.Sprintf("group %s: state=%s members=%d total lag=%d",
			msg.desc.Name, msg.desc.State, msg.desc.Members, totalLag))

	case spinner.TickMsg:
		if m.loading {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m groupDetailModel) View() string {
	var b strings.Builder
	if m.loading {
		b.WriteString(m.spinner.View())
		b.WriteString(" loading group detail...\n")
	}
	b.WriteString(m.table.View())
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓ navigate · esc back"))
	return b.String()
}

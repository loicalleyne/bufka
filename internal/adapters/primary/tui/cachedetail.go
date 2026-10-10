package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
)

type cacheDetailTab int

const (
	cacheTabMetadata cacheDetailTab = iota
	cacheTabSchema
	cacheTabPreview
	cacheTabLogs
	cacheTabCount
)

func (t cacheDetailTab) String() string {
	switch t {
	case cacheTabMetadata:
		return "Metadata"
	case cacheTabSchema:
		return "Schema"
	case cacheTabPreview:
		return "Preview"
	case cacheTabLogs:
		return "Logs & Errors"
	}
	return ""
}

type cacheDetailModel struct {
	deps  Deps
	topic string
	tab   cacheDetailTab

	vp     viewport.Model
	width  int
	height int

	config *database.CacheConfigRow
	rows   []string
	err    error
}

func newCacheDetailModel(deps Deps, topic string) cacheDetailModel {
	return cacheDetailModel{
		deps:  deps,
		topic: topic,
		tab:   cacheTabMetadata,
		vp:    viewport.New(viewport.WithWidth(80), viewport.WithHeight(20)),
	}
}

type cacheDetailLoadedMsg struct {
	config *database.CacheConfigRow
	rows   []string
	err    error
}

func (m cacheDetailModel) Init() tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		if m.deps.DB == nil {
			return cacheDetailLoadedMsg{err: fmt.Errorf("duckdb is nil")}
		}

		configs, err := database.ReadCacheConfigs(ctx, m.deps.DB)
		if err != nil {
			return cacheDetailLoadedMsg{err: err}
		}
		var cfg *database.CacheConfigRow
		for _, c := range configs {
			if c.Topic == m.topic {
				cfg = &c
				break
			}
		}

		// Preview rows
		var rows []string
		tbl, _ := kafkaingest.TableName(m.topic)
		conn, err := m.deps.DB.Connect()
		if err == nil {
			defer func() { _ = conn.Close() }()
			schema, _ := conn.GetTableSchema(ctx, nil, nil, tbl)
			if schema != nil {
				res, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %q LIMIT 30", tbl))
				if err == nil {
					defer res.Close()
					for res.Reader.Next() {
						rec := res.Reader.RecordBatch()
						for i := 0; i < int(rec.NumRows()); i++ {
							var parts []string
							for c := 0; c < int(rec.NumCols()); c++ {
								col := rec.Column(c)
								colName := rec.Schema().Field(c).Name
								valStr := formatColumnValue(col, i)
								parts = append(parts, fmt.Sprintf("%s: %s", colName, valStr))
							}
							rows = append(rows, strings.Join(parts, " | "))
						}
					}
				}
			}
		}

		return cacheDetailLoadedMsg{config: cfg, rows: rows}
	}
}

func (m cacheDetailModel) resize(w, h int) cacheDetailModel {
	m.width, m.height = w, h
	m.vp = resizedViewport(m.vp, w, h-4)
	m.renderTab()
	return m
}

func (m *cacheDetailModel) renderTab() {
	var b strings.Builder
	switch m.tab {
	case cacheTabMetadata:
		tbl, _ := kafkaingest.TableName(m.topic)
		cnt, disk, _ := database.GetTableStats(context.Background(), m.deps.DB, tbl)
		b.WriteString(fmt.Sprintf("Topic:          %s\n", m.topic))
		b.WriteString(fmt.Sprintf("DuckDB Table:   %s\n", tbl))
		b.WriteString(fmt.Sprintf("Cached Records: %d\n", cnt))
		b.WriteString(fmt.Sprintf("Disk Size:      %s\n", humanBytes(disk)))
		if m.config != nil {
			b.WriteString(fmt.Sprintf("Format:         %s\n", m.config.Format))
			b.WriteString(fmt.Sprintf("Start Offset:   %s\n", m.config.StartFrom))
			b.WriteString(fmt.Sprintf("Limit:          %s %s\n", m.config.LimitType, m.config.LimitValue))
			b.WriteString(fmt.Sprintf("Config Active:  %v\n", m.config.Active))
		}

	case cacheTabSchema:
		if m.config != nil && m.config.Format == "protobuf" {
			b.WriteString(fmt.Sprintf("Message Name:   %s\n", m.config.MessageName))
			b.WriteString(fmt.Sprintf("Proto File:     %s\n", m.config.ProtoFile))
			b.WriteString(fmt.Sprintf("Import Root:    %s\n\n", m.config.ImportRoot))
			body, errStr := protoSchemaBody(m.deps, m.config.MessageName)
			if errStr != "" {
				b.WriteString(errorStyle.Render(errStr))
			} else {
				b.WriteString(protoSchemaHeaderStyle.Render(m.config.MessageName))
				b.WriteString("\n")
				b.WriteString(body)
			}
		} else {
			b.WriteString("Format: Raw JSON\n")
			b.WriteString("Columns:\n")
			b.WriteString("  - _kafka_partition: Int32\n")
			b.WriteString("  - _kafka_offset:    Int64\n")
			b.WriteString("  - _kafka_timestamp: Timestamp_us\n")
			b.WriteString("  - _kafka_key:       Binary\n")
			b.WriteString("  - value:            String (Raw JSON)\n")
		}

	case cacheTabPreview:
		if len(m.rows) == 0 {
			b.WriteString("No cached records in table yet.")
		} else {
			// Wrap each record to the viewport width so long rows are
			// readable instead of being cut off at the right edge.
			wrap := lipgloss.NewStyle()
			if w := m.vp.Width(); w > 0 {
				wrap = wrap.Width(w)
			}
			for i, r := range m.rows {
				if i > 0 {
					b.WriteString("\n")
				}
				b.WriteString(wrap.Render(r))
				b.WriteString("\n")
			}
		}

	case cacheTabLogs:
		if m.deps.SyncManager == nil {
			b.WriteString("Sync manager not available.")
		} else {
			lastErr := m.deps.SyncManager.Error(m.topic)
			if lastErr != "" {
				b.WriteString(errorStyle.Render("CURRENT ERROR: " + lastErr))
				b.WriteString("\n\n")
			}
			logs := m.deps.SyncManager.Logs(m.topic)
			if len(logs) == 0 {
				b.WriteString("No logs recorded for this topic.")
			} else {
				for _, l := range logs {
					b.WriteString(l)
					b.WriteString("\n")
				}
			}
		}
	}

	m.vp.SetContent(b.String())
}

func (m cacheDetailModel) update(msg tea.Msg) (cacheDetailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case cacheDetailLoadedMsg:
		m.config = msg.config
		m.rows = msg.rows
		m.renderTab()
		return m, nil

	case tea.KeyPressMsg:
		switch msg.String() {
		case "tab", "l":
			m.tab = (m.tab + 1) % cacheTabCount
			m.renderTab()
			return m, nil
		case "shift+tab", "h":
			m.tab = (m.tab - 1 + cacheTabCount) % cacheTabCount
			m.renderTab()
			return m, nil
		}
	}

	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

func (m cacheDetailModel) View() string {
	var tabs []string
	for i := 0; i < int(cacheTabCount); i++ {
		t := cacheDetailTab(i)
		if t == m.tab {
			tabs = append(tabs, tabActiveStyle.Render(t.String()))
		} else {
			tabs = append(tabs, tabInactiveStyle.Render(t.String()))
		}
	}
	header := strings.Join(tabs, " ")
	return fmt.Sprintf("%s\n\n%s\n\n%s", header, m.vp.View(), helpStyle.Render("tab/shift+tab switch tab · ↑/↓ scroll · esc back"))
}

func formatColumnValue(col arrow.Array, i int) string {
	if col.IsNull(i) {
		return "null"
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(i)
	case *array.Int32:
		return fmt.Sprintf("%d", c.Value(i))
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(i))
	case *array.Uint32:
		return fmt.Sprintf("%d", c.Value(i))
	case *array.Uint64:
		return fmt.Sprintf("%d", c.Value(i))
	case *array.Float32:
		return fmt.Sprintf("%g", c.Value(i))
	case *array.Float64:
		return fmt.Sprintf("%g", c.Value(i))
	case *array.Boolean:
		return fmt.Sprintf("%v", c.Value(i))
	case *array.Timestamp:
		return c.Value(i).ToTime(arrow.Microsecond).Format(time.RFC3339)
	case *array.Binary:
		return fmt.Sprintf("<%d bytes>", len(c.Value(i)))
	default:
		// Fallback for complex structures
		return col.ValueStr(i)
	}
}

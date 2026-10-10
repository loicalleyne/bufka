package tui

import (
	"context"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/schemafield"
)

const (
	schemaNoneMsg        = "No schema known for this topic. Sample the topic or configure schema sources (settings > schema sources)."
	schemaValueMaxLines  = 3
	schemaCommentMaxRows = 3
)

var schemaWarnStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))

// schemaInfo is everything the Schema tab shows for a topic.
type schemaInfo struct {
	known      bool
	resolvedBy string
	format     string
	name       string // schema/message name
	file       string // spec file or .proto file
	extra      [][2]string
	comments   string // proto message comments
	warnings   []string
	fields     []schemafield.FieldDef
}

// schemaTabState holds the content of the Schema tab.
type schemaTabState struct {
	loaded        bool
	info          schemaInfo
	table         fieldTable
	width, height int
}

func (s schemaTabState) setInfo(info schemaInfo) schemaTabState {
	s.loaded = true
	s.info = info
	s.table = newFieldTable(info.fields)
	return s.resize(s.width, s.height)
}

func (s schemaTabState) resize(w, h int) schemaTabState {
	s.width, s.height = w, h
	if w <= 0 {
		return s
	}
	_, th := s.layout()
	s.table = s.table.resize(w, th)
	return s
}

// layout returns the header lines (capped to half the height) and the height
// left for the field table.
func (s schemaTabState) layout() (lines []string, tableH int) {
	if !s.info.known {
		return []string{helpStyle.Render(schemaNoneMsg)}, 2
	}
	lines = schemaHeaderLines(s.info, s.width)
	maxH := s.height / 2
	if maxH < 1 {
		maxH = 1
	}
	if len(lines) > maxH {
		lines = append(lines[:maxH-1], helpStyle.Render("…"))
	}
	tableH = s.height - len(lines)
	if tableH < 2 {
		tableH = 2
	}
	return lines, tableH
}

func (s schemaTabState) View() string {
	if s.width <= 0 {
		return ""
	}
	lines, _ := s.layout()
	if !s.info.known {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines, "\n") + "\n" + s.table.View()
}

func schemaHeaderLines(info schemaInfo, width int) []string {
	rows := [][2]string{
		{"resolved by", info.resolvedBy},
		{"format", info.format},
		{"schema", info.name},
		{"source", info.file},
	}
	rows = append(rows, info.extra...)
	rows = append(rows, [2]string{"comments", info.comments})
	out := kvHeaderLines(rows, width, schemaValueMaxLines)
	if len(info.warnings) > 0 {
		out = append(out, schemaWarnStyle.Render("Warnings:"))
		for _, w := range info.warnings {
			out = append(out, schemaWarnStyle.Render("  ! "+cellTruncate(strings.Join(strings.Fields(w), " "), width-4)))
		}
	}
	return out
}

// firstLines returns the first n non-empty lines of s, joined by spaces.
func firstLines(s string, n int) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
			if len(out) == n {
				break
			}
		}
	}
	return strings.Join(out, " ")
}

func asyncExtras(ts asyncapi.TopicSpec, msg asyncapi.MessageSpec) [][2]string {
	return [][2]string{
		{"spec", ts.SpecTitle},
		{"message title", msg.Title},
		{"event type", msg.EventType},
		{"summary", msg.Summary},
		{"description", msg.Description},
		{"topic summary", ts.Summary},
		{"topic description", ts.Description},
		{"key", msg.KeyDescription},
		{"cleanup policy", ts.CleanupPolicy},
	}
}

func fieldDefsFromRows(rows []database.TopicFieldRow) []schemafield.FieldDef {
	out := make([]schemafield.FieldDef, len(rows))
	for i, r := range rows {
		out[i] = schemafield.FieldDef{
			Path: r.FieldPath, Type: r.Type, Format: r.Format, Repeated: r.Repeated,
			Required: r.Required, Nullable: r.Nullable, Description: r.Description,
			Enum: r.Enum, Default: r.Default, Example: r.Example, Number: r.FieldNumber,
			Oneof: r.Oneof, Source: schemafield.Source(r.Source),
		}
	}
	return out
}

func findAsyncMessage(ts asyncapi.TopicSpec, name string) (asyncapi.MessageSpec, bool) {
	for _, m := range ts.Messages {
		if m.Name == name {
			return m, true
		}
	}
	if name == "" && len(ts.Messages) > 0 {
		return ts.Messages[0], true
	}
	return asyncapi.MessageSpec{}, false
}

// buildSchemaInfo resolves the Schema tab content: last in-memory detection,
// else stored kafka_topic_schemas/kafka_topic_fields rows, else the AsyncAPI
// documentation of the topic.
func buildSchemaInfo(ctx context.Context, deps Deps, topic string, last *topicsample.TopicResult) schemaInfo {
	var info schemaInfo
	specMsg := ""
	switch {
	case last != nil && (last.Spec != nil || last.MessageName != "" || last.ResolvedBy != ""):
		info = schemaInfo{
			known: true, resolvedBy: last.ResolvedBy, format: last.Format,
			name: last.MessageName, file: last.File, warnings: last.Warnings,
		}
		if last.Spec != nil {
			info.file, specMsg = last.Spec.File, last.Spec.Message
			if info.name == "" {
				info.name = specMsg
			}
		}
	case deps.DB != nil:
		info, specMsg = schemaInfoFromDB(ctx, deps, topic)
	}

	// Fields and enrichment from the loaded sources.
	var ts asyncapi.TopicSpec
	hasSpec := false
	if deps.AsyncAPI != nil {
		ts, hasSpec = deps.AsyncAPI.Lookup(topic)
	}
	isProto := info.format == topicsample.FormatProtobuf
	if hasSpec && !isProto && (specMsg != "" || !info.known || info.resolvedBy == topicsample.ResolvedByAsyncAPI) {
		if msg, ok := findAsyncMessage(ts, specMsg); ok {
			info.extra = asyncExtras(ts, msg)
			if len(info.fields) == 0 {
				info.fields = msg.Fields
			}
			if !info.known {
				info.known = true
				info.resolvedBy = topicsample.ResolvedByAsyncAPI
				info.format = topicsample.FormatJSON
				info.name = msg.Name
			}
			if info.file == "" {
				info.file = ts.SpecFile
			}
		}
	}
	if isProto && info.name != "" && deps.Messages != nil {
		if mi, ok := deps.Messages.Get(info.name); ok {
			info.comments = firstLines(mi.Comments, schemaCommentMaxRows)
			if len(info.fields) == 0 {
				info.fields = mi.FieldDefs()
			}
			if info.file == "" {
				info.file = mi.File
			}
		}
	}
	return info
}

func schemaInfoFromDB(ctx context.Context, deps Deps, topic string) (schemaInfo, string) {
	var info schemaInfo
	rows, _ := database.ReadTopicSchemas(ctx, deps.DB)
	row, haveRow := rows[topic]
	fields, _ := database.ReadTopicFields(ctx, deps.DB, topic)
	if !haveRow && len(fields) == 0 {
		return info, ""
	}
	info.known = true
	info.fields = fieldDefsFromRows(fields)
	if haveRow {
		info.resolvedBy, info.format, info.name = row.ResolvedBy, row.Format, row.MessageName
		info.file, info.warnings = row.ProtoFile, row.Warnings
		if row.MessageOverride && info.resolvedBy == "" {
			info.resolvedBy = topicsample.ResolvedByOverride
		}
		if row.SpecFile != "" {
			info.file = row.SpecFile
		}
		if info.name == "" {
			info.name = row.SpecMessage
		}
		if info.resolvedBy == "" {
			info.resolvedBy = topicsample.ResolvedByDetected
		}
		return info, row.SpecMessage
	}
	if len(fields) > 0 {
		info.name = fields[0].Message
		info.format = map[string]string{"proto": topicsample.FormatProtobuf, "asyncapi": topicsample.FormatJSON}[fields[0].Source]
	}
	return info, ""
}

// refreshSchema rebuilds the Schema tab from the current data sources.
func (m *detailModel) refreshSchema() {
	m.schema = m.schema.setInfo(buildSchemaInfo(context.Background(), m.deps, m.topic, m.lastResult))
}

// enterTab prepares a tab that is computed on entry.
func (m *detailModel) enterTab() {
	if m.tab == tabSchema && !m.schema.loaded {
		m.refreshSchema()
	}
}

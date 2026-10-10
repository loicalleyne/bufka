package tui

import (
	"fmt"
	"sort"
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

// protoSchemaModel shows one resolved protobuf message's leading comments
// and its raw .proto schema definition, sliced out of the source file via
// its descriptor's source location, followed by the definitions of every
// message it references (transitively), so the whole schema can be read
// without jumping between files.
type protoSchemaModel struct {
	fqn  string
	err  string
	vp   viewport.Model
	body string

	// fields is the field table shown instead of the source when showFields
	// is set (toggled with "f").
	fields     fieldTable
	showFields bool
}

var (
	protoSchemaCommentStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Italic(true)
	protoSchemaHeaderStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("63"))
	protoSchemaSectionStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))
	protoSchemaExternalStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Italic(true)
)

func newProtoSchemaModel(deps Deps, fqn string) protoSchemaModel {
	m := protoSchemaModel{fqn: fqn}

	body, err := protoSchemaBody(deps, fqn)
	if err != "" {
		m.err = err
		return m
	}
	m.body = body
	if info, ok := deps.Messages.Get(fqn); ok {
		m.fields = newFieldTable(info.FieldDefs())
	}
	return m
}

// protoSchemaBody renders the schema definition of fqn followed by every
// message it references, or returns a non-empty error string if the message
// cannot be found.
func protoSchemaBody(deps Deps, fqn string) (string, string) {
	if deps.Messages == nil {
		return "", "no protobuf messages loaded"
	}
	info, ok := deps.Messages.Get(fqn)
	if !ok {
		return "", fmt.Sprintf("message %s not found", fqn)
	}

	var b strings.Builder
	b.WriteString(renderProtoSchema(info))

	refs := referencedMessageNames(info.Descriptor, fqn)
	if len(refs) > 0 {
		b.WriteString("\n\n")
		b.WriteString(protoSchemaSectionStyle.Render("── Referenced messages ──"))
		b.WriteString("\n")
		for _, refFQN := range refs {
			b.WriteString("\n")
			refInfo, ok := deps.Messages.Get(refFQN)
			if !ok {
				fmt.Fprintf(&b, "%s\n", protoSchemaHeaderStyle.Render(refFQN))
				b.WriteString(protoSchemaExternalStyle.Render("(external type, not loaded from PROTO_IMPORT_PATHS)"))
				b.WriteString("\n")
				continue
			}
			fmt.Fprintf(&b, "%s\n", protoSchemaHeaderStyle.Render(refFQN))
			b.WriteString(renderProtoSchema(refInfo))
			b.WriteString("\n")
		}
	}

	return b.String(), ""
}

// renderProtoSchema builds the display text for one message: its path
// within the .proto root, leading/trailing comments (if any), and the raw
// schema text sliced from the file's source by source location.
func renderProtoSchema(info protoinspect.MessageInfo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s\n\n", info.Root, info.File)

	if strings.TrimSpace(info.Comments) != "" {
		for _, line := range strings.Split(strings.TrimRight(info.Comments, "\n"), "\n") {
			b.WriteString(protoSchemaCommentStyle.Render("//" + line))
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	snippet := messageSourceSnippet(info)
	if snippet == "" {
		snippet = "(schema source unavailable)"
	}
	b.WriteString(snippet)
	return b.String()
}

// messageSourceSnippet slices the message's declaration out of
// info.FileSource using its descriptor's source location, so it reflects
// exactly what is written in the .proto file (including nested messages).
func messageSourceSnippet(info protoinspect.MessageInfo) string {
	if info.Descriptor == nil || info.FileSource == "" {
		return ""
	}
	loc := info.Descriptor.ParentFile().SourceLocations().ByDescriptor(info.Descriptor)
	if loc.StartLine == 0 && loc.EndLine == 0 {
		return ""
	}
	lines := strings.Split(info.FileSource, "\n")
	start, end := loc.StartLine, loc.EndLine
	if start < 0 {
		start = 0
	}
	if end >= len(lines) {
		end = len(lines) - 1
	}
	if start > end || start >= len(lines) {
		return ""
	}
	return strings.Join(lines[start:end+1], "\n")
}

// referencedMessageNames walks desc's fields (including map values) and
// transitively collects the fully-qualified names of every message type it
// refers to, excluding desc itself and synthetic map-entry types, in
// first-encountered order.
func referencedMessageNames(desc protoreflect.MessageDescriptor, rootFQN string) []string {
	if desc == nil {
		return nil
	}
	visited := map[string]bool{rootFQN: true}
	var order []string

	var walk func(d protoreflect.MessageDescriptor)
	walk = func(d protoreflect.MessageDescriptor) {
		fields := d.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			target := messageFieldTarget(fd)
			if target == nil || target.IsMapEntry() {
				continue
			}
			fqn := string(target.FullName())
			if visited[fqn] {
				continue
			}
			visited[fqn] = true
			order = append(order, fqn)
			walk(target)
		}
	}
	walk(desc)

	sort.Strings(order)
	return order
}

// messageFieldTarget returns the message descriptor fd refers to, unwrapping
// map fields to their value type, or nil if fd is not message/group-typed.
func messageFieldTarget(fd protoreflect.FieldDescriptor) protoreflect.MessageDescriptor {
	if fd.IsMap() {
		mv := fd.MapValue()
		if mv == nil || mv.Kind() != protoreflect.MessageKind && mv.Kind() != protoreflect.GroupKind {
			return nil
		}
		return mv.Message()
	}
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		return nil
	}
	return fd.Message()
}

func (m protoSchemaModel) Init() tea.Cmd {
	return nil
}

func (m protoSchemaModel) resize(w, h int) (protoSchemaModel, tea.Cmd) {
	vh := h - 2
	if vh < 1 {
		vh = 1
	}
	m.vp = resizedViewport(m.vp, w, vh)
	m.vp.SetContent(m.body)
	m.fields = m.fields.resize(w, vh)
	return m, nil
}

func (m protoSchemaModel) update(msg tea.Msg) (protoSchemaModel, tea.Cmd) {
	var cmd tea.Cmd
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "f" {
		m.showFields = !m.showFields
		return m, nil
	}
	if m.showFields {
		m.fields, cmd = m.fields.update(msg)
		return m, cmd
	}
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

func (m protoSchemaModel) View() string {
	if m.err != "" {
		return helpStyle.Render(m.err) + "\n" + helpStyle.Render("esc back")
	}
	header := protoSchemaHeaderStyle.Render(m.fqn)
	if m.showFields {
		return header + "\n" + m.fields.View() + "\n" + helpStyle.Render("↑/↓/pgup/pgdn scroll · f show source · esc back")
	}
	return header + "\n" + m.vp.View() + "\n" + helpStyle.Render("↑/↓ scroll · f show fields · esc back")
}

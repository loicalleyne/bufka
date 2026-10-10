package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/couac"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

const (
	sectionProto = iota
	sectionAsync
	sectionInclude
	numSections
)

// sectionOrder is the on-screen (and navigation) order of the sections.
var sectionOrder = [numSections]int{sectionProto, sectionInclude, sectionAsync}

// orderPos returns the position of section i in sectionOrder.
func orderPos(i int) int {
	for p, s := range sectionOrder {
		if s == i {
			return p
		}
	}
	return 0
}

// maxResultLines bounds the failure/warning lines shown per section.
const maxResultLines = 4

// pathSection is one editable path list.
type pathSection struct {
	paths   []string // staged paths
	applied []string // paths currently in effect
	cursor  int
}

func (s pathSection) dirty() bool {
	if len(s.paths) != len(s.applied) {
		return true
	}
	for i := range s.paths {
		if s.paths[i] != s.applied[i] {
			return true
		}
	}
	return false
}

// sectionResult is the last load outcome of one section.
type sectionResult struct {
	status   string
	failures []string
	warnings []string
}

// protoSettingsModel (settings > schema sources) edits the protobuf root
// paths, the protobuf include (import-only) paths and the AsyncAPI paths. Edits are staged locally and only take effect
// (sources reloaded, deps updated) when applied with "s". When a database is
// configured the applied lists are saved there and take precedence over the
// environment on the next launch; without one they last for the session only.
type protoSettingsModel struct {
	ctx   context.Context
	db    *couac.DB
	hasDB bool
	secs  [numSections]pathSection
	focus int

	results [numSections]sectionResult

	input    textinput.Model
	editing  bool
	editIdx  int // index being edited; -1 when adding a new path
	loading  bool
	note     string // last apply result or validation message
	noteIsOK bool
}

// schemaSourcesLoadedMsg carries the result of reloading both sources.
type schemaSourcesLoadedMsg struct {
	protoPaths   []string
	includePaths []string
	asyncPaths   []string

	messages    *protoinspect.OrderedMap[string, protoinspect.MessageInfo]
	protoFailed []protoinspect.FileError
	protoErr    error // proto reload failed; previous messages are kept

	index     *asyncapi.Index
	asyncErrs []asyncapi.LoadError

	wasSave bool  // triggered by "s" (apply & save) rather than "r"
	saved   bool  // paths were persisted to the database
	saveErr error // persisting failed (paths still applied this session)
}

func newProtoSettingsModel(ctx context.Context, deps Deps) protoSettingsModel {
	ti := textinput.New()
	ti.Placeholder = "/path/to/root"
	m := protoSettingsModel{ctx: ctx, db: deps.DB, hasDB: deps.DB != nil, input: ti}
	m.secs[sectionProto] = pathSection{
		paths:   append([]string(nil), deps.ProtoImportPaths...),
		applied: append([]string(nil), deps.ProtoImportPaths...),
	}
	m.secs[sectionAsync] = pathSection{
		paths:   append([]string(nil), deps.AsyncAPIPaths...),
		applied: append([]string(nil), deps.AsyncAPIPaths...),
	}
	m.secs[sectionInclude] = pathSection{
		paths:   append([]string(nil), deps.ProtoIncludePaths...),
		applied: append([]string(nil), deps.ProtoIncludePaths...),
	}
	m.results[sectionInclude].status = includeStatus
	m.results[sectionProto].status = "0 messages loaded"
	if deps.Messages != nil {
		m.results[sectionProto].status = fmt.Sprintf("%d messages loaded", deps.Messages.Len())
	}
	m.results[sectionAsync].status = asyncStatus(deps.AsyncAPI)
	return m
}

// includeStatus is shown under the include list; import failures show in
// the proto section, whose files are the ones being compiled.
const includeStatus = "import-only; google/protobuf and google/type are built in"

func asyncStatus(ix *asyncapi.Index) string {
	if ix == nil {
		return "0 specs, 0 topics loaded"
	}
	return fmt.Sprintf("%d specs, %d topics loaded", len(ix.Files()), len(ix.Topics()))
}

func (m protoSettingsModel) Init() tea.Cmd { return nil }

func (m protoSettingsModel) resize(w, h int) (protoSettingsModel, tea.Cmd) {
	return m, nil
}

// capturingInput reports whether keys are going to the text input, so the
// parent must not treat esc/q etc. as navigation.
func (m protoSettingsModel) capturingInput() bool { return m.editing }

func (m protoSettingsModel) dirty() bool {
	for _, s := range m.secs {
		if s.dirty() {
			return true
		}
	}
	return false
}

// dedupeAppend returns dst followed by the entries of src not already present.
func dedupeAppend(dst, src []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(dst)+len(src))
	for _, p := range append(append([]string(nil), dst...), src...) {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func sectionName(i int) string {
	switch i {
	case sectionProto:
		return "proto"
	case sectionInclude:
		return "include"
	}
	return "asyncapi"
}

func (m protoSettingsModel) update(msg tea.Msg) (protoSettingsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case schemaSourcesLoadedMsg:
		m.loading = false
		ok := true
		if msg.protoErr != nil {
			m.results[sectionProto] = sectionResult{status: "reload failed", failures: []string{msg.protoErr.Error()}}
			ok = false
		} else {
			m.secs[sectionProto].applied = append([]string(nil), msg.protoPaths...)
			m.secs[sectionInclude].applied = append([]string(nil), msg.includePaths...)
			n := 0
			if msg.messages != nil {
				n = msg.messages.Len()
			}
			r := sectionResult{status: fmt.Sprintf("%d messages loaded", n)}
			for _, f := range msg.protoFailed {
				r.failures = append(r.failures, f.Error())
			}
			m.results[sectionProto] = r
		}
		m.secs[sectionAsync].applied = append([]string(nil), msg.asyncPaths...)
		r := sectionResult{status: asyncStatus(msg.index)}
		for _, e := range msg.asyncErrs {
			if errors.Is(e, asyncapi.ErrExternalRef) {
				r.warnings = append(r.warnings, e.Error())
			} else {
				r.failures = append(r.failures, e.Error())
			}
		}
		m.results[sectionAsync] = r

		m.note = "reloaded"
		if msg.wasSave {
			m.note = "applied"
		}
		switch {
		case msg.saveErr != nil:
			m.note += "; NOT saved to database: " + msg.saveErr.Error()
			ok = false
		case msg.saved:
			m.note += "; saved to database"
		case msg.wasSave:
			m.note += "; session only (in-memory database)"
		}
		m.noteIsOK = ok
		return m, nil

	case tea.KeyPressMsg:
		sec := &m.secs[m.focus]
		if m.editing {
			switch msg.String() {
			case "esc":
				m.editing = false
				m.input.Blur()
				m.input.SetValue("")
				return m, nil
			case "enter":
				v := strings.TrimSpace(m.input.Value())
				m.editing = false
				m.input.Blur()
				m.input.SetValue("")
				if v == "" {
					return m, nil
				}
				if m.editIdx >= 0 && m.editIdx < len(sec.paths) {
					sec.paths[m.editIdx] = v
				} else {
					sec.paths = append(sec.paths, v)
					sec.cursor = len(sec.paths) - 1
				}
				m.note = ""
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}

		switch msg.String() {
		case "tab":
			m.focus = sectionOrder[(orderPos(m.focus)+1)%numSections]
		case "shift+tab":
			m.focus = sectionOrder[(orderPos(m.focus)+numSections-1)%numSections]
		case "up", "k":
			// The lists behave as one: moving past the top of a list
			// continues into the end of the one above it.
			if sec.cursor > 0 {
				sec.cursor--
			} else if p := orderPos(m.focus); p > 0 {
				m.focus = sectionOrder[p-1]
				m.secs[m.focus].cursor = max(len(m.secs[m.focus].paths)-1, 0)
			}
		case "down", "j":
			if sec.cursor < len(sec.paths)-1 {
				sec.cursor++
			} else if p := orderPos(m.focus); p < numSections-1 {
				m.focus = sectionOrder[p+1]
				m.secs[m.focus].cursor = 0
			}
		case "a":
			m.editing, m.editIdx = true, -1
			m.input.SetValue("")
			return m, m.input.Focus()
		case "e", "enter":
			if sec.cursor < len(sec.paths) {
				m.editing, m.editIdx = true, sec.cursor
				m.input.SetValue(sec.paths[sec.cursor])
				m.input.CursorEnd()
				return m, m.input.Focus()
			}
		case "d", "x":
			if sec.cursor < len(sec.paths) {
				sec.paths = append(sec.paths[:sec.cursor], sec.paths[sec.cursor+1:]...)
				if sec.cursor >= len(sec.paths) && sec.cursor > 0 {
					sec.cursor--
				}
				m.note = ""
			}
		case "=":
			// The screen has no confirm step for its other edits (they are
			// all staged until "s"), so the copy is applied directly to the
			// staged list; "u" on the other section undoes it.
			// Only proto <-> asyncapi (one docs tree often holds both);
			// include paths are import-only, so they are not copied.
			if m.focus == sectionInclude {
				m.note, m.noteIsOK = "= copies between the proto and asyncapi lists only", false
				return m, nil
			}
			to := sectionAsync
			if m.focus == sectionAsync {
				to = sectionProto
			}
			other := &m.secs[to]
			other.paths = dedupeAppend(other.paths, sec.paths)
			m.note, m.noteIsOK = fmt.Sprintf("copied %s paths to %s list (unapplied)", sectionName(m.focus), sectionName(to)), true
		case "s", "r":
			if m.loading {
				return m, nil
			}
			save := msg.String() == "s"
			if save && !m.dirty() {
				m.note, m.noteIsOK = "no changes to apply", true
				return m, nil
			}
			var paths [numSections][]string
			for i := range m.secs {
				paths[i] = m.secs[i].applied
				if save {
					paths[i] = m.secs[i].paths
				}
			}
			if save {
				for _, p := range append(append([]string(nil), paths[sectionProto]...), paths[sectionInclude]...) {
					if st, err := os.Stat(p); err != nil || !st.IsDir() {
						m.note, m.noteIsOK = fmt.Sprintf("not a directory: %s", p), false
						return m, nil
					}
				}
				for _, p := range paths[sectionAsync] {
					if _, err := os.Stat(p); err != nil {
						m.note, m.noteIsOK = fmt.Sprintf("asyncapi path does not exist: %s", p), false
						return m, nil
					}
				}
			}
			m.loading = true
			m.note, m.noteIsOK = "loading...", true
			return m, applySchemaSourcesCmd(ctxOrBackground(m.ctx), m.db, schemaPaths{
				proto:   append([]string(nil), paths[sectionProto]...),
				include: append([]string(nil), paths[sectionInclude]...),
				async:   append([]string(nil), paths[sectionAsync]...),
			}, save)
		case "u":
			sec.paths = append([]string(nil), sec.applied...)
			if sec.cursor >= len(sec.paths) {
				sec.cursor = max(len(sec.paths)-1, 0)
			}
			m.note = ""
		}
	}
	return m, nil
}

// schemaPaths are the three path lists of the schema sources screen.
type schemaPaths struct {
	proto, include, async []string
}

// applySchemaSourcesCmd reloads the sources in the background. When save is
// set each list is written with its own database function (a failed proto
// reload persists neither the proto nor the include list).
func applySchemaSourcesCmd(ctx context.Context, db *couac.DB, p schemaPaths, save bool) tea.Cmd {
	return func() tea.Msg {
		protoPaths, asyncPaths := p.proto, p.async
		out := schemaSourcesLoadedMsg{protoPaths: protoPaths, includePaths: p.include, asyncPaths: asyncPaths, wasSave: save}
		if len(protoPaths) > 0 {
			msgs, failures, err := protoinspect.LoadMessages(protoPaths, p.include...)
			if err != nil {
				out.protoErr = err
			} else {
				out.messages, out.protoFailed = msgs, failures
			}
		}
		if len(asyncPaths) > 0 {
			out.index, out.asyncErrs = asyncapi.Load(asyncPaths)
		}
		if save && db != nil {
			var errs []error
			if out.protoErr == nil {
				if err := database.WriteProtoImportPaths(ctx, db, protoPaths); err != nil {
					errs = append(errs, err)
				}
				if err := database.WriteProtoIncludePaths(ctx, db, p.include); err != nil {
					errs = append(errs, err)
				}
			}
			if err := database.WriteAsyncAPIPaths(ctx, db, asyncPaths); err != nil {
				errs = append(errs, err)
			}
			out.saveErr = errors.Join(errs...)
			out.saved = out.saveErr == nil
		}
		return out
	}
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (m protoSettingsModel) renderSection(b *strings.Builder, i int, title string) {
	sec := m.secs[i]
	hdr := "  " + title
	if i == m.focus {
		hdr = "▶ " + title
	}
	if sec.dirty() {
		hdr += "  (unapplied changes)"
	}
	b.WriteString(hdr + "\n")
	if len(sec.paths) == 0 {
		cur := "    "
		if i == m.focus && !m.editing {
			cur = "  > "
		}
		b.WriteString(cur + "(none configured)\n")
	}
	for j, p := range sec.paths {
		cur := "    "
		if i == m.focus && j == sec.cursor && !m.editing {
			cur = "  > "
		}
		b.WriteString(cur + p + "\n")
	}
	r := m.results[i]
	b.WriteString("    " + r.status + "\n")
	for _, l := range truncLines(r.failures, "failures") {
		b.WriteString("    ✗ " + l + "\n")
	}
	for _, l := range truncLines(r.warnings, "warnings") {
		b.WriteString("    ! " + l + "\n")
	}
}

func truncLines(lines []string, what string) []string {
	if len(lines) <= maxResultLines {
		return lines
	}
	out := append([]string(nil), lines[:maxResultLines]...)
	return append(out, fmt.Sprintf("... and %d more %s", len(lines)-maxResultLines, what))
}

func (m protoSettingsModel) View() string {
	var b strings.Builder
	titles := map[int]string{
		sectionProto:   "PROTO ROOT PATHS (messages listed and detected)",
		sectionInclude: "PROTO INCLUDE PATHS (imports only, like protoc -I)",
		sectionAsync:   "ASYNCAPI PATHS",
	}
	for p, i := range sectionOrder {
		if p > 0 {
			b.WriteString("\n")
		}
		m.renderSection(&b, i, titles[i])
	}
	if m.editing {
		b.WriteString("\n")
		if m.editIdx >= 0 {
			b.WriteString("edit path: ")
		} else {
			b.WriteString("new path:  ")
		}
		b.WriteString(m.input.View())
		b.WriteString("\n")
	}
	body := borderStyle.Render(strings.TrimRight(b.String(), "\n"))

	out := body + "\n"
	if m.note != "" {
		if m.noteIsOK {
			out += statusStyle.Render(m.note) + "\n"
		} else {
			out += errorStyle.Render(m.note) + "\n"
		}
	}
	if m.editing {
		return out + helpStyle.Render("enter save · esc cancel")
	}
	where := "s apply (session only)"
	if m.hasDB {
		where = "s apply & save"
	}
	return out + helpStyle.Render("tab section · ↑/↓ select · a add · e/enter edit · d delete · = copy proto⇄asyncapi · "+where+" · r reload · u undo · esc back (discards unapplied)")
}

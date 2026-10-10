package tui

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
)

// ksField describes one editable Kafka setting.
type ksField struct {
	label string
	// ptr selects this field's raw text inside a SettingsInput.
	ptr func(in *kafka.SettingsInput) *string
	// normalize validates raw text and returns the value to store. Empty
	// input is always accepted (it means "use the environment default").
	normalize func(string) (string, error)
	// envName fields hold the NAME of an environment variable and are shown
	// and edited with a leading "$".
	envName bool
	// def returns the environment default shown when the field is empty.
	def func(env *config.Config) string
}

func identity(s string) (string, error) { return strings.TrimSpace(s), nil }

var ksFields = []ksField{
	{
		label: "Seeds",
		ptr:   func(in *kafka.SettingsInput) *string { return &in.Seeds },
		normalize: func(s string) (string, error) {
			if strings.TrimSpace(s) == "" {
				return "", nil
			}
			seeds, err := kafka.ParseSeeds(s)
			if err != nil {
				return "", err
			}
			return strings.Join(seeds, ";"), nil
		},
		def: func(e *config.Config) string { return e.KafkaSeeds },
	},
	{
		label:     "Consumer group",
		ptr:       func(in *kafka.SettingsInput) *string { return &in.ConsumerGroup },
		normalize: identity,
		def:       func(e *config.Config) string { return e.KafkaConsumerGroup },
	},
	{
		label:     "Consumer group prefix",
		ptr:       func(in *kafka.SettingsInput) *string { return &in.ConsumerGroupPrefix },
		normalize: identity,
		def:       func(e *config.Config) string { return e.KafkaConsumerGroupPrefix },
	},
	{
		label:     "Instance prefix",
		ptr:       func(in *kafka.SettingsInput) *string { return &in.InstancePrefix },
		normalize: identity,
		def:       func(e *config.Config) string { return e.KafkaInstancePrefix },
	},
	{
		label: "Msg chan capacity",
		ptr:   func(in *kafka.SettingsInput) *string { return &in.MsgChanCap },
		normalize: func(s string) (string, error) {
			if strings.TrimSpace(s) == "" {
				return "", nil
			}
			n, err := kafka.ParseMsgChanCap(s)
			if err != nil {
				return "", err
			}
			return strconv.Itoa(n), nil
		},
		def: func(e *config.Config) string { return e.KafkaMsgChanCap },
	},
	{
		label: "TLS enabled",
		ptr:   func(in *kafka.SettingsInput) *string { return &in.TLS },
		normalize: func(s string) (string, error) {
			if strings.TrimSpace(s) == "" {
				return "", nil
			}
			b, err := kafka.ParseTLS(s)
			if err != nil {
				return "", err
			}
			return strconv.FormatBool(b), nil
		},
		def: func(e *config.Config) string { return strconv.FormatBool(e.KafkaTLSEnabled) },
	},
	{
		label:     "SASL user",
		ptr:       func(in *kafka.SettingsInput) *string { return &in.UserEnv },
		normalize: normalizeEnvField,
		envName:   true,
	},
	{
		label:     "SASL password",
		ptr:       func(in *kafka.SettingsInput) *string { return &in.PasswordEnv },
		normalize: normalizeEnvField,
		envName:   true,
	},
}

func normalizeEnvField(s string) (string, error) {
	if err := kafka.ValidateEnvName(s); err != nil {
		return "", err
	}
	return kafka.NormalizeEnvName(s), nil
}

// kafkaSettingsAppliedMsg carries the result of applying edited settings.
type kafkaSettingsAppliedMsg struct {
	input    kafka.SettingsInput
	warnings []string
	saved    bool  // persisted to the database
	saveErr  error // persisting failed (settings are still applied this session)
	err      error // validation/resolution failed; nothing was applied
}

// kafkaSettingsModel shows and edits the Kafka connection settings. Edits are
// staged and only take effect when applied with "s": the shared KafkaConf is
// updated in place (operations already running keep the settings they
// started with) and, when a database is configured, the raw input is saved
// there so it takes precedence over the environment on the next launch.
// SASL user/password fields hold the NAME of an environment variable, never
// the credential itself.
type kafkaSettingsModel struct {
	ctx   context.Context
	deps  Deps
	hasDB bool

	staged  kafka.SettingsInput
	applied kafka.SettingsInput
	cursor  int

	input    textinput.Model
	editing  bool
	loading  bool
	note     string
	noteIsOK bool
}

func newKafkaSettingsModel(ctx context.Context, deps Deps) kafkaSettingsModel {
	ti := textinput.New()
	return kafkaSettingsModel{
		ctx:     ctx,
		deps:    deps,
		hasDB:   deps.DB != nil,
		staged:  deps.KafkaSettings,
		applied: deps.KafkaSettings,
		input:   ti,
	}
}

func (m kafkaSettingsModel) Init() tea.Cmd { return nil }

func (m kafkaSettingsModel) resize(w, h int) (kafkaSettingsModel, tea.Cmd) { return m, nil }

// capturingInput reports whether keys are going to the text input, so the
// parent must not treat esc as navigation.
func (m kafkaSettingsModel) capturingInput() bool { return m.editing }

func (m kafkaSettingsModel) dirty() bool { return m.staged != m.applied }

func (m kafkaSettingsModel) env() *config.Config {
	if m.deps.EnvConfig != nil {
		return m.deps.EnvConfig
	}
	return &config.Config{}
}

func (m kafkaSettingsModel) update(msg tea.Msg) (kafkaSettingsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case kafkaSettingsAppliedMsg:
		m.loading = false
		if msg.err != nil {
			m.note, m.noteIsOK = "not applied: "+msg.err.Error(), false
			return m, nil
		}
		m.applied = msg.input
		m.note, m.noteIsOK = "applied", true
		switch {
		case msg.saveErr != nil:
			m.note += "; NOT saved to database: " + msg.saveErr.Error()
			m.noteIsOK = false
		case msg.saved:
			m.note += " and saved to database"
		default:
			m.note += " (session only: in-memory database)"
		}
		if len(msg.warnings) > 0 {
			m.note += "; warning: " + strings.Join(msg.warnings, ", ")
			m.noteIsOK = false
		}
		return m, nil

	case tea.KeyPressMsg:
		f := ksFields[m.cursor]
		if m.editing {
			switch msg.String() {
			case "esc":
				m.editing = false
				m.input.Blur()
				m.input.SetValue("")
				m.note = ""
				return m, nil
			case "enter":
				v, err := f.normalize(m.input.Value())
				if err != nil {
					// Stay in the editor so the user can fix it.
					m.note, m.noteIsOK = f.label+": "+err.Error(), false
					return m, nil
				}
				*f.ptr(&m.staged) = v
				m.editing = false
				m.input.Blur()
				m.input.SetValue("")
				m.note = ""
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}

		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(ksFields)-1 {
				m.cursor++
			}
		case "e", "enter":
			m.editing = true
			m.note = ""
			m.input.Prompt = "> "
			if f.envName {
				m.input.Prompt = "$ "
			}
			m.input.SetValue(*f.ptr(&m.staged))
			m.input.CursorEnd()
			return m, m.input.Focus()
		case "d":
			*f.ptr(&m.staged) = ""
			m.note = ""
		case "u":
			m.staged = m.applied
			m.note = ""
		case "s":
			if m.loading {
				return m, nil
			}
			if !m.dirty() {
				m.note, m.noteIsOK = "no changes to apply", true
				return m, nil
			}
			m.loading = true
			m.note, m.noteIsOK = "applying...", true
			return m, applyKafkaSettingsCmd(ctxOrBackground(m.ctx), m.deps, m.staged)
		}
	}
	return m, nil
}

// applyKafkaSettingsCmd validates and resolves in against the environment,
// saves it to the database (when configured), then updates the live conf.
// Nothing is saved or applied if validation fails.
func applyKafkaSettingsCmd(ctx context.Context, deps Deps, in kafka.SettingsInput) tea.Cmd {
	return func() tea.Msg {
		out := kafkaSettingsAppliedMsg{input: in}
		s, err := kafka.ResolveSettings(in, deps.EnvConfig, os.Getenv)
		if err != nil {
			out.err = err
			return out
		}
		out.warnings = s.Warnings
		if deps.DB != nil {
			if err := database.WriteKafkaSettings(ctx, deps.DB, in.ToMap()); err != nil {
				out.saveErr = err
			} else {
				out.saved = true
			}
		}
		if deps.KafkaConf != nil {
			deps.KafkaConf.ApplySettings(s)
		}
		return out
	}
}

// prefixInEffect returns the consumer group prefix that will apply: the staged
// value, or the environment's when the staged one is empty.
func (m kafkaSettingsModel) prefixInEffect() string {
	if p := strings.TrimSpace(m.staged.ConsumerGroupPrefix); p != "" {
		return p
	}
	return strings.TrimSpace(m.env().KafkaConsumerGroupPrefix)
}

// effectiveGroup describes the consumer group the live configuration is
// using. With a prefix it is the prefix plus a random suffix generated when
// the settings were loaded/applied, so it is only known from the live conf.
func (m kafkaSettingsModel) effectiveGroup() string {
	if m.deps.KafkaConf == nil {
		return "-"
	}
	g := m.deps.KafkaConf.Snapshot().ConsumerGroup
	if g == "" {
		return "- (none: clients are not in a consumer group)"
	}
	return g
}

// display renders a field's current staged value, or its environment
// default (dimmed) when empty.
func (m kafkaSettingsModel) display(f ksField) string {
	raw := *f.ptr(&m.staged)
	if f.label == "Consumer group" && m.prefixInEffect() != "" {
		shown := raw
		if shown == "" {
			shown = f.def(m.env())
		}
		if shown == "" {
			shown = "-"
		}
		return helpStyle.Render(shown + "  (ignored: overridden by the prefix)")
	}
	if f.envName {
		if raw == "" {
			which := "KAFKA_SASL_USER"
			if f.label == "SASL password" {
				which = "KAFKA_SASL_PASSWORD"
			}
			return "$" + helpStyle.Render("(empty: uses "+which+")")
		}
		status := "(set)"
		if os.Getenv(raw) == "" {
			status = "(NOT SET)"
		}
		return "$" + raw + "  " + helpStyle.Render(status)
	}
	if raw == "" {
		d := f.def(m.env())
		if d == "" {
			d = "-"
		}
		return helpStyle.Render("(env: " + d + ")")
	}
	return raw
}

func (m kafkaSettingsModel) View() string {
	if m.deps.KafkaConf == nil {
		return helpStyle.Render("no Kafka configuration loaded")
	}

	var b strings.Builder
	b.WriteString("Kafka connection settings")
	if m.dirty() {
		b.WriteString("  (unapplied changes)")
	}
	b.WriteString("\n\n")
	for i, f := range ksFields {
		cur := "  "
		if i == m.cursor && !m.editing {
			cur = "> "
		}
		fmt.Fprintf(&b, "%s%-22s %s\n", cur, f.label+":", m.display(f))
	}
	fmt.Fprintf(&b, "  %-22s %s\n", "Group in effect:", m.effectiveGroup())
	if m.dirty() {
		b.WriteString("  " + helpStyle.Render("(reflects the last applied settings; apply to update)") + "\n")
	}
	fmt.Fprintf(&b, "  %-22s %s\n", "Client count:",
		helpStyle.Render(fmt.Sprintf("%d (read-only; used by the kafka consume command)", m.deps.KafkaConf.ClientCount.Load())))

	if m.editing {
		f := ksFields[m.cursor]
		b.WriteString("\n" + f.label + ": ")
		b.WriteString(m.input.View())
		b.WriteString("\n")
		b.WriteString(helpStyle.Render(m.editHint(f)))
		b.WriteString("\n")
	}

	out := borderStyle.Render(strings.TrimRight(b.String(), "\n")) + "\n"
	out += helpStyle.Render("SASL user/password: enter the NAME of an environment variable holding the value (e.g. $MY_KAFKA_PASSWORD), not the value itself.") + "\n"
	out += helpStyle.Render("Consumer group prefix overrides Consumer group: when a prefix is set, the group is the prefix plus a random suffix.") + "\n"
	out += helpStyle.Render("Empty fields use the environment defaults.") + "\n"
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
	return out + helpStyle.Render("↑/↓ select · e/enter edit · d clear · "+where+" · u undo · esc back (discards unapplied)")
}

func (m kafkaSettingsModel) editHint(f ksField) string {
	switch f.label {
	case "Seeds":
		return "semicolon- or comma-separated host:port list, e.g. a:9092;b:9092"
	case "Msg chan capacity":
		return "integer >= 1"
	case "TLS enabled":
		return "t, f, true or false (any case)"
	case "SASL user":
		return "NAME of the env var holding the user (leave empty to use KAFKA_SASL_USER); the leading $ is optional"
	case "SASL password":
		return "NAME of the env var holding the password (leave empty to use KAFKA_SASL_PASSWORD); the leading $ is optional"
	}
	switch f.label {
	case "Consumer group":
		return "free text; ignored whenever a Consumer group prefix is set"
	case "Consumer group prefix":
		return "free text; when set, overrides Consumer group (the group becomes the prefix plus a random suffix)"
	}
	return "free text; empty uses the environment default"
}

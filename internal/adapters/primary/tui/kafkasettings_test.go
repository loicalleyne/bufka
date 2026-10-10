package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
)

func ksTestDeps() Deps {
	c := kafka.NewKafkaConfig()
	c.Seeds = []string{"env:9092"}
	return Deps{KafkaConf: c, EnvConfig: &config.Config{KafkaSeeds: "env:9092", KafkaMsgChanCap: "10"}}
}

func ksType(m kafkaSettingsModel, s string) kafkaSettingsModel {
	for _, r := range s {
		m, _ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	return m
}

// ksEdit moves to the field with the given label and enters text.
func ksEdit(t *testing.T, m kafkaSettingsModel, label, text string) kafkaSettingsModel {
	t.Helper()
	for i, f := range ksFields {
		if f.label == label {
			m.cursor = i
			m, _ = m.update(tea.KeyPressMsg{Code: 'e', Text: "e"})
			m = ksType(m, text)
			m, _ = m.update(tea.KeyPressMsg{Code: tea.KeyEnter})
			return m
		}
	}
	t.Fatalf("no field %q", label)
	return m
}

func TestKafkaSettingsFieldValidation(t *testing.T) {
	cases := []struct {
		label, text string
		ok          bool
	}{
		{"Seeds", "a:9092;b:9093", true},
		{"Seeds", "a:9092, b:9093", true},
		{"Seeds", "nohostport", false},
		{"Seeds", "a:99999", false},
		{"Msg chan capacity", "1", true},
		{"Msg chan capacity", "0", false},
		{"Msg chan capacity", "abc", false},
		{"TLS enabled", "TrUe", true},
		{"TLS enabled", "f", true},
		{"TLS enabled", "yes", false},
		{"SASL password", "$MY_PASS", true},
		{"SASL password", "MY_PASS", true},
		{"SASL password", "bad name", false},
	}
	for _, c := range cases {
		m := newKafkaSettingsModel(context.Background(), ksTestDeps())
		m = ksEdit(t, m, c.label, c.text)
		if c.ok && (m.editing || !m.dirty()) {
			t.Errorf("%s=%q should be accepted (editing=%v note=%q)", c.label, c.text, m.editing, m.note)
		}
		if !c.ok && (!m.editing || m.dirty() || m.noteIsOK) {
			t.Errorf("%s=%q should be rejected and stay in the editor", c.label, c.text)
		}
	}
}

func TestKafkaSettingsNormalizesStoredValues(t *testing.T) {
	m := newKafkaSettingsModel(context.Background(), ksTestDeps())
	m = ksEdit(t, m, "Seeds", " a:1 , b:2 ")
	m = ksEdit(t, m, "TLS enabled", "T")
	m = ksEdit(t, m, "SASL user", "${MY_USER}")
	if m.staged.Seeds != "a:1;b:2" || m.staged.TLS != "true" || m.staged.UserEnv != "MY_USER" {
		t.Fatalf("not normalized: %+v", m.staged)
	}
}

func TestKafkaSettingsSASLShowsDollarAndEnvStatus(t *testing.T) {
	t.Setenv("KS_TEST_PASS", "secret-value")
	m := newKafkaSettingsModel(context.Background(), ksTestDeps())
	m = ksEdit(t, m, "SASL password", "KS_TEST_PASS")
	v := m.View()
	if !strings.Contains(v, "$KS_TEST_PASS") {
		t.Errorf("env var name should be shown with a leading $:\n%s", v)
	}
	if strings.Contains(v, "secret-value") {
		t.Error("the credential value must never be displayed")
	}
	if !strings.Contains(v, "NAME of an environment variable") {
		t.Error("help note explaining the env var name is missing")
	}
}

func TestKafkaSettingsApplyUpdatesLiveConfAndSaves(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "k.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	t.Setenv("KS_U", "alice")
	t.Setenv("KS_P", "pw")
	deps := ksTestDeps()
	deps.DB = db

	in := kafka.SettingsInput{Seeds: "new:1;new:2", MsgChanCap: "7", TLS: "true", UserEnv: "KS_U", PasswordEnv: "KS_P"}
	msg := applyKafkaSettingsCmd(ctx, deps, in)().(kafkaSettingsAppliedMsg)
	if msg.err != nil || !msg.saved || msg.saveErr != nil {
		t.Fatalf("apply failed: %+v", msg)
	}

	snap := deps.KafkaConf.Snapshot()
	if len(snap.Seeds) != 2 || snap.MsgChanCap != 7 || snap.TLSDialer == nil || snap.User != "alice" || snap.Password != "pw" {
		t.Fatalf("live conf not updated: %+v", snap)
	}

	kv, _ := database.ReadKafkaSettings(ctx, db)
	if got := kafka.SettingsInputFromMap(kv); got != in {
		t.Fatalf("saved input mismatch: %+v", got)
	}
	// The credential itself must never be written to the database.
	for _, v := range kv {
		if v == "pw" || v == "alice" {
			t.Fatalf("credential value leaked into database: %v", kv)
		}
	}
}

func TestKafkaSettingsInvalidApplyChangesNothing(t *testing.T) {
	db, err := database.InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "k.duckdb")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	deps := ksTestDeps()
	deps.DB = db

	// Only one of user/password resolves → rejected before save or apply.
	t.Setenv("KS_ONLY_U", "bob")
	msg := applyKafkaSettingsCmd(context.Background(), deps, kafka.SettingsInput{UserEnv: "KS_ONLY_U", PasswordEnv: "KS_MISSING"})().(kafkaSettingsAppliedMsg)
	if msg.err == nil || msg.saved {
		t.Fatalf("expected rejection, got %+v", msg)
	}
	if deps.KafkaConf.Snapshot().User != "" {
		t.Fatal("live conf must not change on a rejected apply")
	}
	if kv, _ := database.ReadKafkaSettings(context.Background(), db); len(kv) != 0 {
		t.Fatalf("nothing should be saved, got %v", kv)
	}
}

func TestKafkaSettingsEscWhileEditingStaysOnPage(t *testing.T) {
	mod := New(nil, ksTestDeps())
	mod.page = pageKafkaSettings
	n, _ := mod.Update(tea.KeyPressMsg{Code: 'e', Text: "e"})
	n, _ = n.(Model).Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if n.(Model).page != pageKafkaSettings {
		t.Fatal("esc during an edit must not leave the page")
	}
	n, _ = n.(Model).Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if n.(Model).page != pageMenu {
		t.Fatal("esc outside an edit should return to the menu")
	}
}

func TestKafkaSettingsShowsEffectiveGroupAndOverrideNote(t *testing.T) {
	deps := ksTestDeps()
	deps.KafkaConf.ApplySettings(kafka.Settings{Seeds: []string{"a:1"}, MsgChanCap: 1, ConsumerGroup: "plain"})
	m := newKafkaSettingsModel(context.Background(), deps)
	v := m.View()
	if !strings.Contains(v, "Group in effect:") || !strings.Contains(v, "plain") {
		t.Errorf("effective group missing:\n%s", v)
	}
	if !strings.Contains(v, "Consumer group prefix overrides Consumer group") {
		t.Error("override note missing")
	}
	if strings.Contains(v, "overridden by the prefix") {
		t.Error("no prefix is set, so the group must not be shown as overridden")
	}

	// A prefix: the real group is prefix + random suffix, and the editable
	// Consumer group row is marked as ignored.
	deps.KafkaConf.ApplySettings(kafka.Settings{Seeds: []string{"a:1"}, MsgChanCap: 1, ConsumerGroup: "plain", ConsumerGroupPrefix: "acl-"})
	m = newKafkaSettingsModel(context.Background(), deps)
	m = ksEdit(t, m, "Consumer group prefix", "acl-")
	v = m.View()
	eff := deps.KafkaConf.Snapshot().ConsumerGroup
	if !strings.HasPrefix(eff, "acl-") || len(eff) <= len("acl-") || !strings.Contains(v, eff) {
		t.Errorf("effective group %q not shown:\n%s", eff, v)
	}
	if !strings.Contains(v, "overridden by the prefix") {
		t.Error("Consumer group row should be marked as overridden")
	}
	if !strings.Contains(v, "apply to update") {
		t.Error("should note the effective group reflects the last applied settings while edits are unapplied")
	}
}

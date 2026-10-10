package kafka

import (
	"sync"
	"testing"

	"github.com/loicalleyne/bufka/internal/config"
)

func TestParseSeeds(t *testing.T) {
	for _, ok := range []string{"a:9092", "a:9092;b:9093", "a:9092, b:9093", "[::1]:9092;host.example.com:1", "a:9092;"} {
		if _, err := ParseSeeds(ok); err != nil {
			t.Errorf("ParseSeeds(%q) unexpected error: %v", ok, err)
		}
	}
	for _, bad := range []string{"", ";", "a", "a:", ":9092", "a:abc", "a:0", "a:65536", "a:9092;b", "a b:9092", "a:9092:1"} {
		if _, err := ParseSeeds(bad); err == nil {
			t.Errorf("ParseSeeds(%q) expected error", bad)
		}
	}
	got, _ := ParseSeeds(" a:1 ; b:2 ,c:3")
	if len(got) != 3 || got[0] != "a:1" || got[2] != "c:3" {
		t.Errorf("got %v", got)
	}
}

func TestParseMsgChanCap(t *testing.T) {
	for _, bad := range []string{"", "0", "-1", "x", "1.5"} {
		if _, err := ParseMsgChanCap(bad); err == nil {
			t.Errorf("ParseMsgChanCap(%q) expected error", bad)
		}
	}
	if n, err := ParseMsgChanCap(" 1 "); err != nil || n != 1 {
		t.Errorf("got %d %v", n, err)
	}
}

func TestParseTLS(t *testing.T) {
	for in, want := range map[string]bool{"t": true, "T": true, "true": true, "TRUE": true, "tRuE": true, "f": false, "F": false, "False": false, "fAlSe": false} {
		if got, err := ParseTLS(in); err != nil || got != want {
			t.Errorf("ParseTLS(%q) = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "yes", "1", "0", "tru", "on"} {
		if _, err := ParseTLS(bad); err == nil {
			t.Errorf("ParseTLS(%q) expected error", bad)
		}
	}
}

func TestEnvNames(t *testing.T) {
	for in, want := range map[string]string{"X": "X", "$X": "X", "${X}": "X", " $X ": "X", "": ""} {
		if got := NormalizeEnvName(in); got != want {
			t.Errorf("NormalizeEnvName(%q) = %q want %q", in, got, want)
		}
	}
	for _, bad := range []string{"1X", "A-B", "A B", "$$X"} {
		if ValidateEnvName(bad) == nil {
			t.Errorf("ValidateEnvName(%q) expected error", bad)
		}
	}
}

func TestResolveSettings(t *testing.T) {
	env := &config.Config{
		KafkaSeeds: "env:9092", KafkaMsgChanCap: "10", KafkaTLSEnabled: true,
		KafkaSASLUser: "envuser", KafkaSASLPassword: "envpass", KafkaInstancePrefix: "envinst",
	}
	vars := map[string]string{"MY_USER": "u", "MY_PASS": "p"}
	getenv := func(k string) string { return vars[k] }

	// Empty input → everything from the environment.
	s, err := ResolveSettings(SettingsInput{}, env, getenv)
	if err != nil || s.Seeds[0] != "env:9092" || s.MsgChanCap != 10 || !s.TLS || s.User != "envuser" || s.Password != "envpass" || s.InstancePrefix != "envinst" {
		t.Fatalf("env fallback wrong: %+v %v", s, err)
	}

	// Saved values win; SASL resolved through the named variables.
	s, err = ResolveSettings(SettingsInput{Seeds: "x:1;y:2", MsgChanCap: "5", TLS: "F", UserEnv: "$MY_USER", PasswordEnv: "${MY_PASS}", InstancePrefix: "mine"}, env, getenv)
	if err != nil || len(s.Seeds) != 2 || s.MsgChanCap != 5 || s.TLS || s.User != "u" || s.Password != "p" || s.InstancePrefix != "mine" {
		t.Fatalf("saved values wrong: %+v %v", s, err)
	}

	// Only one SASL value resolving violates the pairing rule.
	if _, err = ResolveSettings(SettingsInput{UserEnv: "MY_USER", PasswordEnv: "NOPE"}, env, getenv); err == nil {
		t.Fatal("expected pairing error when only one SASL value resolves")
	}
	// Both unset: allowed, but each is reported as a warning.
	s, err = ResolveSettings(SettingsInput{UserEnv: "NOPE_U", PasswordEnv: "NOPE_P"}, env, getenv)
	if err != nil || len(s.Warnings) != 2 {
		t.Fatalf("expected two unset warnings, got %+v %v", s, err)
	}

	for _, bad := range []SettingsInput{{Seeds: "nope"}, {MsgChanCap: "0"}, {TLS: "maybe"}, {UserEnv: "1bad"}} {
		if _, err := ResolveSettings(bad, env, getenv); err == nil {
			t.Errorf("expected error for %+v", bad)
		}
	}
}

func TestSettingsInputMapRoundTrip(t *testing.T) {
	in := SettingsInput{Seeds: "a:1", ConsumerGroup: "g", ConsumerGroupPrefix: "p", InstancePrefix: "i", MsgChanCap: "3", TLS: "t", UserEnv: "U", PasswordEnv: "P"}
	if got := SettingsInputFromMap(in.ToMap()); got != in {
		t.Fatalf("round trip: %+v", got)
	}
	if !(SettingsInput{}).IsZero() || in.IsZero() {
		t.Fatal("IsZero wrong")
	}
}

func TestApplySettingsAndSnapshotAreRaceFree(t *testing.T) {
	c := NewKafkaConfig()
	c.Seeds = []string{"old:1"}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.ApplySettings(Settings{Seeds: []string{"new:2"}, MsgChanCap: j + 1, TLS: j%2 == 0, ConsumerGroupPrefix: "p-"})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s := c.Snapshot()
				_ = s.Seeds
				_ = s.MsgChanCap
				_ = s.TLSDialer
			}
		}()
	}
	wg.Wait()

	// A snapshot is isolated from later edits.
	snap := c.Snapshot()
	c.ApplySettings(Settings{Seeds: []string{"later:3"}, MsgChanCap: 99})
	if snap.Seeds[0] == "later:3" || snap.MsgChanCap == 99 {
		t.Fatalf("snapshot changed after ApplySettings: %+v", snap)
	}
	if (*KafkaClientConf)(nil).Snapshot() != nil {
		t.Fatal("nil Snapshot must be nil")
	}
}

func TestApplySettingsDerivesGroupFromPrefix(t *testing.T) {
	c := NewKafkaConfig()
	c.ApplySettings(Settings{Seeds: []string{"a:1"}, MsgChanCap: 1, ConsumerGroup: "ignored", ConsumerGroupPrefix: "acl-"})
	g := c.Snapshot().ConsumerGroup
	if len(g) <= len("acl-") || g[:4] != "acl-" || g == "ignored" {
		t.Fatalf("group not derived from prefix: %q", g)
	}
	c.ApplySettings(Settings{Seeds: []string{"a:1"}, MsgChanCap: 1, ConsumerGroup: "plain"})
	if got := c.Snapshot().ConsumerGroup; got != "plain" {
		t.Fatalf("expected plain group, got %q", got)
	}
}

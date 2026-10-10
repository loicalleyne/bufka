package kafka

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/loicalleyne/bufka/internal/config"
)

// Settings is the editable subset of KafkaClientConf as resolved, validated
// values. Topics, mungers and the client count are deliberately excluded.
type Settings struct {
	Seeds               []string
	ConsumerGroup       string // raw; ApplySettings derives it from the prefix when one is set
	ConsumerGroupPrefix string
	InstancePrefix      string
	MsgChanCap          int
	TLS                 bool
	User                string
	Password            string
	// Warnings are non-fatal problems, e.g. a SASL env var that is unset.
	Warnings []string
}

// SettingsInput is the raw text of each editable field exactly as the user
// entered (and the database stores) it. An empty field means "use the
// environment default". UserEnv and PasswordEnv hold the NAME of an
// environment variable, never the credential itself.
type SettingsInput struct {
	Seeds               string
	ConsumerGroup       string
	ConsumerGroupPrefix string
	InstancePrefix      string
	MsgChanCap          string
	TLS                 string
	UserEnv             string
	PasswordEnv         string
}

// Keys used to persist a SettingsInput.
const (
	keySeeds       = "seeds"
	keyGroup       = "consumer_group"
	keyGroupPrefix = "consumer_group_prefix"
	keyInstance    = "instance_prefix"
	keyMsgChanCap  = "msg_chan_cap"
	keyTLS         = "tls_enabled"
	keyUserEnv     = "sasl_user_env"
	keyPasswordEnv = "sasl_password_env"
)

// ToMap converts in to the key/value form persisted in the database.
func (in SettingsInput) ToMap() map[string]string {
	return map[string]string{
		keySeeds: in.Seeds, keyGroup: in.ConsumerGroup, keyGroupPrefix: in.ConsumerGroupPrefix,
		keyInstance: in.InstancePrefix, keyMsgChanCap: in.MsgChanCap, keyTLS: in.TLS,
		keyUserEnv: in.UserEnv, keyPasswordEnv: in.PasswordEnv,
	}
}

// SettingsInputFromMap is the inverse of ToMap; unknown/missing keys are empty.
func SettingsInputFromMap(m map[string]string) SettingsInput {
	return SettingsInput{
		Seeds: m[keySeeds], ConsumerGroup: m[keyGroup], ConsumerGroupPrefix: m[keyGroupPrefix],
		InstancePrefix: m[keyInstance], MsgChanCap: m[keyMsgChanCap], TLS: m[keyTLS],
		UserEnv: m[keyUserEnv], PasswordEnv: m[keyPasswordEnv],
	}
}

// IsZero reports whether every field is empty (nothing saved).
func (in SettingsInput) IsZero() bool { return in == SettingsInput{} }

// ParseSeeds validates a list of host:port seed brokers separated by
// semicolons or commas and returns them trimmed.
func ParseSeeds(s string) ([]string, error) {
	var out []string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ';' || r == ',' }) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, port, err := net.SplitHostPort(part)
		if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " \t") {
			return nil, fmt.Errorf("%q is not a valid host:port", part)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("%q has an invalid port (must be 1-65535)", part)
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one host:port is required")
	}
	return out, nil
}

// ParseMsgChanCap validates a message channel capacity (an integer >= 1).
func ParseMsgChanCap(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 1 {
		return 0, fmt.Errorf("must be an integer >= 1")
	}
	return n, nil
}

// ParseTLS accepts t, f, true or false in any letter case.
func ParseTLS(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true":
		return true, nil
	case "f", "false":
		return false, nil
	}
	return false, fmt.Errorf("must be t, f, true or false (any case)")
}

var envNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NormalizeEnvName trims s and strips a leading "$" or a "${...}" wrapper so
// "$NAME", "${NAME}" and "NAME" are equivalent.
func NormalizeEnvName(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return strings.TrimSpace(s[2 : len(s)-1])
	}
	return strings.TrimSpace(strings.TrimPrefix(s, "$"))
}

// ValidateEnvName checks that s (after NormalizeEnvName) is a legal
// environment variable name. An empty name is valid and means "default".
func ValidateEnvName(s string) error {
	n := NormalizeEnvName(s)
	if n == "" || envNameRE.MatchString(n) {
		return nil
	}
	return fmt.Errorf("%q is not a valid environment variable name", n)
}

// ResolveSettings validates in and fills every empty field from env (the
// configuration loaded from the environment). SASL user/password are looked
// up through getenv by the variable names in in.UserEnv / in.PasswordEnv,
// falling back to env's KAFKA_SASL_USER / KAFKA_SASL_PASSWORD when empty.
func ResolveSettings(in SettingsInput, env *config.Config, getenv func(string) string) (Settings, error) {
	if env == nil {
		env = &config.Config{}
	}
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	pick := func(v, def string) string {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
		return def
	}

	var s Settings
	var err error
	if s.Seeds, err = ParseSeeds(pick(in.Seeds, env.KafkaSeeds)); err != nil {
		return Settings{}, fmt.Errorf("seeds: %w", err)
	}
	if s.MsgChanCap, err = ParseMsgChanCap(pick(in.MsgChanCap, env.KafkaMsgChanCap)); err != nil {
		return Settings{}, fmt.Errorf("msg chan capacity: %w", err)
	}
	if v := strings.TrimSpace(in.TLS); v != "" {
		if s.TLS, err = ParseTLS(v); err != nil {
			return Settings{}, fmt.Errorf("TLS enabled: %w", err)
		}
	} else {
		s.TLS = env.KafkaTLSEnabled
	}
	s.ConsumerGroup = pick(in.ConsumerGroup, env.KafkaConsumerGroup)
	s.ConsumerGroupPrefix = pick(in.ConsumerGroupPrefix, env.KafkaConsumerGroupPrefix)
	s.InstancePrefix = pick(in.InstancePrefix, env.KafkaInstancePrefix)

	resolve := func(label, nameIn, def string) (string, error) {
		if err := ValidateEnvName(nameIn); err != nil {
			return "", fmt.Errorf("%s: %w", label, err)
		}
		name := NormalizeEnvName(nameIn)
		if name == "" {
			return def, nil
		}
		v := getenv(name)
		if v == "" {
			s.Warnings = append(s.Warnings, fmt.Sprintf("$%s is not set", name))
		}
		return v, nil
	}
	if s.User, err = resolve("SASL user", in.UserEnv, env.KafkaSASLUser); err != nil {
		return Settings{}, err
	}
	if s.Password, err = resolve("SASL password", in.PasswordEnv, env.KafkaSASLPassword); err != nil {
		return Settings{}, err
	}
	if (s.User == "") != (s.Password == "") {
		return Settings{}, fmt.Errorf("SASL user and password must both be set or both be empty")
	}
	return s, nil
}

// OverrideConfig writes s over the matching fields of cfg so that
// NewKafkaConfigFromConfig builds a conf from the resolved settings.
func OverrideConfig(cfg *config.Config, s Settings) {
	cfg.KafkaSeeds = strings.Join(s.Seeds, ",")
	cfg.KafkaConsumerGroup = s.ConsumerGroup
	cfg.KafkaConsumerGroupPrefix = s.ConsumerGroupPrefix
	cfg.KafkaInstancePrefix = s.InstancePrefix
	cfg.KafkaMsgChanCap = strconv.Itoa(s.MsgChanCap)
	cfg.KafkaTLSEnabled = s.TLS
	cfg.KafkaSASLUser = s.User
	cfg.KafkaSASLPassword = s.Password
}

// ApplySettings updates c in place. It is safe to call while other
// goroutines use c through Snapshot; operations already running keep the
// settings they started with.
func (c *KafkaClientConf) ApplySettings(s Settings) {
	group := s.ConsumerGroup
	if s.ConsumerGroupPrefix != "" {
		group = s.ConsumerGroupPrefix + rand.Text()
	}
	var dialer *tls.Dialer
	if s.TLS {
		dialer = &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config:    &tls.Config{MinVersion: tls.VersionTLS12},
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.Seeds = append([]string(nil), s.Seeds...)
	c.ConsumerGroup = group
	c.ConsumerGroupPrefix = s.ConsumerGroupPrefix
	c.InstancePrefix = s.InstancePrefix
	c.MsgChanCap = s.MsgChanCap
	c.TLSDialer = dialer
	c.User = s.User
	c.Password = s.Password
}

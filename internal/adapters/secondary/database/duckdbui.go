package database

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/couac"
)

// DuckDBUISettingsTableName is the DuckDB table holding the DuckDB UI
// settings edited in the TUI (settings > duckdb ui), as key/value rows.
const DuckDBUISettingsTableName = "duckdb_ui_settings"

// DefaultDuckDBUIPort is DuckDB's default ui_local_port.
const DefaultDuckDBUIPort = 4213

var duckDBUISettingsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "key", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
}, nil)

// DuckDBUISettings controls DuckDB's built-in UI server. The server only
// listens on localhost; Port is its ui_local_port.
type DuckDBUISettings struct {
	Enabled bool
	Port    int
}

// URL is the address the UI is served at.
func (s DuckDBUISettings) URL() string {
	return fmt.Sprintf("http://localhost:%d/", s.Port)
}

// DuckDBUIFromConfig is the DuckDB UI setting from the environment:
// DUCKDB_UI_ENABLED (default off) on the port named in DUCKDB_UI_ADDR
// (default 4213, also used when the address names no valid port).
func DuckDBUIFromConfig(cfg *config.Config) DuckDBUISettings {
	s := DuckDBUISettings{Enabled: cfg.DuckDBUIEnabled, Port: DefaultDuckDBUIPort}
	if u, err := url.Parse(cfg.DuckDBUIAddr); err == nil && u.Port() != "" {
		if p, err := ValidateDuckDBUIPort(u.Port()); err == nil {
			s.Port = p
		}
	}
	return s
}

// ValidateDuckDBUIPort parses a port number in the unprivileged range.
func ValidateDuckDBUIPort(s string) (int, error) {
	p, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("port must be a number")
	}
	if p < 1024 || p > 65535 {
		return 0, fmt.Errorf("port must be between 1024 and 65535")
	}
	return p, nil
}

// WriteDuckDBUISettings replaces the saved DuckDB UI settings.
func WriteDuckDBUISettings(ctx context.Context, db *couac.DB, s DuckDBUISettings) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	b := array.NewRecordBuilder(memory.DefaultAllocator, duckDBUISettingsSchema)
	defer b.Release()
	kB := b.Field(0).(*array.StringBuilder)
	vB := b.Field(1).(*array.StringBuilder)
	kB.Append("enabled")
	vB.Append(strconv.FormatBool(s.Enabled))
	kB.Append("port")
	vB.Append(strconv.Itoa(s.Port))
	rec := b.NewRecordBatch()
	defer rec.Release()

	if _, err := conn.IngestReplace(ctx, DuckDBUISettingsTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", DuckDBUISettingsTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadDuckDBUISettings returns the saved settings. found is false when
// nothing was ever saved, in which case callers fall back to the
// environment. An unparsable saved port is reported as an error.
func ReadDuckDBUISettings(ctx context.Context, db *couac.DB) (s DuckDBUISettings, found bool, err error) {
	if db == nil {
		return s, false, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return s, false, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, DuckDBUISettingsTableName)
	if schema == nil {
		return s, false, nil
	}
	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT key, value FROM %q`, DuckDBUISettingsTableName))
	if err != nil {
		return s, false, fmt.Errorf("query %s: %w", DuckDBUISettingsTableName, err)
	}
	defer res.Close()

	kv := map[string]string{}
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		kc, vc := rec.Column(0).(*array.String), rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			kv[strings.Clone(kc.Value(i))] = strings.Clone(vc.Value(i))
		}
	}
	if err := res.Reader.Err(); err != nil {
		return s, false, fmt.Errorf("read %s: %w", DuckDBUISettingsTableName, err)
	}
	if len(kv) == 0 {
		return s, false, nil
	}
	s.Enabled, _ = strconv.ParseBool(kv["enabled"])
	s.Port = DefaultDuckDBUIPort
	if v, ok := kv["port"]; ok {
		if s.Port, err = ValidateDuckDBUIPort(v); err != nil {
			return DuckDBUISettings{}, false, fmt.Errorf("saved duckdb ui port %q: %w", v, err)
		}
	}
	return s, true, nil
}

// portFree reports whether nothing is listening on localhost:port. DuckDB's
// start_ui_server blocks indefinitely when its port is taken, so this is
// checked before starting.
func portFree(port int) error {
	l, err := net.Listen("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("port %d is already in use", port)
	}
	return l.Close()
}

// StartDuckDBUI starts DuckDB's UI server on localhost:port, stopping it
// first if it is already running (possibly on another port).
func StartDuckDBUI(ctx context.Context, db *couac.DB, port int) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Stopping a server that is not running is a no-op.
	if _, err := conn.Exec(ctx, `CALL stop_ui_server();`); err != nil {
		return fmt.Errorf("stop duckdb ui server: %w", err)
	}
	if err := portFree(port); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET ui_local_port = %d;`, port)); err != nil {
		return fmt.Errorf("set ui_local_port: %w", err)
	}
	if _, err := conn.Exec(ctx, startUIServerQry); err != nil {
		return fmt.Errorf("start duckdb ui server: %w", err)
	}
	return nil
}

// StopDuckDBUI stops DuckDB's UI server; a no-op when it is not running.
func StopDuckDBUI(ctx context.Context, db *couac.DB) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Exec(ctx, `CALL stop_ui_server();`); err != nil {
		return fmt.Errorf("stop duckdb ui server: %w", err)
	}
	return nil
}

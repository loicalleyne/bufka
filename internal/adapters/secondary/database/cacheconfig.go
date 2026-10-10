package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

// CacheConfigTableName is the DuckDB table name holding topic sync configurations.
const CacheConfigTableName = "kafka_cache_configs"

// CacheConfigRow defines one cached topic's configuration and status.
type CacheConfigRow struct {
	Topic               string
	Format              string
	MessageName         string
	ProtoFile           string
	ImportRoot          string
	CutConfluencePrefix bool
	StartFrom           string // "beginning" or "now"
	LimitType           string // "indefinitely", "duration", "count"
	LimitValue          string // duration string or integer count
	Active              bool
	EstimatedSize       int64
	CreatedAt           time.Time
}

var cacheConfigSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "format", Type: arrow.BinaryTypes.String},
	{Name: "message_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "proto_file", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "import_root", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cut_confluence_prefix", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "start_from", Type: arrow.BinaryTypes.String},
	{Name: "limit_type", Type: arrow.BinaryTypes.String},
	{Name: "limit_value", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "estimated_size", Type: arrow.PrimitiveTypes.Int64},
	{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us},
}, nil)

func buildCacheConfigRecord(rows []CacheConfigRow) arrow.RecordBatch {
	pool := memory.DefaultAllocator
	b := array.NewRecordBuilder(pool, cacheConfigSchema)
	defer b.Release()

	topicB := b.Field(0).(*array.StringBuilder)
	formatB := b.Field(1).(*array.StringBuilder)
	msgNameB := b.Field(2).(*array.StringBuilder)
	protoFileB := b.Field(3).(*array.StringBuilder)
	importRootB := b.Field(4).(*array.StringBuilder)
	cutConfB := b.Field(5).(*array.BooleanBuilder)
	startFromB := b.Field(6).(*array.StringBuilder)
	limitTypeB := b.Field(7).(*array.StringBuilder)
	limitValB := b.Field(8).(*array.StringBuilder)
	activeB := b.Field(9).(*array.BooleanBuilder)
	estSizeB := b.Field(10).(*array.Int64Builder)
	createdAtB := b.Field(11).(*array.TimestampBuilder)

	for _, r := range rows {
		topicB.Append(r.Topic)
		formatB.Append(r.Format)
		if r.MessageName != "" {
			msgNameB.Append(r.MessageName)
		} else {
			msgNameB.AppendNull()
		}
		if r.ProtoFile != "" {
			protoFileB.Append(r.ProtoFile)
		} else {
			protoFileB.AppendNull()
		}
		if r.ImportRoot != "" {
			importRootB.Append(r.ImportRoot)
		} else {
			importRootB.AppendNull()
		}
		cutConfB.Append(r.CutConfluencePrefix)
		startFromB.Append(r.StartFrom)
		limitTypeB.Append(r.LimitType)
		if r.LimitValue != "" {
			limitValB.Append(r.LimitValue)
		} else {
			limitValB.AppendNull()
		}
		activeB.Append(r.Active)
		estSizeB.Append(r.EstimatedSize)
		ts, _ := arrow.TimestampFromTime(r.CreatedAt, arrow.Microsecond)
		createdAtB.Append(ts)
	}

	return b.NewRecordBatch()
}

// EnsureCacheConfigTable ensures that the kafka_cache_configs table exists in db.
func EnsureCacheConfigTable(ctx context.Context, db *couac.DB) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, CacheConfigTableName)
	if schema != nil {
		return nil
	}
	rec := buildCacheConfigRecord(nil)
	defer rec.Release()
	if _, err := conn.IngestReplace(ctx, CacheConfigTableName, rec); err != nil {
		return fmt.Errorf("init %s: %w", CacheConfigTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// WriteCacheConfigs replaces the cache configuration table with rows.
func WriteCacheConfigs(ctx context.Context, db *couac.DB, rows []CacheConfigRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if len(rows) == 0 {
		rec := buildCacheConfigRecord(nil)
		defer rec.Release()
		if _, err := conn.IngestReplace(ctx, CacheConfigTableName, rec); err != nil {
			return fmt.Errorf("create empty %s: %w", CacheConfigTableName, err)
		}
		_ = db.Checkpoint(ctx)
		return nil
	}

	rec := buildCacheConfigRecord(rows)
	defer rec.Release()

	if _, err := conn.IngestReplace(ctx, CacheConfigTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", CacheConfigTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadCacheConfigs returns all saved topic cache configurations.
func ReadCacheConfigs(ctx context.Context, db *couac.DB) ([]CacheConfigRow, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, CacheConfigTableName)
	if schema == nil {
		return nil, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT topic, format, message_name, proto_file, import_root,
		cut_confluence_prefix, start_from, limit_type, limit_value, active, estimated_size, created_at
		FROM %q ORDER BY topic`, CacheConfigTableName))
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", CacheConfigTableName, err)
	}
	defer res.Close()

	var rows []CacheConfigRow
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		topicCol := rec.Column(0).(*array.String)
		formatCol := rec.Column(1).(*array.String)
		msgNameCol := rec.Column(2).(*array.String)
		protoFileCol := rec.Column(3).(*array.String)
		importRootCol := rec.Column(4).(*array.String)
		cutConfCol := rec.Column(5).(*array.Boolean)
		startFromCol := rec.Column(6).(*array.String)
		limitTypeCol := rec.Column(7).(*array.String)
		limitValCol := rec.Column(8).(*array.String)
		activeCol := rec.Column(9).(*array.Boolean)
		estSizeCol := rec.Column(10).(*array.Int64)
		createdAtCol := rec.Column(11).(*array.Timestamp)

		for i := 0; i < int(rec.NumRows()); i++ {
			msgName := ""
			if msgNameCol.IsValid(i) {
				msgName = strings.Clone(msgNameCol.Value(i))
			}
			protoFile := ""
			if protoFileCol.IsValid(i) {
				protoFile = strings.Clone(protoFileCol.Value(i))
			}
			importRoot := ""
			if importRootCol.IsValid(i) {
				importRoot = strings.Clone(importRootCol.Value(i))
			}
			limitVal := ""
			if limitValCol.IsValid(i) {
				limitVal = strings.Clone(limitValCol.Value(i))
			}

			rows = append(rows, CacheConfigRow{
				Topic:               strings.Clone(topicCol.Value(i)),
				Format:              strings.Clone(formatCol.Value(i)),
				MessageName:         msgName,
				ProtoFile:           protoFile,
				ImportRoot:          importRoot,
				CutConfluencePrefix: cutConfCol.Value(i),
				StartFrom:           strings.Clone(startFromCol.Value(i)),
				LimitType:           strings.Clone(limitTypeCol.Value(i)),
				LimitValue:          limitVal,
				Active:              activeCol.Value(i),
				EstimatedSize:       estSizeCol.Value(i),
				CreatedAt:           createdAtCol.Value(i).ToTime(arrow.Microsecond),
			})
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", CacheConfigTableName, err)
	}
	return rows, nil
}

// UpsertCacheConfig updates or inserts a topic cache configuration.
// If an existing row with the same topic is found, non-empty fields in row
// update the existing configuration while preserving previously populated fields
// (such as ProtoFile, MessageName, ImportRoot).
func UpsertCacheConfig(ctx context.Context, db *couac.DB, row CacheConfigRow) error {
	existing, err := ReadCacheConfigs(ctx, db)
	if err != nil {
		return err
	}

	found := false
	for i, r := range existing {
		if r.Topic == row.Topic {
			merged := r
			merged.Active = row.Active
			if row.Format != "" {
				merged.Format = row.Format
			}
			if row.MessageName != "" {
				merged.MessageName = row.MessageName
			}
			if row.ProtoFile != "" {
				merged.ProtoFile = row.ProtoFile
			}
			if row.ImportRoot != "" {
				merged.ImportRoot = row.ImportRoot
			}
			if row.StartFrom != "" {
				merged.StartFrom = row.StartFrom
			}
			if row.LimitType != "" {
				merged.LimitType = row.LimitType
			}
			if row.LimitValue != "" {
				merged.LimitValue = row.LimitValue
			}
			if row.EstimatedSize > 0 {
				merged.EstimatedSize = row.EstimatedSize
			}
			existing[i] = merged
			found = true
			break
		}
	}
	if !found {
		existing = append(existing, row)
	}

	return WriteCacheConfigs(ctx, db, existing)
}

// DeleteCacheConfig deletes a topic cache configuration.
func DeleteCacheConfig(ctx context.Context, db *couac.DB, topic string) error {
	existing, err := ReadCacheConfigs(ctx, db)
	if err != nil {
		return err
	}

	filtered := make([]CacheConfigRow, 0, len(existing))
	for _, r := range existing {
		if r.Topic != topic {
			filtered = append(filtered, r)
		}
	}

	return WriteCacheConfigs(ctx, db, filtered)
}

// GetResumeOffsets returns the maximum consumed offset for each partition in table.
// If the table doesn't exist or holds no records, an empty map is returned.
func GetResumeOffsets(ctx context.Context, db *couac.DB, table string) (map[int32]int64, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, table)
	if schema == nil {
		return map[int32]int64{}, nil
	}

	// Current tables use _kafka_partition/_kafka_offset. Tables written by
	// older versions may also (or only) carry kafka_partition/kafka_offset or
	// partition/offset; rows from all present column pairs are combined.
	// Tables with none of these return empty; their offsets live in
	// kafka_cache_offsets.
	cols := make(map[string]bool, len(schema.Fields()))
	for _, f := range schema.Fields() {
		cols[f.Name] = true
	}
	var selects []string
	for _, pair := range [][2]string{
		{"_kafka_partition", "_kafka_offset"},
		{"kafka_partition", "kafka_offset"},
		{"partition", "offset"},
	} {
		if cols[pair[0]] && cols[pair[1]] {
			selects = append(selects, fmt.Sprintf(`SELECT %q AS p, %q AS o FROM %q`, pair[0], pair[1], table))
		}
	}
	if len(selects) == 0 {
		return map[int32]int64{}, nil
	}

	query := fmt.Sprintf(`SELECT p::INTEGER, MAX(o)::BIGINT FROM (%s) WHERE p IS NOT NULL GROUP BY p`, strings.Join(selects, " UNION ALL "))
	res, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query partition offsets from %s: %w", table, err)
	}
	defer res.Close()

	offsets := make(map[int32]int64)
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		part := rec.Column(0).(*array.Int32)
		off := rec.Column(1).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			if part.IsValid(i) && off.IsValid(i) {
				offsets[part.Value(i)] = off.Value(i)
			}
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read offsets from %s: %w", table, err)
	}
	return offsets, nil
}

// GetTableStats returns row count and estimated disk space in bytes for table.
func GetTableStats(ctx context.Context, db *couac.DB, table string) (rowCount int64, diskBytes int64, err error) {
	if db == nil {
		return 0, 0, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return 0, 0, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, table)
	if schema == nil {
		return 0, 0, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT count(*) FROM %q`, table))
	if err != nil {
		return 0, 0, fmt.Errorf("count rows in %s: %w", table, err)
	}
	defer res.Close()

	if res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		cntCol := rec.Column(0).(*array.Int64)
		if cntCol.IsValid(0) {
			rowCount = cntCol.Value(0)
		}
	}

	// Approximate 256 bytes per row on disk as baseline estimate for storage
	diskBytes = rowCount * 256
	return rowCount, diskBytes, nil
}

// CacheOffsetsTableName is the DuckDB table holding the last consumed offset
// per partition for each cached topic. Protobuf cache tables carry only the
// decoded message fields, so resume positions are tracked here.
const CacheOffsetsTableName = "kafka_cache_offsets"

func ensureCacheOffsetsTable(ctx context.Context, conn *couac.Conn) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (topic VARCHAR, "partition" INTEGER, "offset" BIGINT)`, CacheOffsetsTableName))
	if err != nil {
		return fmt.Errorf("create %s: %w", CacheOffsetsTableName, err)
	}
	return nil
}

// SaveCacheOffsets records offsets (the last consumed offset per partition)
// for topic, keeping the higher of the stored and new offset per partition.
func SaveCacheOffsets(ctx context.Context, db *couac.DB, topic string, offsets map[int32]int64) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	if len(offsets) == 0 {
		return nil
	}
	existing, err := ReadCacheOffsets(ctx, db, topic)
	if err != nil {
		return err
	}
	for p, off := range offsets {
		if cur, ok := existing[p]; !ok || off > cur {
			existing[p] = off
		}
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := ensureCacheOffsetsTable(ctx, conn); err != nil {
		return err
	}
	var vals []string
	for p, off := range existing {
		vals = append(vals, fmt.Sprintf("(%s, %d, %d)", sqlString(topic), p, off))
	}
	q := fmt.Sprintf(`BEGIN; DELETE FROM %q WHERE topic = %s; INSERT INTO %q VALUES %s; COMMIT;`,
		CacheOffsetsTableName, sqlString(topic), CacheOffsetsTableName, strings.Join(vals, ", "))
	if _, err := conn.Exec(ctx, q); err != nil {
		_, _ = conn.Exec(ctx, "ROLLBACK")
		return fmt.Errorf("save offsets for %s: %w", topic, err)
	}
	return nil
}

// ReadCacheOffsets returns the stored last consumed offset per partition for topic.
func ReadCacheOffsets(ctx context.Context, db *couac.DB, topic string) (map[int32]int64, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	offsets := make(map[int32]int64)
	if schema, _ := conn.GetTableSchema(ctx, nil, nil, CacheOffsetsTableName); schema == nil {
		return offsets, nil
	}
	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT "partition", "offset" FROM %q WHERE topic = %s`,
		CacheOffsetsTableName, sqlString(topic)))
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", CacheOffsetsTableName, err)
	}
	defer res.Close()
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		part := rec.Column(0).(*array.Int32)
		off := rec.Column(1).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			if part.IsValid(i) && off.IsValid(i) {
				offsets[part.Value(i)] = off.Value(i)
			}
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", CacheOffsetsTableName, err)
	}
	return offsets, nil
}

// DeleteCacheOffsets removes the stored offsets for topic.
func DeleteCacheOffsets(ctx context.Context, db *couac.DB, topic string) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if schema, _ := conn.GetTableSchema(ctx, nil, nil, CacheOffsetsTableName); schema == nil {
		return nil
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %q WHERE topic = %s`, CacheOffsetsTableName, sqlString(topic))); err != nil {
		return fmt.Errorf("delete offsets for %s: %w", topic, err)
	}
	return nil
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/topicsfile"
	"github.com/loicalleyne/couac"
)

// MigrateTopicsToml imports topic schemas from tomlPath into the DuckDB table kafka_topic_schemas.
// If tomlPath exists and is successfully migrated, it is renamed to tomlPath+".bak" so it will not
// be re-imported on subsequent runs.
func MigrateTopicsToml(ctx context.Context, db *couac.DB, tomlPath string) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("duckdb is nil")
	}
	if tomlPath == "" {
		return 0, nil
	}

	info, err := os.Stat(tomlPath)
	if errors.Is(err, os.ErrNotExist) || info == nil {
		return 0, nil
	}

	f, err := topicsfile.Load(tomlPath)
	if err != nil {
		return 0, fmt.Errorf("load %s: %w", tomlPath, err)
	}

	topics := f.CopyTopics()
	if len(topics) == 0 {
		_ = os.Rename(tomlPath, tomlPath+".bak")
		return 0, nil
	}

	rows := make([]TopicSchemaRow, 0, len(topics))
	now := time.Now().UTC()
	for topicName, t := range topics {
		updatedAt := now
		if t.SampledAt != "" {
			if parsed, pErr := time.Parse(time.RFC3339, t.SampledAt); pErr == nil {
				updatedAt = parsed
			}
		}

		rows = append(rows, TopicSchemaRow{
			Topic:               topicName,
			Table:               t.Table,
			Format:              t.Format,
			MessageName:         t.MessageName,
			ProtoFile:           t.ProtoFile,
			ImportRoot:          t.ImportRoot,
			CutConfluencePrefix: t.CutConfluencePrefix,
			MessageOverride:     t.MessageOverride,
			SampleCount:         int32(t.SampleCount),
			MatchedCount:        int32(t.MatchedCount),
			UpdatedAt:           updatedAt,
		})
	}

	if err := UpsertTopicSchemas(ctx, db, rows); err != nil {
		return 0, fmt.Errorf("upsert topic schemas: %w", err)
	}

	_ = os.Rename(tomlPath, tomlPath+".bak")
	return len(rows), nil
}

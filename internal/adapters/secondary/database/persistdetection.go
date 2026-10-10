package database

import (
	"context"
	"time"

	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/bufka/pkg/schemafield"
	"github.com/loicalleyne/couac"
)

// TopicSchemaRowFromResult builds the kafka_topic_schemas row for a detection result.
func TopicSchemaRowFromResult(topic, table string, res topicsample.TopicResult) TopicSchemaRow {
	row := TopicSchemaRow{
		Topic:               topic,
		Table:               table,
		Format:              res.Format,
		MessageName:         res.MessageName,
		ProtoFile:           res.File,
		ImportRoot:          res.Root,
		CutConfluencePrefix: res.CutConfluencePrefix,
		SampleCount:         int32(res.SampleCount),
		MatchedCount:        int32(res.MatchedCount),
		UpdatedAt:           time.Now().UTC(),
		ResolvedBy:          res.ResolvedBy,
		Warnings:            res.Warnings,
	}
	if res.Spec != nil {
		row.SpecFile = res.Spec.File
		row.SpecMessage = res.Spec.Message
	}
	return row
}

// topicFieldsFromResult picks the fields to document for a detection result:
// the matched AsyncAPI message for JSON, the proto message's fields for protobuf.
func topicFieldsFromResult(topic string, res topicsample.TopicResult, specs *asyncapi.Index,
	messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo],
) (source, message string, fields []schemafield.FieldDef, ok bool) {
	if res.Spec != nil && res.Format == topicsample.FormatJSON && specs != nil {
		ts, found := specs.Lookup(topic)
		if !found {
			return "", "", nil, false
		}
		for _, m := range ts.Messages {
			if m.Name == res.Spec.Message {
				return string(schemafield.SourceAsyncAPI), m.Name, m.Fields, true
			}
		}
		return "", "", nil, false
	}
	if res.Format == topicsample.FormatProtobuf && res.MessageName != "" && messages != nil {
		if info, found := messages.Get(res.MessageName); found {
			return string(schemafield.SourceProto), res.MessageName, info.FieldDefs(), true
		}
	}
	return "", "", nil, false
}

// SaveTopicFields replaces the topic's kafka_topic_fields from a detection
// result, unless the topic has a manual message override (those keep the
// fields they have). Results with nothing to document leave the table alone.
// specs and messages may be nil.
func SaveTopicFields(ctx context.Context, db *couac.DB, topic string, res topicsample.TopicResult,
	specs *asyncapi.Index, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo],
) error {
	source, msg, fields, ok := topicFieldsFromResult(topic, res, specs, messages)
	if !ok {
		return nil
	}
	existing, err := ReadTopicSchemas(ctx, db)
	if err != nil {
		return err
	}
	if prev, found := existing[topic]; found && prev.MessageOverride {
		return nil
	}
	return ReplaceTopicFields(ctx, db, topic, source, msg, fields)
}

// SaveDetection persists a detection result: it upserts the kafka_topic_schemas
// row (UpsertTopicSchemas keeps manual message overrides) and then saves the
// topic's fields via SaveTopicFields.
func SaveDetection(ctx context.Context, db *couac.DB, topic, table string, res topicsample.TopicResult,
	specs *asyncapi.Index, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo],
) error {
	if err := SaveTopicFields(ctx, db, topic, res, specs, messages); err != nil {
		return err
	}
	return UpsertTopicSchemas(ctx, db, []TopicSchemaRow{TopicSchemaRowFromResult(topic, table, res)})
}

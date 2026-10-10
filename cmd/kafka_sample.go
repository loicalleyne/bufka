/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	tinydecideadapter "github.com/loicalleyne/bufka/internal/adapters/secondary/tinydecide"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/autoscale"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/couac"
	"github.com/spf13/cobra"
)

var (
	sampleCount         int
	sampleTimeout       time.Duration
	sampleTinyDecideDir string
)

// kafkaSampleMessagesCmd samples recent messages from Kafka topics and
// detects their encoding.
var kafkaSampleMessagesCmd = &cobra.Command{
	Use:   "sample-messages <topic>|all",
	Short: "Sample recent messages from a topic (or all topics) and detect their schema",
	Long: `Read the newest --count messages from <topic>, or from every topic when the
argument is "all", and store them with their headers in the DuckDB table
<topic table>_sample (replaced on every run; needs a database file, not DUCKDB_PATH=:memory:).

Each topic's encoding is detected: whether values carry a Confluent Schema
Registry header, and which protobuf message compiled from the
semicolon-separated PROTO_IMPORT_PATHS decodes every sample cleanly, falling
back to json and then unknown. Results are stored in DuckDB (kafka_topic_schemas).

With "all", topics are sampled concurrently: 8 at a time at first, adjusted in
steps of 8 up to 64 according to throughput (topics finished per second).

When --tinydecide-model is set and the match between two or more candidate
message types is a genuine structural tie, TinyDecide (a small zero-shot
decision model) is asked which one the decoded message content best matches.
Its answer is advisory only — it is never applied automatically — and is
recorded alongside the structural pick for manual review.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runSampleMessages(ctx, args[0])
	},
}

func runSampleMessages(ctx context.Context, target string) error {
	cfg := GetConfig()
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("sample-messages needs a database file (DUCKDB_PATH is :memory:)")
	}
	if sampleCount <= 0 {
		return fmt.Errorf("--count must be greater than zero")
	}
	kafkaConf, err := kafka.NewKafkaConfigFromConfig(cfg)
	if err != nil {
		return err
	}

	var messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]
	if len(cfg.ProtoImportPaths) == 0 {
		log.Printf("PROTO_IMPORT_PATHS is not set; only json and unknown can be detected")
	} else {
		var failures []protoinspect.FileError
		messages, failures, err = protoinspect.LoadMessages(cfg.ProtoImportPaths, cfg.ProtoIncludePaths...)
		if err != nil {
			return err
		}
		for _, f := range failures {
			log.Printf("skipped %v", f)
		}
		log.Printf("loaded %d protobuf messages from %s", messages.Len(), strings.Join(cfg.ProtoImportPaths, ";"))
	}

	var specs *asyncapi.Index
	if len(cfg.AsyncAPIPaths) > 0 {
		var loadErrs []asyncapi.LoadError
		specs, loadErrs = asyncapi.Load(cfg.AsyncAPIPaths)
		for _, e := range loadErrs {
			log.Printf("skipped AsyncAPI spec %v", e)
		}
	}

	var advisor topicsample.Advisor
	if sampleTinyDecideDir != "" {
		a, err := tinydecideadapter.Load(sampleTinyDecideDir)
		if err != nil {
			return err
		}
		advisor = a
		log.Printf("loaded TinyDecide model from %s; will review structural schema ties", sampleTinyDecideDir)
	}

	db, err := database.InitDB(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_ = database.EnsureTopicSchemaTable(ctx, db)
	_ = database.EnsureTopicFieldsTable(ctx, db)

	topics := []string{target}
	if target == "all" {
		if topics, err = kafka.ListTopics(ctx, kafkaConf); err != nil {
			return err
		}
	}

	// Sample topics concurrently (autoscaled from 8 to 64 by throughput, see
	// pkg/autoscale); results are kept in topic order so the printed table is
	// stable.
	type result struct {
		entry topicsample.TopicResult
		sRow  database.TopicSchemaRow
		ok    bool
	}
	results := make([]result, len(topics))
	var dbMu sync.Mutex
	pool := autoscale.NewPool()
	for i, topic := range topics {
		if pool.Go(ctx, func() {
			if ctx.Err() != nil {
				return
			}
			entry, sRow, err := sampleTopic(ctx, kafkaConf, db, &dbMu, messages, specs, advisor, topic)
			if err != nil {
				log.Printf("sample %q: %v", topic, err)
				return
			}
			results[i] = result{entry: entry, sRow: sRow, ok: true}
		}) != nil {
			break
		}
	}
	pool.Wait()

	var schemaRows []database.TopicSchemaRow
	var printRows []schemaRow
	for i, topic := range topics {
		if !results[i].ok {
			continue
		}
		entry, sRow := results[i].entry, results[i].sRow
		schemaRows = append(schemaRows, sRow)
		printRows = append(printRows, schemaRow{
			Topic:       topic,
			Format:      entry.Format,
			MessageName: entry.MessageName,
			Cut:         entry.CutConfluencePrefix,
			Indexes:     entry.MessageIndexes,
			Matched:     entry.MatchedCount,
			Sample:      entry.SampleCount,
		})
		if entry.Reviewed {
			if entry.ReviewPick == "" {
				log.Printf("%s: TinyDecide reviewed the tie between %v but had no opinion", topic,
					append([]string{entry.MessageName}, entry.Candidates...))
			} else if !entry.ReviewAgrees {
				log.Printf("%s: TinyDecide disagrees with the structural pick %q, preferring %q (confidence %.3f) — review manually",
					topic, entry.MessageName, entry.ReviewPick, entry.ReviewConfidence)
			} else {
				log.Printf("%s: TinyDecide agrees with %q (confidence %.3f)", topic, entry.MessageName, entry.ReviewConfidence)
			}
		}
	}
	if err := printSchemaTable(printRows); err != nil {
		return err
	}

	if len(schemaRows) > 0 && db != nil {
		if err := database.UpsertTopicSchemas(ctx, db, schemaRows); err != nil {
			log.Printf("save topic schemas to duckdb: %v", err)
		}
	}
	return ctx.Err()
}

// sampleTopic samples one topic, writes its _sample table and returns its detection result.
// advisor may be nil, in which case structural ties are left unreviewed.
// The Kafka read and detection run unlocked; dbMu serializes the DuckDB writes.
func sampleTopic(ctx context.Context, conf *kafka.KafkaClientConf, db *couac.DB, dbMu *sync.Mutex,
	messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], specs *asyncapi.Index, advisor topicsample.Advisor, topic string,
) (topicsample.TopicResult, database.TopicSchemaRow, error) {
	rows, table, res, err := detectTopicSample(ctx, conf, messages, specs, advisor, topic, sampleTimeout)
	if err != nil {
		return topicsample.TopicResult{}, database.TopicSchemaRow{}, err
	}
	dbMu.Lock()
	defer dbMu.Unlock()
	if err := database.WriteSampleTable(ctx, db, table, rows); err != nil {
		return topicsample.TopicResult{}, database.TopicSchemaRow{}, err
	}
	sRow := database.TopicSchemaRowFromResult(topic, table, res)
	if db != nil {
		if err := database.SaveTopicFields(ctx, db, topic, res, specs, messages); err != nil {
			log.Printf("save topic fields %q: %v", topic, err)
		}
	}
	return res, sRow, nil
}

// detectTopicSample reads a topic, detects its schema and builds the sample
// rows and detection result, without touching the database. timeout bounds the
// Kafka read. The returned table is empty when no records were sampled.
// advisor may be nil, in which case structural ties are left unreviewed.
func detectTopicSample(ctx context.Context, conf *kafka.KafkaClientConf,
	messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], specs *asyncapi.Index, advisor topicsample.Advisor,
	topic string, timeout time.Duration,
) ([]database.SampleRow, string, topicsample.TopicResult, error) {
	table, err := kafkaingest.SampleTableName(topic)
	if err != nil {
		return nil, "", topicsample.TopicResult{}, err
	}
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	records, err := kafka.SampleTopic(sampleCtx, conf, topic, sampleCount)
	cancel()
	switch {
	case errors.Is(err, kafka.ErrTopicEmpty):
		log.Printf("sample %q: %v", topic, err)
	case err != nil:
		return nil, "", topicsample.TopicResult{}, err
	case len(records) == 0:
		log.Printf("sample %q: no records received within timeout %s", topic, timeout)
	}

	values := make([][]byte, len(records))
	for i, r := range records {
		values[i] = r.Value
	}
	res := topicsample.Detect(topic, values, messages, specs, advisor)

	rows := make([]database.SampleRow, len(records))
	for i, r := range records {
		headers := make([]database.SampleHeader, len(r.Headers))
		for j, h := range r.Headers {
			headers[j] = database.SampleHeader{Key: h.Key, Value: h.Value}
		}
		d := res.Rows[i]
		rows[i] = database.SampleRow{
			Topic:                   r.Topic,
			Partition:               r.Partition,
			Offset:                  r.Offset,
			Timestamp:               r.Timestamp,
			Key:                     r.Key,
			Value:                   r.Value,
			Headers:                 headers,
			DetectedFormat:          res.Format,
			MessageName:             res.MessageName,
			CutConfluencePrefix:     res.CutConfluencePrefix,
			ConfluentHeaderLen:      int32(d.HeaderLen),
			ConfluentSchemaID:       d.SchemaID,
			ConfluentMessageIndexes: d.MessageIndexes,
			DecodeOK:                d.DecodeOK,
			DecodedJSON:             d.Decoded,
			Reviewed:                res.Reviewed,
			ReviewPick:              res.ReviewPick,
			ReviewConfidence:        res.ReviewConfidence,
			ReviewAgrees:            res.ReviewAgrees,
		}
	}
	if len(rows) == 0 {
		table = ""
	}

	return rows, table, res, nil
}

func init() {
	kafkaSampleMessagesCmd.Flags().IntVar(&sampleCount, "count", 50, "number of newest messages to sample per topic")
	kafkaSampleMessagesCmd.Flags().DurationVar(&sampleTimeout, "timeout", 30*time.Second, "maximum time spent reading each topic")
	kafkaSampleMessagesCmd.Flags().StringVar(&sampleTinyDecideDir, "tinydecide-model", "", "directory with meta.json/model.bin; when set, TinyDecide reviews structural schema-match ties (advisory only)")
	kafkaCmd.AddCommand(kafkaSampleMessagesCmd)
}

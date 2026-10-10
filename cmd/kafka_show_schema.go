/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	tinydecideadapter "github.com/loicalleyne/bufka/internal/adapters/secondary/tinydecide"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/couac"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	showSchemaTinyDecideDir string
	showSchemaConcurrency   int
)

// emptyResampleTimeout bounds the re-sampling of a topic that held no samples
// at its last sampling. Kept short so showing many topics stays fast.
const emptyResampleTimeout = time.Second

// kafkaShowSchemaCmd shows the detected schema of a topic (or all topics) from
// what sample-messages already stored, re-sampling only topics that have no
// stored samples.
var kafkaShowSchemaCmd = &cobra.Command{
	Use:   "show-schema <topic>|all",
	Short: "Show the detected schema of a topic (or all topics) without re-sampling stored topics",
	Long: `Show, for <topic> or for every topic when the argument is "all", the schema
that "kafka sample-messages" detected, reading what is already stored instead of
re-sampling. The summary is the same one sample-messages prints.

A topic whose _sample table in DuckDB already holds rows is shown from that
stored sample, with no Kafka access. A topic that was empty at its last
sampling (no stored rows) is re-sampled once, with a short timeout of
` + emptyResampleTimeout.String() + `, and its result is stored in DuckDB. Such
re-samples run concurrently (see --concurrency).

Needs a database file. Run "kafka sample-messages" first to populate the
stored samples.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runShowSchema(ctx, args[0])
	},
}

func runShowSchema(ctx context.Context, target string) error {
	cfg := GetConfig()
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("show-schema needs a database file (DUCKDB_PATH is :memory:)")
	}
	kafkaConf, err := kafka.NewKafkaConfigFromConfig(cfg)
	if err != nil {
		return err
	}

	var messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]
	if len(cfg.ProtoImportPaths) == 0 {
		log.Printf("PROTO_IMPORT_PATHS is not set; only json and unknown can be detected when re-sampling")
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
	if showSchemaTinyDecideDir != "" {
		a, err := tinydecideadapter.Load(showSchemaTinyDecideDir)
		if err != nil {
			return err
		}
		advisor = a
		log.Printf("loaded TinyDecide model from %s; will review structural schema ties", showSchemaTinyDecideDir)
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
	sort.Strings(topics)

	// Decide per topic whether its stored sample can be shown as-is or whether
	// it must be re-sampled because it held no rows last time.
	summaries := make(map[string]database.SampleSummary, len(topics))
	var toResample []string
	for _, topic := range topics {
		if ctx.Err() != nil {
			break
		}
		table, err := kafkaingest.SampleTableName(topic)
		if err != nil {
			log.Printf("show-schema %q: %v", topic, err)
			continue
		}
		summary, ok, err := database.ReadSampleSummary(ctx, db, table)
		if err != nil {
			log.Printf("show-schema %q: %v", topic, err)
			continue
		}
		if ok {
			summaries[topic] = summary
			continue
		}
		toResample = append(toResample, topic)
	}

	updates, results, err := resampleEmptyTopics(ctx, kafkaConf, db, messages, specs, advisor, toResample)
	if err != nil {
		return err
	}
	var schemaRows []database.TopicSchemaRow
	for topic, entry := range updates {
		summaries[topic] = database.SampleSummary{
			Format:              entry.Format,
			MessageName:         entry.MessageName,
			CutConfluencePrefix: entry.CutConfluencePrefix,
			MessageIndexes:      entry.MessageIndexes,
			MatchedCount:        entry.MatchedCount,
			SampleCount:         entry.SampleCount,
		}
		table, _ := kafkaingest.SampleTableName(topic)
		schemaRows = append(schemaRows, database.TopicSchemaRowFromResult(topic, table, results[topic]))
	}

	// Documentation persisted by sample-messages / earlier re-samples; the
	// freshly re-sampled topics' rows are not yet upserted, so overlay them.
	stored, _ := database.ReadTopicSchemas(ctx, db)
	for _, r := range schemaRows {
		stored[r.Topic] = r
	}

	var printRows []schemaRow
	var details []schemaDetail
	for _, topic := range topics {
		s, ok := summaries[topic]
		if !ok {
			continue
		}
		sr := stored[topic]
		fields, _ := database.ReadTopicFields(ctx, db, topic)
		spec := sr.SpecFile
		if spec != "" && sr.SpecMessage != "" {
			spec += ":" + sr.SpecMessage
		}
		details = append(details, schemaDetail{Topic: topic, Warnings: sr.Warnings, Fields: fields})
		printRows = append(printRows, schemaRow{
			ResolvedBy:  sr.ResolvedBy,
			Spec:        spec,
			Warnings:    len(sr.Warnings),
			Fields:      len(fields),
			Topic:       topic,
			Format:      s.Format,
			MessageName: s.MessageName,
			Cut:         s.CutConfluencePrefix,
			Indexes:     s.MessageIndexes,
			Matched:     s.MatchedCount,
			Sample:      s.SampleCount,
		})
	}
	if err := printSchemaTableDocs(printRows, true); err != nil {
		return err
	}
	if err := printSchemaDetails(os.Stdout, details); err != nil {
		return err
	}

	if len(schemaRows) > 0 {
		if err := database.UpsertTopicSchemas(ctx, db, schemaRows); err != nil {
			log.Printf("save topic schemas to duckdb: %v", err)
		}
	}
	return ctx.Err()
}

// resampleEmptyTopics re-samples the given topics concurrently (bounded by
// --concurrency), writes each one's _sample table and returns their
// detection results. The Kafka read of each topic is bounded by emptyResampleTimeout.
func resampleEmptyTopics(ctx context.Context, conf *kafka.KafkaClientConf, db *couac.DB,
	messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], specs *asyncapi.Index, advisor topicsample.Advisor, topics []string,
) (map[string]topicsample.TopicResult, map[string]topicsample.TopicResult, error) {
	updates := make(map[string]topicsample.TopicResult, len(topics))
	resolved := make(map[string]topicsample.TopicResult, len(topics))
	if len(topics) == 0 {
		return updates, resolved, nil
	}
	log.Printf("re-sampling %d topic(s) with no stored samples (timeout %s)", len(topics), emptyResampleTimeout)

	concurrency := showSchemaConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, topic := range topics {
		topic := topic
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			// Detect concurrently (Kafka reads), but serialize the DuckDB write.
			rows, table, res, err := detectTopicSample(gctx, conf, messages, specs, advisor, topic, emptyResampleTimeout)
			if err != nil {
				log.Printf("re-sample %q: %v", topic, err)
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			if err := database.WriteSampleTable(gctx, db, table, rows); err != nil {
				log.Printf("re-sample %q: %v", topic, err)
				return nil
			}
			if err := database.SaveTopicFields(gctx, db, topic, res, specs, messages); err != nil {
				log.Printf("re-sample %q: save topic fields: %v", topic, err)
			}
			updates[topic] = res
			resolved[topic] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return updates, resolved, nil
}

func init() {
	kafkaShowSchemaCmd.Flags().StringVar(&showSchemaTinyDecideDir, "tinydecide-model", "", "directory with meta.json/model.bin; when set, TinyDecide reviews structural schema-match ties while re-sampling (advisory only)")
	kafkaShowSchemaCmd.Flags().IntVar(&showSchemaConcurrency, "concurrency", 4, "maximum number of empty topics re-sampled at once")
	kafkaCmd.AddCommand(kafkaShowSchemaCmd)
}

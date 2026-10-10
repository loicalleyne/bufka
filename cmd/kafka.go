/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/couac"
)

// startKafkaIngest starts the consumer -> ingestor -> DuckDB sink pipeline.
// The returned wait func blocks until the sink has drained after ctx is
// canceled, then closes the consumer and writer. It returns (nil, nil) when
// no Kafka topics are configured.
func startKafkaIngest(ctx context.Context, cfg *config.Config, duckDB *couac.DB) (func(), error) {
	if !cfg.KafkaEnabled {
		return nil, nil
	}

	batchSize, err := strconv.Atoi(strings.TrimSpace(cfg.KafkaArrowBatchSize))
	if err != nil || batchSize <= 0 {
		return nil, fmt.Errorf("KAFKA_ARROW_BATCH_SIZE must be a positive integer")
	}
	flushInterval, err := time.ParseDuration(strings.TrimSpace(cfg.KafkaArrowFlushInterval))
	if err != nil || flushInterval <= 0 {
		return nil, fmt.Errorf("KAFKA_ARROW_FLUSH_INTERVAL must be a positive duration")
	}
	tables, err := kafkaingest.TableNames(cfg.KafkaTopics)
	if err != nil {
		return nil, err
	}
	topics := withIncludePaths(cfg.KafkaTopics, cfg.ProtoIncludePaths)

	batches := make(chan kafkaingest.TopicBatch, len(topics)*2)
	ingestor, err := kafkaingest.NewIngestor(topics, batchSize, flushInterval, batches)
	if err != nil {
		return nil, err
	}
	kafkaConf, err := kafka.NewKafkaConfigFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(kafkaConf)
	if err != nil {
		return nil, err
	}
	writer, err := database.NewArrowTableWriter(duckDB)
	if err != nil {
		consumer.Close()
		return nil, err
	}
	if err := consumer.Start(ctx); err != nil {
		consumer.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("start kafka consumer: %w", err)
	}

	go func() {
		ingestor.Run(ctx, consumer.Messages())
		close(batches)
	}()
	sinkDone := make(chan struct{})
	go func() {
		kafkaingest.RunSink(ctx, batches, writer, tables)
		close(sinkDone)
	}()
	log.Printf("kafka ingest started: consumer group %q, tables %v", kafkaConf.ConsumerGroup, tables)

	return func() {
		<-sinkDone
		consumer.Close()
		if err := writer.Close(); err != nil {
			log.Printf("close kafka table writer: %v", err)
		}
		log.Printf("kafka ingest stopped")
	}, nil
}

// withIncludePaths returns topics with include (PROTO_INCLUDE_PATHS)
// appended to each topic's own import paths, so a topic's
// KAFKA__TOPICS__N__IMPORT_PATHS are searched first.
func withIncludePaths(topics []config.KafkaTopicConfig, include []string) []config.KafkaTopicConfig {
	out := make([]config.KafkaTopicConfig, len(topics))
	for i, t := range topics {
		t.ImportPaths = append(append([]string(nil), t.ImportPaths...), include...)
		out[i] = t
	}
	return out
}

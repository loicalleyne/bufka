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
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/core/services/topicsize"
	"github.com/loicalleyne/couac"
	"github.com/spf13/cobra"
)

var (
	estimateCount   int
	estimateTimeout time.Duration
)

// kafkaEstimateSizeCmd estimates the total on-the-wire size of a topic from
// its message count and the sizes of a sample of its messages.
var kafkaEstimateSizeCmd = &cobra.Command{
	Use:   "estimate-size <topic>|all",
	Short: "Estimate a topic's total size from its message count and sample message sizes",
	Long: `Estimate the total size of <topic>, or of every topic when the argument is
"all", as (number of messages in the topic) * (average size of --count sampled
messages). The message count comes from partition start/end offsets; the
average message size is measured from a sample of the newest messages
(key + value + headers in bytes).

Each estimate is printed and, unless DUCKDB_PATH is :memory:, stored in the DuckDB
table kafka_topic_size_estimates (replaced on every run).`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runEstimateSize(ctx, args[0])
	},
}

func runEstimateSize(ctx context.Context, target string) error {
	cfg := GetConfig()
	if estimateCount <= 0 {
		return fmt.Errorf("--count must be greater than zero")
	}
	kafkaConf, err := kafka.NewKafkaConfigFromConfig(cfg)
	if err != nil {
		return err
	}

	var db *couac.DB
	if cfg.DuckDBPath != "" {
		if db, err = database.InitDB(cfg); err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
	} else {
		log.Printf("in-memory database (DUCKDB_PATH=:memory:); estimates will only be printed")
	}

	topics := []string{target}
	if target == "all" {
		if topics, err = kafka.ListTopics(ctx, kafkaConf); err != nil {
			return err
		}
	}

	var rows []database.EstimateRow
	for _, topic := range topics {
		if ctx.Err() != nil {
			break
		}
		res, partitions, err := estimateTopic(ctx, kafkaConf, topic)
		if err != nil {
			log.Printf("estimate %q: %v", topic, err)
			continue
		}
		fmt.Printf("%s\tmessages=%d\tavg=%s\test=%s\t[%s..%s]\tsample=%d\n",
			topic, res.TotalMessages, humanBytes(int64(res.AvgMessageBytes)),
			humanBytes(res.EstimatedBytes), humanBytes(res.LowBytes), humanBytes(res.HighBytes), res.SampleCount)
		rows = append(rows, database.EstimateRow{
			Topic:           topic,
			EstimatedAt:     time.Now().UTC(),
			Partitions:      int32(partitions),
			TotalMessages:   res.TotalMessages,
			SampleCount:     int32(res.SampleCount),
			AvgMessageBytes: res.AvgMessageBytes,
			StdDevBytes:     res.StdDevBytes,
			MinMessageBytes: int32(res.MinMessageBytes),
			MaxMessageBytes: int32(res.MaxMessageBytes),
			EstimatedBytes:  res.EstimatedBytes,
			LowBytes:        res.LowBytes,
			HighBytes:       res.HighBytes,
		})
	}

	if db != nil {
		if err := database.WriteEstimateTable(ctx, db, rows); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// estimateTopic counts a topic's messages, samples it and returns the size
// estimate plus the topic's partition count.
func estimateTopic(ctx context.Context, conf *kafka.KafkaClientConf, topic string) (topicsize.Result, int, error) {
	counts, err := kafka.TopicMessageCount(ctx, conf, topic)
	if err != nil {
		return topicsize.Result{}, 0, err
	}

	sampleCtx, cancel := context.WithTimeout(ctx, estimateTimeout)
	records, err := kafka.SampleTopic(sampleCtx, conf, topic, estimateCount)
	cancel()
	switch {
	case errors.Is(err, kafka.ErrTopicEmpty):
		log.Printf("estimate %q: %v", topic, err)
	case err != nil:
		return topicsize.Result{}, 0, err
	}

	sizes := make([]int, len(records))
	for i, r := range records {
		size := len(r.Key) + len(r.Value)
		for _, h := range r.Headers {
			size += len(h.Key) + len(h.Value)
		}
		sizes[i] = size
	}
	return topicsize.Estimate(counts.Total, topicsize.Stats(sizes)), counts.Partitions, nil
}

// humanBytes formats n as a human-readable size with binary (1024) units.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func init() {
	kafkaEstimateSizeCmd.Flags().IntVar(&estimateCount, "count", 50, "number of newest messages to sample per topic for the average size")
	kafkaEstimateSizeCmd.Flags().DurationVar(&estimateTimeout, "timeout", 30*time.Second, "maximum time spent sampling each topic")
	kafkaCmd.AddCommand(kafkaEstimateSizeCmd)
}

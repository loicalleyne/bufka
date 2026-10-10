/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/spf13/cobra"
)

// kafkaCmd groups Kafka-related utility subcommands.
var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Kafka utility commands",
}

// kafkaListTopicsCmd prints the topics available on the configured cluster.
var kafkaListTopicsCmd = &cobra.Command{
	Use:   "list-topics",
	Short: "List available Kafka topics",
	Long:  `Connect to the configured Kafka cluster and print its non-internal topics, one per line.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		kafkaConf, err := kafka.NewKafkaConfigFromConfig(GetConfig())
		if err != nil {
			return err
		}
		topics, err := kafka.ListTopics(context.Background(), kafkaConf)
		if err != nil {
			return err
		}
		if len(topics) == 0 {
			fmt.Println("no topics found")
			return nil
		}
		for _, topic := range topics {
			fmt.Println(topic)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(kafkaCmd)
	kafkaCmd.AddCommand(kafkaListTopicsCmd)
}

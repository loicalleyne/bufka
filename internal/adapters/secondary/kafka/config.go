package kafka

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/loicalleyne/bufka/internal/config"
)

// NewKafkaConfigFromConfig converts application Kafka settings into adapter settings.
func NewKafkaConfigFromConfig(source *config.Config) (*KafkaClientConf, error) {
	if source == nil {
		return nil, fmt.Errorf("config is nil")
	}
	seeds := splitKafkaSeeds(source.KafkaSeeds)
	if len(seeds) == 0 {
		return nil, fmt.Errorf("kafka seeds are required")
	}
	clientCount, err := parsePositiveKafkaInt32("KAFKA_CLIENT_COUNT", source.KafkaClientCount)
	if err != nil {
		return nil, err
	}
	msgChanCap, err := parsePositiveKafkaInt("KAFKA_MSG_CHAN_CAP", source.KafkaMsgChanCap)
	if err != nil {
		return nil, err
	}

	result := NewKafkaConfig()
	result.Seeds = seeds
	result.ConsumerGroup = source.KafkaConsumerGroup
	result.ConsumerGroupPrefix = source.KafkaConsumerGroupPrefix
	if source.KafkaConsumerGroupPrefix != "" {
		result.ConsumerGroup = source.KafkaConsumerGroupPrefix + rand.Text()
	}
	result.InstancePrefix = source.KafkaInstancePrefix
	result.ClientCount.Store(clientCount)
	result.MsgChanCap = msgChanCap
	result.User = source.KafkaSASLUser
	result.Password = source.KafkaSASLPassword
	if source.KafkaCutConfluencePrefix {
		result.Munger = WithMessageCutConfluencePrefix
	}
	result.Topics = make([]string, 0, len(source.KafkaTopics))
	for _, topic := range source.KafkaTopics {
		result.Topics = append(result.Topics, topic.Topic)
		if topic.CutConfluencePrefix {
			if result.TopicMungers == nil {
				result.TopicMungers = make(map[string]func([]byte) []byte, len(source.KafkaTopics))
			}
			result.TopicMungers[topic.Topic] = WithMessageCutConfluencePrefix
		}
	}
	if source.KafkaTLSEnabled {
		result.TLSDialer = &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config:    &tls.Config{MinVersion: tls.VersionTLS12},
		}
	}
	return result, nil
}

func splitKafkaSeeds(value string) []string {
	// Seeds may be separated by commas (KAFKA_SEEDS) or semicolons.
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' })
	seeds := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			seeds = append(seeds, part)
		}
	}
	return seeds
}

func parsePositiveKafkaInt(name, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parsePositiveKafkaInt32(name, value string) (int32, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive 32-bit integer", name)
	}
	return int32(parsed), nil
}

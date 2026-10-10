package kafkaingest

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/loicalleyne/bufka/internal/config"
)

const (
	tableNamePrefix = "kafka_"
	// maxTableNameLen leaves room for couac's IngestMerge "mergetmp" suffix within 63 chars.
	maxTableNameLen   = 55
	mergeTableSuffix  = "mergetmp"
	hashedTableKeepTo = maxTableNameLen - 1 - 8
)

var tableNamePattern = regexp.MustCompile(`^kafka_[a-z0-9_]{1,49}$`)

// TableName converts a Kafka topic into a DuckDB table name that is a valid unquoted SQL identifier.
//
// The topic is lowercased, every character outside [a-z0-9_] becomes "_", runs of "_" are collapsed
// and trimmed, and the result is prefixed with "kafka_". Names longer than 55 characters are
// truncated and suffixed with "_" plus the 8-hex-digit FNV-1a-32 hash of the original topic.
func TableName(topic string) (string, error) {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(topic) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	body := strings.Trim(b.String(), "_")
	if body == "" {
		return "", fmt.Errorf("kafka topic %q has no characters usable in a table name", topic)
	}

	name := tableNamePrefix + body
	if len(name) > maxTableNameLen {
		h := fnv.New32a()
		_, _ = h.Write([]byte(topic))
		name = fmt.Sprintf("%s_%08x", name[:hashedTableKeepTo], h.Sum32())
	}
	if !tableNamePattern.MatchString(name) {
		return "", fmt.Errorf("kafka topic %q produced invalid table name %q", topic, name)
	}
	return name, nil
}

// SampleTableName returns the table holding sampled messages for topic: its
// TableName plus "_sample", at most 62 characters.
func SampleTableName(topic string) (string, error) {
	name, err := TableName(topic)
	if err != nil {
		return "", err
	}
	return name + "_sample", nil
}

// TableNames maps each configured topic to its table name, rejecting names that collide with
// each other or with another table's IngestMerge temporary table.
func TableNames(topics []config.KafkaTopicConfig) (map[string]string, error) {
	tables := make(map[string]string, len(topics))
	topicByTable := make(map[string]string, len(topics))
	for _, topic := range topics {
		table, err := TableName(topic.Topic)
		if err != nil {
			return nil, err
		}
		if other, exists := topicByTable[table]; exists && other != topic.Topic {
			return nil, fmt.Errorf("kafka topics %q and %q both map to table %q", other, topic.Topic, table)
		}
		topicByTable[table] = topic.Topic
		tables[topic.Topic] = table
	}
	for table, topic := range topicByTable {
		if other, exists := topicByTable[table+mergeTableSuffix]; exists {
			return nil, fmt.Errorf("kafka topic %q table %q collides with the merge table of topic %q", other, table+mergeTableSuffix, topic)
		}
	}
	return tables, nil
}

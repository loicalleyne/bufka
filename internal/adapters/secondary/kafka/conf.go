// Package kafka is a SECONDARY adapter (outbound) that consumes records
// from a Kafka cluster using franz-go, with optional SASL/TLS auth.
package kafka

import (
	"crypto/tls"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/loicalleyne/bufka/pkg/confluentwire"
)

// KafkaClientConf holds the settings needed to build one or more franz-go
// consumer clients.
type KafkaClientConf struct {
	// mu guards every field below against live edits (ApplySettings) racing
	// with adapter calls. Adapters never read the fields directly from a
	// shared conf: they call Snapshot first and use the private copy.
	mu sync.RWMutex
	// ClientCount is the number of Kafka clients to open. Default value is 1.
	// If using more than one, use of a consumer group is recommended.
	ClientCount atomic.Int32
	// ConsumerGroup is the Kafka consumer group name.
	ConsumerGroup string
	// ConsumerGroupPrefix, when set, is the prefix the cluster's ACLs
	// whitelist consumer group names under; it is used to build ConsumerGroup
	// for the groups this process joins/creates when actually consuming.
	// Read-only group-browsing (ListConsumerGroups, DescribeConsumerGroup) is
	// not restricted to it, since inspecting other teams' groups needs no ACL
	// beyond describe/list.
	ConsumerGroupPrefix string
	// InstancePrefix is used to build a per-client client ID.
	InstancePrefix string
	// MsgChanCap is the message channel capacity, must be greater than 0.
	// Default capacity is 122880.
	MsgChanCap int
	// Munger mutates message bytes prior to transcoding. As an example,
	// Confluent's Java client adds 6 magic bytes at the beginning of message
	// data for use with Schema Registry, which must be removed prior to
	// protobuf decoding. Munger is applied to every consumed topic that has
	// no entry in TopicMungers.
	Munger func([]byte) []byte
	// TopicMungers maps a topic name to a munger function that overrides
	// Munger for messages consumed from that topic.
	TopicMungers map[string]func([]byte) []byte
	// TLSDialer, when set, is used to dial brokers over TLS.
	TLSDialer *tls.Dialer
	// Topics to consume.
	Topics []string
	// Seeds are the Kafka seed brokers.
	Seeds []string
	// User is the SASL auth user.
	User string
	// Password is the SASL auth password.
	Password string
}

// NewKafkaConfig returns a KafkaClientConf populated with defaults.
func NewKafkaConfig() *KafkaClientConf {
	conf := &KafkaClientConf{}
	conf.ClientCount.Store(1)
	conf.MsgChanCap = 122880
	return conf
}

// WithMessageCutConfluencePrefix removes the Confluent Schema Registry
// protobuf header (magic byte, schema ID and message-index path), leaving
// values without a valid header unchanged.
func WithMessageCutConfluencePrefix(m []byte) []byte {
	return confluentwire.StripHeader(m)
}

// Snapshot returns a private copy of c, safe to read without locking while
// ApplySettings runs concurrently. Operations take one at their start, so a
// long-running tail or sync job keeps the settings it started with. It is
// nil-safe: a nil conf yields nil.
func (c *KafkaClientConf) Snapshot() *KafkaClientConf {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := &KafkaClientConf{
		ConsumerGroup:       c.ConsumerGroup,
		ConsumerGroupPrefix: c.ConsumerGroupPrefix,
		InstancePrefix:      c.InstancePrefix,
		MsgChanCap:          c.MsgChanCap,
		Munger:              c.Munger,
		TopicMungers:        c.TopicMungers,
		TLSDialer:           c.TLSDialer,
		Topics:              slices.Clone(c.Topics),
		Seeds:               slices.Clone(c.Seeds),
		User:                c.User,
		Password:            c.Password,
	}
	s.ClientCount.Store(c.ClientCount.Load())
	return s
}

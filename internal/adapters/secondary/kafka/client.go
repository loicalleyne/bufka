package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a consumed Kafka record after configured value transformations.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Client consumes records from one Kafka cluster using one or more clients.
type Client struct {
	clients      []*kgo.Client
	msgs         chan Message
	munger       func([]byte) []byte
	topicMungers map[string]func([]byte) []byte

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool
	closed  bool
	wg      sync.WaitGroup
}

// NewConsumer creates a Kafka consumer for the configured topics.
func NewConsumer(conf *KafkaClientConf) (*Client, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return nil, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return nil, fmt.Errorf("kafka seed brokers are required")
	}
	if len(conf.Topics) == 0 {
		return nil, fmt.Errorf("kafka topics are required")
	}
	if conf.MsgChanCap <= 0 {
		return nil, fmt.Errorf("kafka message channel capacity must be greater than zero")
	}
	clientCount := conf.ClientCount.Load()
	if clientCount <= 0 {
		return nil, fmt.Errorf("kafka client count must be greater than zero")
	}

	clients := make([]*kgo.Client, 0, clientCount)
	for i := int32(0); i < clientCount; i++ {
		clientID := ""
		if conf.InstancePrefix != "" {
			clientID = fmt.Sprintf("%s-%d", conf.InstancePrefix, i)
		}
		opts := clientOpts(conf, clientID)
		client, err := kgo.NewClient(opts...)
		if err != nil {
			for _, created := range clients {
				created.Close()
			}
			return nil, fmt.Errorf("create Kafka consumer %d: %w", i, err)
		}
		clients = append(clients, client)
	}

	return &Client{
		clients:      clients,
		msgs:         make(chan Message, conf.MsgChanCap),
		munger:       conf.Munger,
		topicMungers: conf.TopicMungers,
	}, nil
}

// Start begins polling Kafka records until the context is canceled or closed.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("kafka consumer is closed")
	}
	if c.started {
		return fmt.Errorf("kafka consumer is already started")
	}
	pollCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.started = true
	c.wg.Add(len(c.clients))
	for _, client := range c.clients {
		go c.poll(pollCtx, client)
	}
	return nil
}

func (c *Client) poll(ctx context.Context, client *kgo.Client) {
	defer c.wg.Done()
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		for _, fetchErr := range fetches.Errors() {
			if fetchErr.Err == nil {
				continue
			}
			log.Printf("kafka fetch error for topic %q partition %d: %v", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			if !c.send(ctx, iter.Next()) {
				return
			}
		}
	}
}

// mungerFor returns the munger for topic, preferring a per-topic override
// from topicMungers and falling back to the client-wide munger.
func (c *Client) mungerFor(topic string) func([]byte) []byte {
	if munger, ok := c.topicMungers[topic]; ok {
		return munger
	}
	return c.munger
}

func (c *Client) send(ctx context.Context, record *kgo.Record) bool {
	value := append([]byte(nil), record.Value...)
	if munger := c.mungerFor(record.Topic); munger != nil {
		value = munger(value)
	}
	message := Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       append([]byte(nil), record.Key...),
		Value:     value,
		Timestamp: record.Timestamp,
	}
	select {
	case c.msgs <- message:
		return true
	case <-ctx.Done():
		return false
	}
}

// Messages returns the channel receiving consumed records.
func (c *Client) Messages() <-chan Message {
	return c.msgs
}

// Topics returns the names of non-internal topics available on the cluster.
func (c *Client) Topics(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("kafka consumer is closed")
	}
	client := c.clients[0]
	c.mu.Unlock()

	return topicNames(ctx, client)
}

// ListTopics returns the names of non-internal topics available on the
// cluster described by conf, without starting a consumer.
func ListTopics(ctx context.Context, conf *KafkaClientConf) ([]string, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return nil, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return nil, fmt.Errorf("kafka seed brokers are required")
	}
	client, err := kgo.NewClient(adminOpts(conf)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka admin client: %w", err)
	}
	defer client.Close()

	return topicNames(ctx, client)
}

// topicNames lists non-internal topics using client, which must remain open;
// kadm.Client wraps client without taking ownership, so it is never closed here.
func topicNames(ctx context.Context, client *kgo.Client) ([]string, error) {
	details, err := kadm.NewClient(client).ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list kafka topics: %w", err)
	}
	return details.TopicsSet().Sorted().Topics(), nil
}

// Close stops polling, closes Kafka clients, and closes the message channel.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	for _, client := range c.clients {
		client.Close()
	}
	c.mu.Unlock()

	c.wg.Wait()
	close(c.msgs)
}

package kafkaingest

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TopicBatch contains a flushed Arrow batch for one Kafka topic.
type TopicBatch struct {
	Topic  string
	Record arrow.RecordBatch
}

type topicState struct {
	transcoder  *bufarrowlib.Transcoder
	meta        *metaBuilder
	count       int
	lastMessage time.Time
}

// Ingestor decodes protobuf Kafka messages into full-fidelity Arrow batches,
// prefixed with _kafka_partition, _kafka_offset, _kafka_timestamp and _kafka_key columns.
type Ingestor struct {
	topics        map[string]*topicState
	batchSize     int
	flushInterval time.Duration
	output        chan<- TopicBatch
}

// NewIngestor creates one protobuf transcoder for each configured topic.
func NewIngestor(topics []config.KafkaTopicConfig, batchSize int, flushInterval time.Duration, output chan<- TopicBatch) (*Ingestor, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka ingest topics are required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("kafka ingest batch size must be greater than zero")
	}
	if flushInterval <= 0 {
		return nil, fmt.Errorf("kafka ingest flush interval must be greater than zero")
	}
	if output == nil {
		return nil, fmt.Errorf("kafka ingest output channel is required")
	}

	ingestor := &Ingestor{
		topics:        make(map[string]*topicState, len(topics)),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		output:        output,
	}
	for _, topic := range topics {
		if topic.Topic == "" || topic.ProtoFile == "" || topic.MessageName == "" {
			ingestor.release()
			return nil, fmt.Errorf("kafka ingest topic configuration is incomplete")
		}
		if _, exists := ingestor.topics[topic.Topic]; exists {
			ingestor.release()
			return nil, fmt.Errorf("duplicate Kafka ingest topic %q", topic.Topic)
		}
		fd, err := bufarrowlib.CompileProtoToFileDescriptor(topic.ProtoFile, protoinspect.WithEmbeddedImportDir(topic.ImportPaths))
		if err != nil {
			ingestor.release()
			return nil, fmt.Errorf("compile proto for Kafka topic %q: %w", topic.Topic, err)
		}
		md := findMessageDescriptor(fd, topic.MessageName)
		if md == nil {
			ingestor.release()
			return nil, fmt.Errorf("find message %q for Kafka topic %q: message not found in file descriptor", topic.MessageName, topic.Topic)
		}
		prunedMD, pruneErr := bufarrowlib.PruneEmptyMessages(md)
		if pruneErr == nil && prunedMD != nil {
			md = prunedMD
		}
		cyclic, cycErr := bufarrowlib.CyclicTypes(md)
		if cycErr != nil {
			ingestor.release()
			return nil, fmt.Errorf("check cyclic types for Kafka topic %q: %w", topic.Topic, cycErr)
		}
		hyperType := bufarrowlib.NewHyperType(md)
		opts := []bufarrowlib.Option{
			bufarrowlib.WithHyperType(hyperType),
			bufarrowlib.WithPruneEmptyMessages(),
		}
		if len(cyclic) > 0 {
			// Encode cyclic message types as protojson strings.
			opts = append(opts, bufarrowlib.WithJSONTermination(cyclic...))
		}
		transcoder, err := bufarrowlib.New(md, memory.DefaultAllocator, opts...)
		if err != nil {
			ingestor.release()
			return nil, fmt.Errorf("create transcoder for Kafka topic %q: %w", topic.Topic, err)
		}
		if err := checkMetaCollisions(transcoder.Schema()); err != nil {
			transcoder.Release()
			ingestor.release()
			return nil, fmt.Errorf("kafka topic %q: %w", topic.Topic, err)
		}
		ingestor.topics[topic.Topic] = &topicState{transcoder: transcoder, meta: newMetaBuilder(memory.DefaultAllocator)}
	}
	return ingestor, nil
}

// LoadProtoMessageDescriptor compiles the given proto file and finds the message descriptor.
func LoadProtoMessageDescriptor(protoFile string, importPaths []string, messageName string) (protoreflect.MessageDescriptor, error) {
	fd, err := bufarrowlib.CompileProtoToFileDescriptor(protoFile, protoinspect.WithEmbeddedImportDir(importPaths))
	if err != nil {
		return nil, fmt.Errorf("compile proto %q: %w", protoFile, err)
	}
	md := findMessageDescriptor(fd, messageName)
	if md == nil {
		return nil, fmt.Errorf("find message %q in %q: message not found", messageName, protoFile)
	}
	return md, nil
}

// Run decodes messages until the context is canceled or the input closes.
func (i *Ingestor) Run(ctx context.Context, input <-chan kafka.Message) {
	ticker := time.NewTicker(i.flushInterval)
	defer ticker.Stop()
	defer i.release()

	for {
		select {
		case <-ctx.Done():
			i.flushAll(context.Background())
			return
		case <-ticker.C:
			i.flushExpired(ctx)
		case message, ok := <-input:
			if !ok {
				i.flushAll(context.Background())
				return
			}
			i.append(ctx, message)
		}
	}
}

func (i *Ingestor) append(ctx context.Context, message kafka.Message) {
	state, ok := i.topics[message.Topic]
	if !ok {
		log.Printf("dropping Kafka message for unconfigured topic %q", message.Topic)
		return
	}
	if err := state.transcoder.AppendRaw(message.Value); err != nil {
		log.Printf("dropping invalid protobuf message for Kafka topic %q: %v", message.Topic, err)
		return
	}
	// Only record metadata after a successful decode so rows stay aligned.
	state.meta.append(message)
	state.count++
	state.lastMessage = time.Now()
	if state.count >= i.batchSize {
		i.flush(ctx, message.Topic, state)
	}
}

func (i *Ingestor) flushExpired(ctx context.Context) {
	now := time.Now()
	for topic, state := range i.topics {
		if state.count > 0 && now.Sub(state.lastMessage) >= i.flushInterval {
			i.flush(ctx, topic, state)
		}
	}
}

func (i *Ingestor) flushAll(ctx context.Context) {
	for topic, state := range i.topics {
		if state.count > 0 {
			i.flush(ctx, topic, state)
		}
	}
}

func (i *Ingestor) flush(ctx context.Context, topic string, state *topicState) {
	record, err := state.meta.combine(state.transcoder.NewRecordBatch())
	state.count = 0
	state.lastMessage = time.Time{}
	if err != nil {
		log.Printf("dropping batch for Kafka topic %q: %v", topic, err)
		return
	}
	select {
	case i.output <- TopicBatch{Topic: topic, Record: record}:
	case <-ctx.Done():
		record.Release()
	}
}

func (i *Ingestor) release() {
	for _, state := range i.topics {
		if state.transcoder != nil {
			state.transcoder.Release()
			state.transcoder = nil
		}
		if state.meta != nil {
			state.meta.release()
			state.meta = nil
		}
	}
}

// findMessageDescriptor searches fd for a message descriptor matching target,
// which can be either a fully-qualified name (e.g. "shop.v1.Order")
// or a short name ("Order"). Both top-level and nested message types are
// searched.
func findMessageDescriptor(fd protoreflect.FileDescriptor, target string) protoreflect.MessageDescriptor {
	md, err := bufarrowlib.GetMessageDescriptorByName(fd, target)
	if err != nil {
		return nil
	}
	return md
}

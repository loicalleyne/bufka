package kafkaingest

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// DynamicProtoJSONIngestor decodes protobuf messages into dynamicpb and formats them
// as JSON strings in the raw JSON Arrow schema, providing full compatibility for messages
// with cyclic/recursive types that cannot be flattened into a static Arrow schema.
type DynamicProtoJSONIngestor struct {
	topic         string
	desc          protoreflect.MessageDescriptor
	batchSize     int
	flushInterval time.Duration
	output        chan<- TopicBatch

	builder *array.RecordBuilder
	count   int
	partB   *array.Int32Builder
	offsetB *array.Int64Builder
	timeB   *array.TimestampBuilder
	keyB    *array.BinaryBuilder
	valB    *array.StringBuilder
}

// NewDynamicProtoJSONIngestor creates an ingestor that serializes protobuf messages to JSON in Arrow.
func NewDynamicProtoJSONIngestor(topic string, desc protoreflect.MessageDescriptor, batchSize int, flushInterval time.Duration, output chan<- TopicBatch) *DynamicProtoJSONIngestor {
	pool := memory.DefaultAllocator
	b := array.NewRecordBuilder(pool, RawJSONSchema)
	return &DynamicProtoJSONIngestor{
		topic:         topic,
		desc:          desc,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		output:        output,
		builder:       b,
		partB:         b.Field(0).(*array.Int32Builder),
		offsetB:       b.Field(1).(*array.Int64Builder),
		timeB:         b.Field(2).(*array.TimestampBuilder),
		keyB:          b.Field(3).(*array.BinaryBuilder),
		valB:          b.Field(4).(*array.StringBuilder),
	}
}

// Run processes incoming Kafka messages until ctx is done or input is closed.
func (d *DynamicProtoJSONIngestor) Run(ctx context.Context, input <-chan kafka.Message) {
	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()
	defer d.release()

	for {
		select {
		case <-ctx.Done():
			d.flush()
			return

		case <-ticker.C:
			d.flush()

		case msg, ok := <-input:
			if !ok {
				d.flush()
				return
			}
			d.append(msg)
			if d.count >= d.batchSize {
				d.flush()
			}
		}
	}
}

func (d *DynamicProtoJSONIngestor) append(msg kafka.Message) {
	d.partB.Append(msg.Partition)
	d.offsetB.Append(msg.Offset)

	ts, _ := arrow.TimestampFromTime(msg.Timestamp, arrow.Microsecond)
	d.timeB.Append(ts)

	if len(msg.Key) > 0 {
		d.keyB.Append(msg.Key)
	} else {
		d.keyB.AppendNull()
	}

	// Unmarshal protobuf binary into dynamicpb, then serialize to JSON
	dynMsg := dynamicpb.NewMessage(d.desc)
	jsonStr := ""
	if err := proto.Unmarshal(msg.Value, dynMsg); err == nil {
		if b, err := protojson.Marshal(dynMsg); err == nil {
			jsonStr = string(b)
		}
	}
	if jsonStr == "" {
		// Fallback to raw string representation of value if proto unmarshal fails
		jsonStr = string(msg.Value)
	}

	d.valB.Append(jsonStr)
	d.count++
}

func (d *DynamicProtoJSONIngestor) flush() {
	if d.count == 0 {
		return
	}
	rec := d.builder.NewRecordBatch()
	d.count = 0
	d.output <- TopicBatch{
		Topic:  d.topic,
		Record: rec,
	}
}

func (d *DynamicProtoJSONIngestor) release() {
	if d.builder != nil {
		d.builder.Release()
		d.builder = nil
	}
}

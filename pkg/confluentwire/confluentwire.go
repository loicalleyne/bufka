// Package confluentwire parses the Confluent Schema Registry wire-format
// header that Confluent serializers prepend to Kafka message values.
//
// Protobuf header: magic byte 0x00, 4-byte big-endian schema ID, then a
// zigzag-varint count followed by that many zigzag-varint message indexes
// (the path of the message inside the registered .proto file). A count of 0
// is shorthand for the index path [0].
//
// JSON Schema (and Avro) header: magic byte 0x00 and the 4-byte schema ID only.
package confluentwire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	magicByte = 0x00
	// JSONHeaderLen is the length of the JSON Schema / Avro header.
	JSONHeaderLen = 5
	// maxIndexes bounds the message-index count to reject garbage early.
	maxIndexes = 32
)

// ErrNoHeader is returned when the input does not start with a valid header.
var ErrNoHeader = errors.New("confluentwire: no valid header")

// Header is a parsed Confluent protobuf wire-format header.
type Header struct {
	SchemaID int32
	// MessageIndexes is the path of the message within the registered schema; never empty.
	MessageIndexes []int32
	// Len is the number of header bytes preceding the protobuf payload.
	Len int
}

// ParseHeader parses the Confluent protobuf header at the start of b.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < JSONHeaderLen+1 || b[0] != magicByte {
		return Header{}, ErrNoHeader
	}
	h := Header{SchemaID: int32(binary.BigEndian.Uint32(b[1:5]))}
	pos := JSONHeaderLen

	count, n := binary.Varint(b[pos:])
	if n <= 0 {
		return Header{}, fmt.Errorf("%w: bad message-index count", ErrNoHeader)
	}
	pos += n
	if count < 0 || count > maxIndexes {
		return Header{}, fmt.Errorf("%w: message-index count %d out of range", ErrNoHeader, count)
	}
	if count == 0 {
		h.MessageIndexes = []int32{0}
		h.Len = pos
		return h, nil
	}
	h.MessageIndexes = make([]int32, 0, count)
	for range count {
		idx, n := binary.Varint(b[pos:])
		if n <= 0 {
			return Header{}, fmt.Errorf("%w: truncated message index", ErrNoHeader)
		}
		if idx < 0 || idx > 1<<20 {
			return Header{}, fmt.Errorf("%w: message index %d out of range", ErrNoHeader, idx)
		}
		pos += n
		h.MessageIndexes = append(h.MessageIndexes, int32(idx))
	}
	h.Len = pos
	return h, nil
}

// StripHeader returns b without its Confluent protobuf header, or b unchanged
// when no valid header is present.
func StripHeader(b []byte) []byte {
	h, err := ParseHeader(b)
	if err != nil {
		return b
	}
	return b[h.Len:]
}

// ParseJSONHeader returns the schema ID of a 5-byte Confluent JSON Schema header.
func ParseJSONHeader(b []byte) (int32, error) {
	if len(b) < JSONHeaderLen || b[0] != magicByte {
		return 0, ErrNoHeader
	}
	return int32(binary.BigEndian.Uint32(b[1:5])), nil
}

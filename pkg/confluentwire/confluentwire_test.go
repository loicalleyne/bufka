package confluentwire_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/loicalleyne/bufka/pkg/confluentwire"
)

var schemaID = []byte{0x00, 0x00, 0x01, 0x02}

func header(rest ...byte) []byte {
	return append(append([]byte{0x00}, schemaID...), rest...)
}

func TestParseHeader(t *testing.T) {
	payload := []byte{0x0a, 0x01, 'x'}
	tests := []struct {
		name    string
		in      []byte
		indexes []int32
		length  int
		wantErr bool
	}{
		{name: "first message shortcut", in: header(0x00), indexes: []int32{0}, length: 6},
		{name: "second top-level message", in: header(0x02, 0x02), indexes: []int32{1}, length: 7},
		{name: "nested message", in: header(0x04, 0x02, 0x00), indexes: []int32{1, 0}, length: 8},
		{name: "too short", in: []byte{0x00, 0x00, 0x01}, wantErr: true},
		{name: "non-zero magic", in: append([]byte{0x01}, header(0x00)[1:]...), wantErr: true},
		{name: "negative count", in: header(0x01), wantErr: true},
		{name: "count too large", in: header(0x42), wantErr: true},
		{name: "truncated indexes", in: header(0x04, 0x02), wantErr: true},
		{name: "negative index", in: header(0x02, 0x01), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			if !tt.wantErr {
				in = append(slices.Clone(in), payload...)
			}
			h, err := confluentwire.ParseHeader(in)
			if tt.wantErr {
				if !errors.Is(err, confluentwire.ErrNoHeader) {
					t.Fatalf("err = %v, want ErrNoHeader", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseHeader: %v", err)
			}
			if h.SchemaID != 0x0102 {
				t.Errorf("SchemaID = %d, want %d", h.SchemaID, 0x0102)
			}
			if !slices.Equal(h.MessageIndexes, tt.indexes) {
				t.Errorf("MessageIndexes = %v, want %v", h.MessageIndexes, tt.indexes)
			}
			if h.Len != tt.length {
				t.Errorf("Len = %d, want %d", h.Len, tt.length)
			}
		})
	}
}

func TestStripHeader(t *testing.T) {
	payload := []byte{0x0a, 0x01, 'x'}
	if got := confluentwire.StripHeader(append(header(0x04, 0x02, 0x00), payload...)); !bytes.Equal(got, payload) {
		t.Errorf("StripHeader = %x, want %x", got, payload)
	}
	if got := confluentwire.StripHeader(payload); !bytes.Equal(got, payload) {
		t.Errorf("StripHeader without header = %x, want unchanged %x", got, payload)
	}
	if got := confluentwire.StripHeader(nil); got != nil {
		t.Errorf("StripHeader(nil) = %x, want nil", got)
	}
}

func TestParseJSONHeader(t *testing.T) {
	id, err := confluentwire.ParseJSONHeader(append(header(), '{', '}'))
	if err != nil || id != 0x0102 {
		t.Fatalf("ParseJSONHeader = %d, %v; want %d, nil", id, err, 0x0102)
	}
	if _, err := confluentwire.ParseJSONHeader([]byte(`{}`)); err == nil {
		t.Fatal("ParseJSONHeader on raw JSON: want error")
	}
}

func ExampleStripHeader() {
	value := []byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x02, 0x02, 0x0a, 0x01, 'x'}
	h, _ := confluentwire.ParseHeader(value)
	fmt.Println(h.SchemaID, h.MessageIndexes, h.Len)
	fmt.Printf("%x\n", confluentwire.StripHeader(value))
	// Output:
	// 7 [1] 7
	// 0a0178
}

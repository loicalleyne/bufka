package kafka

import (
	"bytes"
	"testing"
)

func TestWithMessageCutConfluencePrefix(t *testing.T) {
	payload := []byte{0x0a, 0x01, 'x'}
	tests := []struct {
		name string
		in   []byte
		want []byte
	}{
		{name: "6-byte header", in: append([]byte{0, 0, 0, 0, 1, 0x00}, payload...), want: payload},
		{name: "7-byte header", in: append([]byte{0, 0, 0, 0, 1, 0x02, 0x02}, payload...), want: payload},
		{name: "8-byte header", in: append([]byte{0, 0, 0, 0, 1, 0x04, 0x02, 0x00}, payload...), want: payload},
		{name: "no header", in: payload, want: payload},
		{name: "short input", in: []byte{0, 1}, want: []byte{0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WithMessageCutConfluencePrefix(tt.in); !bytes.Equal(got, tt.want) {
				t.Errorf("got %x, want %x", got, tt.want)
			}
		})
	}
}

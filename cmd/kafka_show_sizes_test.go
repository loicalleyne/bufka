package cmd

import (
	"slices"
	"testing"
)

func TestWrapText(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		width int
		want  []string
	}{
		{name: "fits", s: "orders.events", width: 20, want: []string{"orders.events"}},
		{name: "break on dot", s: "orders.events.created", width: 14, want: []string{"orders.events.", "created"}},
		{name: "break on dash", s: "order-fulfillment-events", width: 10, want: []string{"order-", "fulfillmen", "t-events"}},
		{name: "hard split no separator", s: "abcdefghijkl", width: 5, want: []string{"abcde", "fghij", "kl"}},
		{name: "exact width", s: "abcde", width: 5, want: []string{"abcde"}},
		{name: "empty", s: "", width: 5, want: []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapText(tt.s, tt.width)
			for _, line := range got {
				if len(line) > tt.width {
					t.Errorf("line %q exceeds width %d", line, tt.width)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("wrapText(%q, %d) = %q, want %q", tt.s, tt.width, got, tt.want)
			}
		})
	}
}

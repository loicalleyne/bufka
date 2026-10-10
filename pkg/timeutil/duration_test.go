package timeutil

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{input: "30s", want: 30 * time.Second},
		{input: "2h", want: 2 * time.Hour},
		{input: "1d", want: 24 * time.Hour},
		{input: "2d13h56s", want: 2*24*time.Hour + 13*time.Hour + 56*time.Second},
		{input: "0s", want: 0},
		{input: "1d2h3m4s", want: 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second},
		{input: "", wantErr: true},
		{input: "invalid", wantErr: true},
		{input: "-1d", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{input: 30 * time.Second, want: "30s"},
		{input: 2 * time.Hour, want: "2h"},
		{input: 24 * time.Hour, want: "1d"},
		{input: 2*24*time.Hour + 13*time.Hour + 56*time.Second, want: "2d13h56s"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatDuration(tt.input)
			if got != tt.want {
				t.Errorf("FormatDuration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

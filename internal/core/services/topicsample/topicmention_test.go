package topicsample

import "testing"

func TestContainsTopic(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{text: "// - Topic name: a.jobs\n", want: true},
		{text: "Topic: a.jobs.", want: true},
		{text: "a.jobs", want: true},
		{text: "'a.jobs'", want: true},
		{text: "a.jobs.v1", want: false},
		{text: "a.jobs-results", want: false},
		{text: "xa.jobs", want: false},
		{text: "data.jobs and a.jobs", want: true},
		{text: "nothing here", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			if got := containsTopic(tt.text, "a.jobs"); got != tt.want {
				t.Errorf("containsTopic = %v, want %v", got, tt.want)
			}
		})
	}
}

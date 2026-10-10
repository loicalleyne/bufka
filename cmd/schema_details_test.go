package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
)

func TestPrintSchemaDetails(t *testing.T) {
	var buf bytes.Buffer
	err := printSchemaDetails(&buf, []schemaDetail{
		{Topic: "orders", Warnings: []string{"missing id"}, Fields: []database.TopicFieldRow{
			{FieldPath: "id", Type: "string", Required: true, Description: "Order\n id " + strings.Repeat("x", 100)},
			{FieldPath: "note", Type: "string"},
		}},
		{Topic: "empty"},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"TOPIC", "WARNING", "missing id", "PATH", "REQUIRED", "orders", "id", "string", "true", "Order id xxx", "…"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "empty") {
		t.Errorf("topic without docs printed:\n%s", out)
	}

	buf.Reset()
	_ = printSchemaDetails(&buf, []schemaDetail{{Topic: "none"}})
	if buf.Len() != 0 {
		t.Errorf("expected no output, got %q", buf.String())
	}
}

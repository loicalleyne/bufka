package topicsample_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
)

const testSpec = `asyncapi: 2.1.0
info:
  title: Test Events
  version: 1.0.0
channels:
  things/created:
    publish:
      message:
        $ref: "#/components/messages/Created"
  things/deleted:
    publish:
      message:
        $ref: "#/components/messages/Deleted"
  single:
    publish:
      message:
        $ref: "#/components/messages/Single"
components:
  messages:
    Created:
      name: created
      contentType: application/json
      payload:
        $ref: "#/components/schemas/created"
    Deleted:
      name: deleted
      contentType: application/json
      payload:
        $ref: "#/components/schemas/deleted"
    Single:
      name: single
      contentType: application/json
      payload:
        $ref: "#/components/schemas/single"
  schemas:
    created:
      type: object
      required: [Id, Type]
      properties:
        Id: {type: string}
        Type: {type: string, enum: [created]}
        Name: {type: string}
    deleted:
      type: object
      required: [Id, Type]
      properties:
        Id: {type: string}
        Type: {type: string, enum: [deleted]}
        Reason: {type: string}
    single:
      type: object
      required: [Id, Name]
      properties:
        Id: {type: string}
        Name: {type: string}
        Opt: {type: string}
        Inner:
          type: object
          required: [A]
          properties:
            A: {type: string}
`

func loadSpec(t *testing.T) *asyncapi.Index {
	t.Helper()
	p := filepath.Join(t.TempDir(), "spec.yml")
	if err := os.WriteFile(p, []byte(testSpec), 0o600); err != nil {
		t.Fatal(err)
	}
	ix, errs := asyncapi.Load([]string{p})
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	return ix
}

func jsonVals(s ...string) [][]byte {
	out := make([][]byte, len(s))
	for i, v := range s {
		out[i] = []byte(v)
	}
	return out
}

func TestSpecSingleMessageNoWarnings(t *testing.T) {
	ix := loadSpec(t)
	got := topicsample.Detect("single", jsonVals(`{"Id":"1","Name":"n"}`, `{"Id":"2","Name":"m","Opt":"x","Inner":{"A":"a"}}`), nil, ix, nil)
	if got.Format != topicsample.FormatJSON || got.ResolvedBy != topicsample.ResolvedByAsyncAPI {
		t.Fatalf("format=%s resolvedBy=%s", got.Format, got.ResolvedBy)
	}
	if got.Spec == nil || got.Spec.Message != "single" || got.Spec.Topic != "single" {
		t.Fatalf("spec = %+v", got.Spec)
	}
	if len(got.Warnings) != 0 {
		t.Fatalf("warnings = %v", got.Warnings)
	}
}

func TestSpecWarnings(t *testing.T) {
	ix := loadSpec(t)
	got := topicsample.Detect("single", jsonVals(`{"Id":"1","Extra":1}`, `{"Id":"2","Name":"n","Inner":{"B":1}}`, `{"Id":"3","Name":"n","extra":1}`), nil, ix, nil)
	want := []string{
		"required field Name missing in 1/3 sampled messages",
		"required field Inner.A missing in 1/3 sampled messages",
		"key Extra not defined in spec in 1/3 messages",
		"key extra not defined in spec in 1/3 messages",
		"key Inner.B not defined in spec in 1/3 messages",
	}
	have := strings.Join(got.Warnings, "\n")
	for _, w := range want {
		if !strings.Contains(have, w) {
			t.Errorf("missing warning %q in %v", w, got.Warnings)
		}
	}
	for _, w := range got.Warnings {
		if strings.Contains(w, "Opt") {
			t.Errorf("warned about optional field: %s", w)
		}
	}
}

func TestSpecDiscriminator(t *testing.T) {
	ix := loadSpec(t)
	got := topicsample.Detect("things", jsonVals(`{"Id":"1","Type":"deleted"}`, `{"Id":"2","Type":"deleted","Reason":"r"}`), nil, ix, nil)
	if got.Spec == nil || got.Spec.Message != "deleted" {
		t.Fatalf("spec = %+v", got.Spec)
	}
	if len(got.Spec.Candidates) != 2 {
		t.Fatalf("candidates = %v", got.Spec.Candidates)
	}
	if len(got.Warnings) != 0 {
		t.Fatalf("warnings = %v", got.Warnings)
	}
	// No discriminator value present: fall back to field overlap.
	got = topicsample.Detect("things", jsonVals(`{"Id":"1","Reason":"r"}`), nil, ix, nil)
	if got.Spec.Message != "deleted" {
		t.Fatalf("overlap pick = %q", got.Spec.Message)
	}
}

func TestSpecNonJSONSamples(t *testing.T) {
	ix := loadSpec(t)
	got := topicsample.Detect("single", [][]byte{{0xff, 0xfe, 0x00, 0x01}}, nil, ix, nil)
	if got.Format != topicsample.FormatUnknown || got.Spec == nil {
		t.Fatalf("got %+v", got)
	}
	if got.ResolvedBy == topicsample.ResolvedByAsyncAPI {
		t.Fatal("must not resolve by asyncapi")
	}
	if len(got.Warnings) != 1 || got.Warnings[0] != "AsyncAPI documents this topic as JSON but samples are unknown" {
		t.Fatalf("warnings = %v", got.Warnings)
	}
}

func TestSpecNilIndexUnchanged(t *testing.T) {
	got := topicsample.Detect("single", jsonVals(`{"Id":"1"}`), nil, nil, nil)
	if got.Format != topicsample.FormatJSON || got.Spec != nil || got.Warnings != nil || got.ResolvedBy != topicsample.ResolvedByDetected {
		t.Fatalf("got %+v", got)
	}
}

func TestSpecEmptyTopic(t *testing.T) {
	ix := loadSpec(t)
	got := topicsample.Detect("single", nil, nil, ix, nil)
	if got.Format != topicsample.FormatEmpty {
		t.Fatalf("format = %s", got.Format)
	}
	if got.Spec == nil || got.Spec.Message != "single" || got.ResolvedBy != topicsample.ResolvedByAsyncAPI {
		t.Fatalf("got %+v", got)
	}
	// Undocumented topic stays untouched.
	got = topicsample.Detect("other", nil, nil, ix, nil)
	if got.Spec != nil || got.ResolvedBy != "" {
		t.Fatalf("got %+v", got)
	}
}

func TestSpecConfluentCutJSON(t *testing.T) {
	ix := loadSpec(t)
	v := append([]byte{0, 0, 0, 0, 7}, `{"Id":"1"}`...)
	got := topicsample.Detect("single", [][]byte{v}, nil, ix, nil)
	if !got.CutConfluencePrefix || got.Spec == nil || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "required field Name") {
		t.Fatalf("got %+v", got)
	}
}

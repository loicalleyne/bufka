package topicsample_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

const testProto = `syntax = "proto3";
package t;

message Order { string name = 1; int64 id = 2; }
message Item { string name = 1; }
message Outer {
  message Inner { string name = 1; }
  int32 x = 1;
}
message Wrapper { Item inner = 1; }
`

func loadMessages(t testing.TB) *protoinspect.OrderedMap[string, protoinspect.MessageInfo] {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "t.proto"), []byte(testProto), 0o644); err != nil {
		t.Fatal(err)
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{dir})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: %v %v", err, failures)
	}
	return msgs
}

// encode marshals the JSON representation js as message fqn.
func encode(t testing.TB, msgs *protoinspect.OrderedMap[string, protoinspect.MessageInfo], fqn, js string) []byte {
	t.Helper()
	info, ok := msgs.Get(fqn)
	if !ok {
		t.Fatalf("message %s not loaded", fqn)
	}
	msg := dynamicpb.NewMessage(info.Descriptor)
	if err := protojson.Unmarshal([]byte(js), msg); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return b
}

func withHeader(schemaID byte, indexes []byte, payload []byte) []byte {
	b := append([]byte{0x00, 0x00, 0x00, 0x00, schemaID}, indexes...)
	return append(b, payload...)
}

func TestDetect(t *testing.T) {
	msgs := loadMessages(t)
	order := encode(t, msgs, "t.Order", `{"name":"a","id":"7"}`)
	item := encode(t, msgs, "t.Item", `{"name":"widget"}`)
	wrapper := encode(t, msgs, "t.Wrapper", `{"inner":{"name":"abc"}}`)

	tests := []struct {
		name        string
		values      [][]byte
		format      string
		message     string
		cut         bool
		schemaIDs   []int32
		indexes     [][]int32
		matched     int
		notCandidat string
	}{
		{
			name:        "raw protobuf, unknown fields reject narrower type",
			values:      [][]byte{order, order},
			format:      topicsample.FormatProtobuf,
			message:     "t.Order",
			matched:     2,
			notCandidat: "t.Item",
		},
		{
			name:      "6-byte header",
			values:    [][]byte{withHeader(9, []byte{0x00}, order)},
			format:    topicsample.FormatProtobuf,
			message:   "t.Order",
			cut:       true,
			schemaIDs: []int32{9},
			indexes:   [][]int32{{0}},
			matched:   1,
		},
		{
			name:      "index path picks second top-level message",
			values:    [][]byte{withHeader(3, []byte{0x02, 0x02}, item)},
			format:    topicsample.FormatProtobuf,
			message:   "t.Item",
			cut:       true,
			schemaIDs: []int32{3},
			indexes:   [][]int32{{1}},
			matched:   1,
		},
		{
			name:      "index path picks nested message",
			values:    [][]byte{withHeader(3, []byte{0x04, 0x04, 0x00}, item)},
			format:    topicsample.FormatProtobuf,
			message:   "t.Outer.Inner",
			cut:       true,
			schemaIDs: []int32{3},
			indexes:   [][]int32{{2, 0}},
			matched:   1,
		},
		{
			name:    "richer interpretation wins",
			values:  [][]byte{wrapper},
			format:  topicsample.FormatProtobuf,
			message: "t.Wrapper",
			matched: 1,
		},
		{
			name: "mixed index paths",
			values: [][]byte{
				withHeader(3, []byte{0x00}, order),
				withHeader(3, []byte{0x02, 0x02}, item),
			},
			format:    topicsample.FormatUnknown,
			schemaIDs: []int32{3},
			indexes:   [][]int32{{0}, {1}},
		},
		{
			name:    "raw json",
			values:  [][]byte{[]byte(`{"a":1}`), []byte(`[1,2]`)},
			format:  topicsample.FormatJSON,
			matched: 2,
		},
		{
			name:      "json with header",
			values:    [][]byte{append([]byte{0, 0, 0, 0, 5}, `{"a":1}`...)},
			format:    topicsample.FormatJSON,
			cut:       true,
			schemaIDs: []int32{5},
			matched:   1,
		},
		{
			name:   "garbage",
			values: [][]byte{{0xff, 0xff, 0xff}},
			format: topicsample.FormatUnknown,
		},
		{
			name:    "tombstones ignored",
			values:  [][]byte{order, nil, {}},
			format:  topicsample.FormatProtobuf,
			message: "t.Order",
			matched: 1,
		},
		{
			name:   "only tombstones",
			values: [][]byte{nil},
			format: topicsample.FormatEmpty,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := topicsample.Detect("", tt.values, msgs, nil, nil)
			if got.Format != tt.format || got.MessageName != tt.message || got.CutConfluencePrefix != tt.cut {
				t.Fatalf("got (%s, %q, cut=%v), want (%s, %q, cut=%v); candidates %v",
					got.Format, got.MessageName, got.CutConfluencePrefix, tt.format, tt.message, tt.cut, got.Candidates)
			}
			if !slices.Equal(got.SchemaIDs, tt.schemaIDs) {
				t.Errorf("SchemaIDs = %v, want %v", got.SchemaIDs, tt.schemaIDs)
			}
			if !slices.EqualFunc(got.MessageIndexes, tt.indexes, slices.Equal) {
				t.Errorf("MessageIndexes = %v, want %v", got.MessageIndexes, tt.indexes)
			}
			if got.MatchedCount != tt.matched || got.SampleCount != len(tt.values) {
				t.Errorf("matched/total = %d/%d, want %d/%d", got.MatchedCount, got.SampleCount, tt.matched, len(tt.values))
			}
			if len(got.Rows) != len(tt.values) {
				t.Fatalf("len(Rows) = %d, want %d", len(got.Rows), len(tt.values))
			}
			if tt.notCandidat != "" && slices.Contains(got.Candidates, tt.notCandidat) {
				t.Errorf("Candidates %v should not contain %s", got.Candidates, tt.notCandidat)
			}
			for i, row := range got.Rows {
				if row.DecodeOK != (len(tt.values[i]) > 0 && tt.matched > 0) {
					t.Errorf("row %d DecodeOK = %v", i, row.DecodeOK)
				}
				if row.DecodeOK && row.Decoded == "" {
					t.Errorf("row %d Decoded is empty", i)
				}
			}
		})
	}
}

// TestDetect_TopicMentionedInProto checks that identically-shaped types are
// ranked by where the topic name appears in their .proto source.
func TestDetect_TopicMentionedInProto(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"money.proto": "syntax = \"proto3\";\npackage g;\nmessage Money { string code = 1; int64 units = 2; }\n",
		"jobs.proto": `syntax = "proto3";
package c;
// Topic name: c.apply-jobs
message ApplyJob { string id = 1; int64 n = 2; }
// Topic name: c.apply-job-results
message ApplyJobResultWithAVeryLongName { string job_id = 1; int32 status = 2; }
`,
		"other.proto": "syntax = \"proto3\";\npackage o;\n// Produced to o.events.\nmessage Event { string id = 1; int64 n = 2; }\n",
	}
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{dir})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: %v %v", err, failures)
	}
	value := encode(t, msgs, "g.Money", `{"code":"abc","units":"2"}`)

	tests := []struct {
		topic string
		want  string
	}{
		{topic: "c.apply-job-results", want: "c.ApplyJobResultWithAVeryLongName"},
		{topic: "c.apply-jobs", want: "c.ApplyJob"},
		{topic: "o.events", want: "o.Event"},
		{topic: "unrelated", want: "g.Money"},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := topicsample.Detect(tt.topic, [][]byte{value}, msgs, nil, nil)
			if got.MessageName != tt.want {
				t.Errorf("MessageName = %s, want %s (candidates %v)", got.MessageName, tt.want, got.Candidates)
			}
		})
	}
}

func TestDetect_EmptyTopicUsesProtoDocs(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"state.proto": `syntax = "proto3";
package u;
// Topic name: s.panels
message PanelState { string id = 1; }
// Topic name: s.panel-groups
message PanelGroupState { string id = 1; }
`,
		"single.proto": `syntax = "proto3";
package v;
// Messages for v.things live here.
message Thing { message Part { string id = 1; } Part part = 1; }
`,
	}
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{dir})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: %v %v", err, failures)
	}

	tests := []struct {
		topic string
		want  string
	}{
		{topic: "s.panel-groups", want: "u.PanelGroupState"},
		{topic: "s.panels", want: "u.PanelState"},
		{topic: "v.things", want: "v.Thing"},
		{topic: "undocumented", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := topicsample.Detect(tt.topic, nil, msgs, nil, nil)
			if got.Format != topicsample.FormatEmpty || got.MessageName != tt.want {
				t.Errorf("got (%s, %q), want (empty, %q); candidates %v", got.Format, got.MessageName, tt.want, got.Candidates)
			}
		})
	}
}

func TestDetect_NoProtos(t *testing.T) {
	got := topicsample.Detect("t", [][]byte{[]byte(`{"a":1}`)}, nil, nil, nil)
	if got.Format != topicsample.FormatJSON {
		t.Fatalf("Format = %s, want json", got.Format)
	}
}

// TestDetect_TopicNameTieBreak checks that among identically-shaped types,
// each in its own file at index 0, the one whose name shares the most words
// with the topic wins.
func TestDetect_TopicNameTieBreak(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Globex", "Initech", "AmericaOnLine"} {
		src := fmt.Sprintf("syntax = \"proto3\";\npackage pay.gateway;\nmessage %sChargeRequest { string id = 1; }\n", name)
		if err := os.WriteFile(filepath.Join(dir, name+".proto"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{dir})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: %v %v", err, failures)
	}
	value := withHeader(1, []byte{0x00}, encode(t, msgs, "pay.gateway.GlobexChargeRequest", `{"id":"x"}`))

	tests := []struct {
		topic string
		want  string
	}{
		{topic: "shop.payment-gateway.charge-requests.initech", want: "pay.gateway.InitechChargeRequest"},
		{topic: "shop.payment-gateway.charge-requests.aol", want: "pay.gateway.AmericaOnLineChargeRequest"},
		{topic: "shop.payment-gateway.charge-requests.globex", want: "pay.gateway.GlobexChargeRequest"},
		{topic: "unrelated", want: "pay.gateway.GlobexChargeRequest"},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := topicsample.Detect(tt.topic, [][]byte{value}, msgs, nil, nil)
			if got.MessageName != tt.want {
				t.Errorf("MessageName = %s, want %s (candidates %v)", got.MessageName, tt.want, got.Candidates)
			}
			if len(got.Candidates) != 2 {
				t.Errorf("Candidates = %v, want the other 2 types", got.Candidates)
			}
		})
	}
}

func ExampleDetect() {
	got := topicsample.Detect("t", [][]byte{append([]byte{0, 0, 0, 0, 42}, `{"id":1}`...)}, nil, nil, nil)
	fmt.Println(got.Format, got.CutConfluencePrefix, got.SchemaIDs, got.Rows[0].Decoded)
	// Output: json true [42] {"id":1}
}

// stubAdvisor is a test double for topicsample.Advisor that records its last
// call and returns a canned answer.
type stubAdvisor struct {
	pick       string
	confidence float64
	ok         bool

	gotTopic      string
	gotCandidates []string
	gotDecoded    []string
}

func (s *stubAdvisor) Review(topic string, candidates []string, decodedSamples []string) (string, float64, bool) {
	s.gotTopic = topic
	s.gotCandidates = candidates
	s.gotDecoded = decodedSamples
	return s.pick, s.confidence, s.ok
}

// TestDetect_AdvisorReviewsGenuineTie checks that on an unresolved structural
// tie (TestDetect_TopicNameTieBreak's "unrelated" topic, where all three
// candidates share the same mention and score), Detect consults the Advisor
// with the structural winner plus its candidates and the decoded samples,
// and records its opinion without changing MessageName.
func TestDetect_AdvisorReviewsGenuineTie(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Globex", "Initech", "AmericaOnLine"} {
		src := fmt.Sprintf("syntax = \"proto3\";\npackage pay.gateway;\nmessage %sChargeRequest { string id = 1; }\n", name)
		if err := os.WriteFile(filepath.Join(dir, name+".proto"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	msgs, failures, err := protoinspect.LoadMessages([]string{dir})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: %v %v", err, failures)
	}
	value := withHeader(1, []byte{0x00}, encode(t, msgs, "pay.gateway.GlobexChargeRequest", `{"id":"x"}`))

	t.Run("advisor agrees", func(t *testing.T) {
		adv := &stubAdvisor{pick: "pay.gateway.GlobexChargeRequest", confidence: 0.9, ok: true}
		got := topicsample.Detect("unrelated", [][]byte{value}, msgs, nil, adv)
		if !got.Reviewed {
			t.Fatal("Reviewed = false, want true (genuine tie)")
		}
		if adv.gotTopic != "unrelated" {
			t.Errorf("advisor topic = %q, want unrelated", adv.gotTopic)
		}
		wantCandidates := append([]string{got.MessageName}, got.Candidates...)
		if !slices.Equal(adv.gotCandidates, wantCandidates) {
			t.Errorf("advisor candidates = %v, want %v", adv.gotCandidates, wantCandidates)
		}
		if len(adv.gotDecoded) != 1 || adv.gotDecoded[0] != got.Rows[0].Decoded {
			t.Errorf("advisor decoded samples = %v, want [%q]", adv.gotDecoded, got.Rows[0].Decoded)
		}
		if got.ReviewPick != "pay.gateway.GlobexChargeRequest" || !got.ReviewAgrees {
			t.Errorf("ReviewPick = %q, ReviewAgrees = %v, want agreement", got.ReviewPick, got.ReviewAgrees)
		}
		if got.ReviewConfidence != 0.9 {
			t.Errorf("ReviewConfidence = %v, want 0.9", got.ReviewConfidence)
		}
		if got.MessageName != "pay.gateway.GlobexChargeRequest" {
			t.Errorf("MessageName = %s, advisor must never change the structural pick", got.MessageName)
		}
	})

	t.Run("advisor disagrees", func(t *testing.T) {
		adv := &stubAdvisor{pick: "pay.gateway.InitechChargeRequest", confidence: 0.6, ok: true}
		got := topicsample.Detect("unrelated", [][]byte{value}, msgs, nil, adv)
		if got.MessageName != "pay.gateway.GlobexChargeRequest" {
			t.Errorf("MessageName = %s, advisor disagreement must not change the structural pick", got.MessageName)
		}
		if got.ReviewAgrees {
			t.Error("ReviewAgrees = true, want false")
		}
		if got.ReviewPick != "pay.gateway.InitechChargeRequest" {
			t.Errorf("ReviewPick = %s, want InitechChargeRequest", got.ReviewPick)
		}
	})

	t.Run("advisor has no opinion", func(t *testing.T) {
		adv := &stubAdvisor{ok: false}
		got := topicsample.Detect("unrelated", [][]byte{value}, msgs, nil, adv)
		if !got.Reviewed {
			t.Error("Reviewed = false, want true")
		}
		if got.ReviewPick != "" || got.ReviewAgrees {
			t.Errorf("ReviewPick = %q, ReviewAgrees = %v, want zero values when advisor has no opinion", got.ReviewPick, got.ReviewAgrees)
		}
	})

	t.Run("no advisor behaves as before", func(t *testing.T) {
		got := topicsample.Detect("unrelated", [][]byte{value}, msgs, nil, nil)
		if got.Reviewed {
			t.Error("Reviewed = true, want false when no Advisor is supplied")
		}
		if got.MessageName != "pay.gateway.GlobexChargeRequest" {
			t.Errorf("MessageName = %s, want unchanged regression behaviour", got.MessageName)
		}
	})

	t.Run("no tie, advisor not consulted", func(t *testing.T) {
		msgs := loadMessages(t)
		order := encode(t, msgs, "t.Order", `{"name":"a","id":"7"}`)
		adv := &stubAdvisor{pick: "should-not-be-used", confidence: 1, ok: true}
		got := topicsample.Detect("t", [][]byte{order}, msgs, nil, adv)
		if got.Reviewed {
			t.Error("Reviewed = true, want false: t.Order outscores t.Item (more populated fields), not a genuine structural tie")
		}
		if adv.gotTopic != "" {
			t.Error("advisor was consulted, want it skipped for a resolved (non-tied) match")
		}
	})
}

// Package topicsample detects the encoding of sampled Kafka message values:
// whether they carry a Confluent Schema Registry header and which protobuf
// message type (if any) decodes every sample cleanly, falling back to JSON
// and then unknown.
package topicsample

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"

	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/confluentwire"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Detected formats.
const (
	FormatProtobuf = "protobuf"
	FormatJSON     = "json"
	FormatUnknown  = "unknown"
	// FormatEmpty means there was nothing to decode; MessageName, if set,
	// comes from .proto comments naming the topic and is unverified.
	FormatEmpty = "empty"
)

const maxCandidates = 5

// ResolvedBy values: how the schema/documentation of a topic was determined.
const (
	// ResolvedByOverride is set by callers when a user override applies;
	// Detect never sets it.
	ResolvedByOverride = "override"
	// ResolvedByAsyncAPI means an AsyncAPI spec documents the topic.
	ResolvedByAsyncAPI = "asyncapi"
	// ResolvedByProtoComment means a guess from .proto comments naming the topic.
	ResolvedByProtoComment = "proto-comment"
	// ResolvedByDetected means the samples were matched structurally.
	ResolvedByDetected = "detected"
)

// Advisor is a content-based second opinion consulted only when the
// structural match between candidate message types is a genuine tie (same
// mention level, same populated-field score). It never picks a schema on its
// own: Detect always keeps the structural winner as MessageName, surfacing
// the advisor's pick separately for a human to weigh in review.
//
// Implementations must be safe for concurrent use if Detect is ever called
// concurrently for different topics sharing the same Advisor, which is the
// expected usage (one model, loaded once, consulted per topic).
type Advisor interface {
	// Review asks which of candidates the decoded JSON samples best match,
	// reporting ok=false when it has no opinion (e.g. every sample empty).
	Review(topic string, candidates []string, decodedSamples []string) (pick string, confidence float64, ok bool)
}

// Row is the per-message detection outcome, aligned with Detect's input.
type Row struct {
	// HeaderLen is the Confluent header length, 0 when there is none.
	HeaderLen      int
	SchemaID       *int32
	MessageIndexes []int32
	DecodeOK       bool
	// Decoded is the message as JSON when DecodeOK.
	Decoded string
}

// TopicResult is the detection verdict for one topic.
type TopicResult struct {
	Format              string
	MessageName         string
	Root                string
	File                string
	CutConfluencePrefix bool
	SchemaIDs           []int32
	// MessageIndexes are the distinct Confluent index paths seen.
	MessageIndexes [][]int32
	SampleCount    int
	MatchedCount   int
	// Candidates are runner-up message names that also decoded every sample.
	Candidates []string
	Rows       []Row

	// Reviewed is true when the match was a structural tie and an Advisor
	// was consulted (regardless of whether it had an opinion).
	Reviewed bool
	// ReviewPick is the Advisor's choice among MessageName and Candidates,
	// empty if it had no opinion. It is never applied automatically.
	ReviewPick string
	// ReviewConfidence is the Advisor's confidence in ReviewPick (1 -
	// normalised entropy), 0 if ReviewPick is empty.
	ReviewConfidence float64
	// ReviewAgrees is true when ReviewPick equals MessageName.
	ReviewAgrees bool

	// ResolvedBy is one of the ResolvedBy* constants, empty when nothing
	// was resolved.
	ResolvedBy string
	// Spec is the AsyncAPI documentation of the topic, nil when none.
	Spec *SpecRef
	// Warnings are spec-validation findings (at most 10, deduplicated).
	Warnings []string
}

// SpecRef points at the AsyncAPI spec entry documenting a topic.
type SpecRef struct {
	File      string
	SpecTitle string
	Topic     string
	// Message is the chosen message name, empty when none could be chosen.
	Message   string
	EventType string
	// Candidates are all message names the spec lists for the topic.
	Candidates []string
}

type match struct {
	name      string
	info      protoinspect.MessageInfo
	score     int
	pathMatch bool
	nameScore int
	mention   int
}

// Detect classifies values, which are raw (unmunged) Kafka message values
// read from topic. Empty values (tombstones) are ignored for the verdict.
// messages may be nil, in which case only JSON and unknown can be detected.
// specs may be nil, in which case no AsyncAPI documentation or validation
// is attempted.
// advisor may be nil, in which case structural ties are left unreviewed.
func Detect(topic string, values [][]byte, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], specs *asyncapi.Index, advisor Advisor) TopicResult {
	res := detect(topic, values, messages, advisor)
	applySpec(&res, topic, specs)
	return res
}

func detect(topic string, values [][]byte, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], advisor Advisor) TopicResult {
	res := TopicResult{Format: FormatUnknown, SampleCount: len(values), Rows: make([]Row, len(values))}

	var nonEmpty []int
	headers := make([]*confluentwire.Header, len(values))
	allHeaders := true
	for i, v := range values {
		if len(v) == 0 {
			continue
		}
		nonEmpty = append(nonEmpty, i)
		if h, err := confluentwire.ParseHeader(v); err == nil {
			headers[i] = &h
		} else {
			allHeaders = false
		}
	}
	if len(nonEmpty) == 0 {
		res.Format = FormatEmpty
		if messages != nil {
			if docs := documentedMessages(topic, messages); len(docs) > 0 {
				res.ResolvedBy = ResolvedByProtoComment
				res.MessageName = docs[0].name
				res.Root = docs[0].info.Root
				res.File = docs[0].info.File
				for _, m := range docs[1:min(len(docs), maxCandidates+1)] {
					res.Candidates = append(res.Candidates, m.name)
				}
			}
		}
		return res
	}

	if allHeaders {
		res.SchemaIDs, res.MessageIndexes = distinctHeaderInfo(headers, nonEmpty)
	}

	if messages != nil && messages.Len() > 0 {
		payloads := make([][]byte, len(nonEmpty))
		for j, i := range nonEmpty {
			payloads[j] = values[i]
		}
		matches := matchProtobuf(topic, payloads, messages, nil)
		cut := false
		if len(matches) == 0 && allHeaders && len(res.MessageIndexes) == 1 {
			for j, i := range nonEmpty {
				payloads[j] = values[i][headers[i].Len:]
			}
			matches = matchProtobuf(topic, payloads, messages, res.MessageIndexes[0])
			cut = true
		}
		if len(matches) > 0 {
			best := matches[0]
			res.Format = FormatProtobuf
			res.ResolvedBy = ResolvedByDetected
			res.MessageName = best.name
			res.Root = best.info.Root
			res.File = best.info.File
			res.CutConfluencePrefix = cut
			for _, m := range matches[1:min(len(matches), maxCandidates+1)] {
				res.Candidates = append(res.Candidates, m.name)
			}
			for j, i := range nonEmpty {
				row := headerRow(headers[i], cut)
				msg := dynamicpb.NewMessage(best.info.Descriptor)
				if err := proto.Unmarshal(payloads[j], msg); err == nil {
					row.DecodeOK = true
					if b, err := protojson.Marshal(msg); err == nil {
						row.Decoded = string(b)
					}
				}
				res.Rows[i] = row
			}
			res.MatchedCount = len(nonEmpty)
			reviewTie(&res, advisor, topic, matches)
			return res
		}
	}

	if detectJSON(values, nonEmpty, &res) {
		return res
	}

	for _, i := range nonEmpty {
		res.Rows[i] = headerRow(headers[i], true)
	}
	return res
}

// reviewTie consults advisor when the top two protobuf matches are a
// genuine structural tie (same topic mention, same populated-field score, so
// matchProtobuf's ordering came down to the softer name/path tiebreakers).
// It only ever annotates res; MessageName is left untouched.
func reviewTie(res *TopicResult, advisor Advisor, topic string, matches []match) {
	if advisor == nil || len(matches) < 2 {
		return
	}
	if matches[0].mention != matches[1].mention || matches[0].score != matches[1].score {
		return
	}
	candidates := []string{res.MessageName}
	candidates = append(candidates, res.Candidates...)

	var decoded []string
	for _, row := range res.Rows {
		if row.DecodeOK && row.Decoded != "" {
			decoded = append(decoded, row.Decoded)
		}
	}

	res.Reviewed = true
	pick, confidence, ok := advisor.Review(topic, candidates, decoded)
	if !ok {
		return
	}
	res.ReviewPick = pick
	res.ReviewConfidence = confidence
	res.ReviewAgrees = pick == res.MessageName
}

// headerRow fills a Row's Confluent header fields; withLen reports the
// header length only when it was (or would be) stripped.
func headerRow(h *confluentwire.Header, withLen bool) Row {
	if h == nil {
		return Row{}
	}
	id := h.SchemaID
	row := Row{SchemaID: &id, MessageIndexes: h.MessageIndexes}
	if withLen {
		row.HeaderLen = h.Len
	}
	return row
}

func distinctHeaderInfo(headers []*confluentwire.Header, idx []int) ([]int32, [][]int32) {
	var ids []int32
	var paths [][]int32
	for _, i := range idx {
		h := headers[i]
		if !slices.Contains(ids, h.SchemaID) {
			ids = append(ids, h.SchemaID)
		}
		if !slices.ContainsFunc(paths, func(p []int32) bool { return slices.Equal(p, h.MessageIndexes) }) {
			paths = append(paths, h.MessageIndexes)
		}
	}
	slices.Sort(ids)
	return ids, paths
}

func detectJSON(values [][]byte, nonEmpty []int, res *TopicResult) bool {
	raw, cut := true, true
	ids := make([]int32, len(values))
	for _, i := range nonEmpty {
		if raw && !json.Valid(values[i]) {
			raw = false
		}
		if cut {
			id, err := confluentwire.ParseJSONHeader(values[i])
			if err != nil || !json.Valid(values[i][confluentwire.JSONHeaderLen:]) {
				cut = false
			} else {
				ids[i] = id
			}
		}
	}
	if !raw && !cut {
		return false
	}
	res.Format = FormatJSON
	res.ResolvedBy = ResolvedByDetected
	res.MatchedCount = len(nonEmpty)
	res.CutConfluencePrefix = !raw
	res.MessageIndexes = nil
	res.SchemaIDs = nil
	for _, i := range nonEmpty {
		if raw {
			res.Rows[i] = Row{DecodeOK: true, Decoded: string(values[i])}
			continue
		}
		id := ids[i]
		res.Rows[i] = Row{HeaderLen: confluentwire.JSONHeaderLen, SchemaID: &id, DecodeOK: true, Decoded: string(values[i][confluentwire.JSONHeaderLen:])}
		if !slices.Contains(res.SchemaIDs, id) {
			res.SchemaIDs = append(res.SchemaIDs, id)
		}
	}
	slices.Sort(res.SchemaIDs)
	return true
}

// documentedMessages returns the messages whose own comments name topic, or
// failing that the top-level messages of files that name it, best first.
func documentedMessages(topic string, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]) []match {
	topicWords := topicTokens(topic)
	var inComments, inFile []match
	messages.Range(func(name string, info protoinspect.MessageInfo) bool {
		m := match{name: name, info: info, nameScore: nameScore(topicWords, name)}
		switch topicMention(topic, info) {
		case mentionInMessageComments:
			inComments = append(inComments, m)
		case mentionInFile:
			if _, nested := info.Descriptor.Parent().(protoreflect.MessageDescriptor); !nested {
				inFile = append(inFile, m)
			}
		}
		return true
	})
	docs := inComments
	if len(docs) == 0 {
		docs = inFile
	}
	sort.SliceStable(docs, func(i, j int) bool {
		a, b := docs[i], docs[j]
		if a.nameScore != b.nameScore {
			return a.nameScore > b.nameScore
		}
		if len(a.name) != len(b.name) {
			return len(a.name) < len(b.name)
		}
		return a.name < b.name
	})
	return docs
}

// matchProtobuf returns every message type that decodes all payloads
// cleanly, best first.
func matchProtobuf(topic string, payloads [][]byte, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo], indexPath []int32) []match {
	topicWords := topicTokens(topic)
	var matches []match
	messages.Range(func(name string, info protoinspect.MessageInfo) bool {
		score, ok := scoreMessage(info.Descriptor, payloads)
		if ok {
			matches = append(matches, match{
				name:      name,
				info:      info,
				score:     score,
				pathMatch: indexPath != nil && slices.Equal(messagePath(info.Descriptor), indexPath),
				nameScore: nameScore(topicWords, name),
				mention:   topicMention(topic, info),
			})
		}
		return true
	})
	sort.SliceStable(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.mention != b.mention {
			return a.mention > b.mention
		}
		if a.score != b.score {
			return a.score > b.score
		}
		if a.pathMatch != b.pathMatch {
			return a.pathMatch
		}
		if a.nameScore != b.nameScore {
			return a.nameScore > b.nameScore
		}
		if len(a.name) != len(b.name) {
			return len(a.name) < len(b.name)
		}
		return strings.Compare(a.name, b.name) < 0
	})
	return matches
}

// scoreMessage reports whether every payload decodes as md with no unknown
// fields and at least one populated field, scoring by total populated fields.
func scoreMessage(md protoreflect.MessageDescriptor, payloads [][]byte) (int, bool) {
	total := 0
	for _, p := range payloads {
		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(p, msg); err != nil {
			return 0, false
		}
		n, clean := inspect(msg)
		if !clean || n == 0 {
			return 0, false
		}
		total += n
	}
	return total, true
}

// inspect counts populated fields recursively and reports whether msg and
// all nested messages are free of unknown fields.
func inspect(msg protoreflect.Message) (int, bool) {
	if len(msg.GetUnknown()) > 0 {
		return 0, false
	}
	count := 0
	clean := true
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		count++
		switch {
		case fd.IsList() && fd.Message() != nil:
			list := v.List()
			for i := 0; i < list.Len() && clean; i++ {
				n, ok := inspect(list.Get(i).Message())
				count += n
				clean = ok
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				n, ok := inspect(mv.Message())
				count += n
				clean = ok
				return clean
			})
		case !fd.IsList() && !fd.IsMap() && fd.Message() != nil:
			n, ok := inspect(v.Message())
			count += n
			clean = ok
		}
		return clean
	})
	return count, clean
}

// messagePath returns md's position within its .proto file, e.g. [1, 0] for
// the first nested message of the second top-level message.
func messagePath(md protoreflect.MessageDescriptor) []int32 {
	var path []int32
	for d := protoreflect.Descriptor(md); d != nil; d = d.Parent() {
		m, ok := d.(protoreflect.MessageDescriptor)
		if !ok {
			break
		}
		path = append(path, int32(m.Index()))
	}
	slices.Reverse(path)
	return path
}

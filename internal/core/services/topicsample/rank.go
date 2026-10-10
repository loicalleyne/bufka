package topicsample

import (
	"github.com/loicalleyne/bufka/pkg/confluentwire"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Candidate is one protobuf message type that decodes every sampled value
// cleanly, as ranked by Detect.
type Candidate struct {
	Name string
	Info protoinspect.MessageInfo
	// CutConfluencePrefix is true when the match only holds once the
	// Confluent header is stripped.
	CutConfluencePrefix bool
	// Score is the total populated-field count across the samples.
	Score int
}

// Rank returns every protobuf message that decodes all non-empty values
// cleanly, best first, using the same ordering as Detect (Detect's
// MessageName is Rank's first entry, its Candidates the next few). Unlike
// Detect it is not capped, so a human can pick among all of them. It returns
// nil when nothing matches or values holds no non-empty value.
func Rank(topic string, values [][]byte, messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]) []Candidate {
	if messages == nil || messages.Len() == 0 {
		return nil
	}
	var payloads [][]byte
	var headers []*confluentwire.Header
	allHeaders := true
	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		payloads = append(payloads, v)
		if h, err := confluentwire.ParseHeader(v); err == nil {
			headers = append(headers, &h)
		} else {
			headers = append(headers, nil)
			allHeaders = false
		}
	}
	if len(payloads) == 0 {
		return nil
	}

	matches := matchProtobuf(topic, payloads, messages, nil)
	cut := false
	if len(matches) == 0 && allHeaders {
		idx := make([]int, len(headers))
		for i := range idx {
			idx[i] = i
		}
		if _, paths := distinctHeaderInfo(headers, idx); len(paths) == 1 {
			stripped := make([][]byte, len(payloads))
			for i, p := range payloads {
				stripped[i] = p[headers[i].Len:]
			}
			matches = matchProtobuf(topic, stripped, messages, paths[0])
			cut = true
		}
	}
	out := make([]Candidate, len(matches))
	for i, m := range matches {
		out[i] = Candidate{Name: m.name, Info: m.info, CutConfluencePrefix: cut, Score: m.score}
	}
	return out
}

// DecodeWith decodes values as info's message type, stripping the Confluent
// header first when cut is true and the value carries one. Unlike Detect it
// never rejects the type: rows that fail to decode simply have DecodeOK
// false, so a user-chosen schema can be previewed and applied even when it
// is not a clean match. matched counts the rows that decoded.
func DecodeWith(values [][]byte, info protoinspect.MessageInfo, cut bool) (rows []Row, matched int) {
	rows = make([]Row, len(values))
	for i, v := range values {
		if len(v) == 0 {
			continue
		}
		var hp *confluentwire.Header
		payload := v
		if h, err := confluentwire.ParseHeader(v); err == nil {
			hp = &h
			if cut {
				payload = v[h.Len:]
			}
		}
		row := headerRow(hp, cut)
		msg := dynamicpb.NewMessage(info.Descriptor)
		if err := proto.Unmarshal(payload, msg); err == nil {
			row.DecodeOK = true
			matched++
			if b, err := protojson.Marshal(msg); err == nil {
				row.Decoded = string(b)
			}
		}
		rows[i] = row
	}
	return rows, matched
}

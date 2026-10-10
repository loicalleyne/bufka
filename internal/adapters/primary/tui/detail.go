package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/internal/core/services/topicsize"
)

// detailTab identifies one tab of the topic detail view.
type detailTab int

const (
	tabMetadata detailTab = iota
	tabSchema
	tabSample
	tabFetch
	tabTail
	tabSize
	tabCount
)

func (t detailTab) String() string {
	switch t {
	case tabMetadata:
		return "Metadata"
	case tabSchema:
		return "Schema"
	case tabSample:
		return "Sample data"
	case tabFetch:
		return "Fetch"
	case tabTail:
		return "Live tail"
	case tabSize:
		return "Size estimate"
	}
	return ""
}

// tailRingCap bounds the in-memory live-tail buffer.
const tailRingCap = 500

type detailModel struct {
	deps  Deps
	topic string
	tab   detailTab

	width, height int

	spinner spinner.Model
	loading map[detailTab]bool

	metaVP viewport.Model
	meta   kafka.TopicDescription
	metaOK bool

	sampleVP  viewport.Model
	sample    []database.SampleRow
	summary   database.SampleSummary
	summaryOK bool

	fetchVP      viewport.Model
	fetched      []kafka.SampledRecord
	fetchDecoded []topicsample.Row

	tailVP      viewport.Model
	tailRecords []kafka.SampledRecord
	tailDecoded []topicsample.Row
	tailRunning bool
	tailPaused  bool
	tailCancel  context.CancelFunc
	tailCh      chan kafka.SampledRecord

	sizeVP   viewport.Model
	estimate database.EstimateRow
	sizeOK   bool

	// lastResult is the most recent detection result for the topic.
	lastResult *topicsample.TopicResult
	schema     schemaTabState
}

func newDetailModel(deps Deps, topic string) detailModel {
	return detailModel{
		deps:    deps,
		topic:   topic,
		spinner: spinner.New(),
		loading: map[detailTab]bool{},
	}
}

func (m detailModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.loadTabCmd(tabMetadata))
}

func (m detailModel) filtering() bool { return false }

func (m detailModel) stopTail() {
	if m.tailCancel != nil {
		m.tailCancel()
	}
}

func (m detailModel) resize(w, h int) (detailModel, tea.Cmd) {
	m.width, m.height = w, h
	vh := h - 2
	if vh < 1 {
		vh = 1
	}
	m.metaVP = resizedViewport(m.metaVP, w, vh)
	m.sampleVP = resizedViewport(m.sampleVP, w, vh)
	m.fetchVP = resizedViewport(m.fetchVP, w, vh)
	m.tailVP = resizedViewport(m.tailVP, w, vh)
	m.sizeVP = resizedViewport(m.sizeVP, w, vh)
	m.schema = m.schema.resize(w, vh)
	return m, nil
}

func resizedViewport(vp viewport.Model, w, h int) viewport.Model {
	if vp.Width() == 0 && vp.Height() == 0 {
		vp = viewport.New(viewport.WithWidth(w), viewport.WithHeight(h))
	} else {
		vp.SetWidth(w)
		vp.SetHeight(h)
	}
	return vp
}

// --- background commands ---

type metaLoadedMsg struct {
	desc kafka.TopicDescription
	err  error
}

type sampleLoadedMsg struct {
	rows    []database.SampleRow
	summary database.SampleSummary
	ok      bool
	err     error
	// res is set when the sample was live-detected (not read from storage).
	res *topicsample.TopicResult
}

// sampleActionDoneMsg reports a finished "s" sampling action.
type sampleActionDoneMsg struct {
	res     topicsample.TopicResult
	records int
}

type fetchLoadedMsg struct {
	records []kafka.SampledRecord
	decoded []topicsample.Row
	err     error
}

type sizeLoadedMsg struct {
	row database.EstimateRow
	err error
}

type tailRecordMsg struct {
	record kafka.SampledRecord
	ch     chan kafka.SampledRecord
}

type tailStoppedMsg struct{ err error }

func (m detailModel) loadTabCmd(tab detailTab) tea.Cmd {
	switch tab {
	case tabMetadata:
		return func() tea.Msg {
			desc, err := kafka.DescribeTopic(context.Background(), m.deps.KafkaConf, m.topic)
			return metaLoadedMsg{desc: desc, err: err}
		}
	case tabSample:
		return m.loadSampleCmd()
	case tabSize:
		return func() tea.Msg {
			if m.deps.DB == nil {
				return sizeLoadedMsg{err: fmt.Errorf("no database (DUCKDB_PATH is :memory:); no stored size estimate")}
			}
			rows, err := database.ReadEstimateTable(context.Background(), m.deps.DB)
			if err != nil {
				return sizeLoadedMsg{err: err}
			}
			for _, r := range rows {
				if r.Topic == m.topic {
					return sizeLoadedMsg{row: r}
				}
			}
			return sizeLoadedMsg{err: fmt.Errorf("no stored size estimate for %q; run the estimate-size action (e)", m.topic)}
		}
	}
	return nil
}

// loadSampleCmd reads the topic's stored _sample table when present, else
// live-samples it with a short timeout, mirroring kafka show-schema.
func (m detailModel) loadSampleCmd() tea.Cmd {
	deps := m.deps
	topic := m.topic
	return func() tea.Msg {
		ctx := context.Background()
		table, err := kafkaingest.SampleTableName(topic)
		if err != nil {
			return sampleLoadedMsg{err: err}
		}
		if deps.DB != nil {
			if rows, ok, err := database.ReadSampleTable(ctx, deps.DB, table); err == nil && ok {
				summary, sumOK, _ := database.ReadSampleSummary(ctx, deps.DB, table)
				return sampleLoadedMsg{rows: rows, summary: summary, ok: sumOK}
			}
		}
		sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		count := deps.SampleCount
		if count <= 0 {
			count = 50
		}
		records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
		if err != nil && err != kafka.ErrTopicEmpty {
			return sampleLoadedMsg{err: err}
		}
		values := make([][]byte, len(records))
		for i, r := range records {
			values[i] = r.Value
		}
		res := topicsample.Detect(topic, values, deps.Messages, deps.AsyncAPI, deps.Advisor)
		rows := make([]database.SampleRow, len(records))
		for i, r := range records {
			d := res.Rows[i]
			rows[i] = database.SampleRow{
				Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp,
				Key: r.Key, Value: r.Value, DetectedFormat: res.Format, MessageName: res.MessageName,
				DecodeOK: d.DecodeOK, DecodedJSON: d.Decoded,
			}
		}
		summary := database.SampleSummary{
			Format: res.Format, MessageName: res.MessageName, CutConfluencePrefix: res.CutConfluencePrefix,
			MessageIndexes: res.MessageIndexes, MatchedCount: res.MatchedCount, SampleCount: res.SampleCount,
		}
		return sampleLoadedMsg{rows: rows, summary: summary, ok: true, res: &res}
	}
}

func fetchCmd(deps Deps, topic string) tea.Cmd {
	count := deps.SampleCount
	if count <= 0 {
		count = 50
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		records, err := kafka.FetchTopic(ctx, deps.KafkaConf, topic, count)
		if err != nil && err != kafka.ErrTopicEmpty {
			return fetchLoadedMsg{err: err}
		}
		return fetchLoadedMsg{records: records, decoded: decodeRecords(deps, topic, records)}
	}
}

// decodeRecords runs topicsample.Detect over records' raw values so that
// live-fetched/tailed messages can be rendered as decoded JSON, the same way
// stored samples are in the Sample data tab.
func decodeRecords(deps Deps, topic string, records []kafka.SampledRecord) []topicsample.Row {
	if len(records) == 0 {
		return nil
	}
	values := make([][]byte, len(records))
	for i, r := range records {
		values[i] = r.Value
	}
	res := topicsample.Detect(topic, values, deps.Messages, deps.AsyncAPI, deps.Advisor)
	return res.Rows
}

// startTailCmd starts kafka.TailTopic in a background goroutine and returns a
// command that waits for the first record (or the goroutine's end).
func startTailCmd(ctx context.Context, deps Deps, topic string, ch chan kafka.SampledRecord) tea.Cmd {
	return func() tea.Msg {
		err := kafka.TailTopic(ctx, deps.KafkaConf, topic, ch)
		return tailStoppedMsg{err: err}
	}
}

// waitTailRecordCmd waits for the next record on ch.
func waitTailRecordCmd(ch chan kafka.SampledRecord) tea.Cmd {
	return func() tea.Msg {
		r, ok := <-ch
		if !ok {
			return nil
		}
		return tailRecordMsg{record: r, ch: ch}
	}
}

// --- update/view ---

func (m detailModel) update(ctx context.Context, msg tea.Msg) (detailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case metaLoadedMsg:
		m.loading[tabMetadata] = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		m.meta, m.metaOK = msg.desc, true
		m.metaVP.SetContent(renderMeta(msg.desc))
		return m, setStatus(fmt.Sprintf("%s: %d partitions", m.topic, msg.desc.Partitions))

	case sampleLoadedMsg:
		m.loading[tabSample] = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		m.sample, m.summary, m.summaryOK = msg.rows, msg.summary, msg.ok
		if msg.res != nil {
			m.lastResult = msg.res
			m.refreshSchema()
		}
		m.sampleVP.SetContent(renderSample(msg.summary, msg.ok, msg.rows))
		return m, setStatus(fmt.Sprintf("%s: %d sampled messages", m.topic, len(msg.rows)))

	case sampleActionDoneMsg:
		res := msg.res
		m.lastResult = &res
		m.refreshSchema()
		return m, setStatus(fmt.Sprintf("sampled %d messages from %s (format=%s)", msg.records, m.topic, res.Format))

	case fetchLoadedMsg:
		m.loading[tabFetch] = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		m.fetched = msg.records
		m.fetchDecoded = msg.decoded
		m.fetchVP.SetContent(renderFetch(msg.records, msg.decoded))
		return m, setStatus(fmt.Sprintf("%s: fetched %d messages", m.topic, len(msg.records)))

	case sizeLoadedMsg:
		m.loading[tabSize] = false
		if msg.err != nil {
			return m, setErr(msg.err)
		}
		m.estimate, m.sizeOK = msg.row, true
		m.sizeVP.SetContent(renderSize(msg.row))
		return m, setStatus(fmt.Sprintf("%s: size estimate loaded", m.topic))

	case tailRecordMsg:
		if !m.tailPaused {
			m.tailRecords = append(m.tailRecords, msg.record)
			decoded := decodeRecords(m.deps, m.topic, []kafka.SampledRecord{msg.record})
			var row topicsample.Row
			if len(decoded) == 1 {
				row = decoded[0]
			}
			m.tailDecoded = append(m.tailDecoded, row)
			if len(m.tailRecords) > tailRingCap {
				m.tailRecords = m.tailRecords[len(m.tailRecords)-tailRingCap:]
				m.tailDecoded = m.tailDecoded[len(m.tailDecoded)-tailRingCap:]
			}
			m.tailVP.SetContent(renderTail(m.tailRecords, m.tailDecoded))
			m.tailVP.GotoBottom()
		}
		if m.tailRunning {
			return m, waitTailRecordCmd(msg.ch)
		}
		return m, nil

	case tailStoppedMsg:
		m.tailRunning = false
		if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
			return m, setErr(msg.err)
		}
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tea.KeyPressMsg:
		switch msg.String() {
		case "tab", "l":
			m.tab = (m.tab + 1) % tabCount
			m.enterTab()
			return m, m.ensureLoaded(m.tab)
		case "h", "shift+tab":
			m.tab = (m.tab - 1 + tabCount) % tabCount
			m.enterTab()
			return m, m.ensureLoaded(m.tab)
		case "t":
			if m.tab == tabTail {
				return m.toggleTail(ctx)
			}
		case "p":
			if m.tab == tabTail && m.tailRunning {
				m.tailPaused = !m.tailPaused
				return m, nil
			}
		case "f":
			if m.tab == tabFetch {
				m.loading[tabFetch] = true
				return m, tea.Batch(m.spinner.Tick, fetchCmd(m.deps, m.topic))
			}
		case "s":
			return m, m.runSampleAction()
		case "e":
			return m, m.runEstimateAction()
		case "r":
			m.loading[tabSample] = true
			return m, tea.Batch(m.spinner.Tick, m.loadSampleCmd())
		}
	}

	var cmd tea.Cmd
	switch m.tab {
	case tabMetadata:
		m.metaVP, cmd = m.metaVP.Update(msg)
	case tabSchema:
		m.schema.table, cmd = m.schema.table.update(msg)
	case tabSample:
		m.sampleVP, cmd = m.sampleVP.Update(msg)
	case tabFetch:
		m.fetchVP, cmd = m.fetchVP.Update(msg)
	case tabTail:
		m.tailVP, cmd = m.tailVP.Update(msg)
	case tabSize:
		m.sizeVP, cmd = m.sizeVP.Update(msg)
	}
	return m, cmd
}

func (m *detailModel) toggleTail(ctx context.Context) (detailModel, tea.Cmd) {
	if m.tailRunning {
		m.stopTail()
		m.tailRunning = false
		return *m, nil
	}
	tailCtx, cancel := context.WithCancel(ctx)
	m.tailCancel = cancel
	m.tailCh = make(chan kafka.SampledRecord, 64)
	m.tailRunning = true
	m.tailPaused = false
	return *m, tea.Batch(startTailCmd(tailCtx, m.deps, m.topic, m.tailCh), waitTailRecordCmd(m.tailCh))
}

func (m detailModel) ensureLoaded(tab detailTab) tea.Cmd {
	if m.loading[tab] {
		return nil
	}
	switch tab {
	case tabMetadata:
		if m.metaOK {
			return nil
		}
	case tabSample:
		if m.summaryOK {
			return nil
		}
	case tabSize:
		if m.sizeOK {
			return nil
		}
	case tabFetch, tabTail, tabSchema:
		return nil
	}
	m.loading[tab] = true
	cmd := m.loadTabCmd(tab)
	if cmd == nil {
		return nil
	}
	return tea.Batch(m.spinner.Tick, cmd)
}

// runSampleAction mirrors "kafka sample-messages" for just this topic.
func (m detailModel) runSampleAction() tea.Cmd {
	deps := m.deps
	topic := m.topic
	return func() tea.Msg {
		ctx := context.Background()
		count := deps.SampleCount
		if count <= 0 {
			count = 50
		}
		timeout := deps.SampleTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		table, err := kafkaingest.SampleTableName(topic)
		if err != nil {
			return errMsg{err}
		}
		sampleCtx, cancel := context.WithTimeout(ctx, timeout)
		records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
		cancel()
		if err != nil && err != kafka.ErrTopicEmpty {
			return errMsg{err}
		}
		values := make([][]byte, len(records))
		for i, r := range records {
			values[i] = r.Value
		}
		res := topicsample.Detect(topic, values, deps.Messages, deps.AsyncAPI, deps.Advisor)
		rows := make([]database.SampleRow, len(records))
		for i, r := range records {
			d := res.Rows[i]
			rows[i] = database.SampleRow{
				Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp,
				Key: r.Key, Value: r.Value, DetectedFormat: res.Format, MessageName: res.MessageName,
				DecodeOK: d.DecodeOK, DecodedJSON: d.Decoded,
			}
		}
		if deps.DB != nil && len(rows) > 0 {
			if err := database.WriteSampleTable(ctx, deps.DB, table, rows); err != nil {
				return errMsg{err}
			}
			_ = database.SaveDetection(ctx, deps.DB, topic, table, res, deps.AsyncAPI, deps.Messages)
		}
		return sampleActionDoneMsg{res: res, records: len(records)}
	}
}

// runEstimateAction mirrors "kafka estimate-size" for just this topic.
func (m detailModel) runEstimateAction() tea.Cmd {
	deps := m.deps
	topic := m.topic
	return func() tea.Msg {
		ctx := context.Background()
		counts, err := kafka.TopicMessageCount(ctx, deps.KafkaConf, topic)
		if err != nil {
			return errMsg{err}
		}
		count := deps.SampleCount
		if count <= 0 {
			count = 50
		}
		timeout := deps.SampleTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		sampleCtx, cancel := context.WithTimeout(ctx, timeout)
		records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
		cancel()
		if err != nil && err != kafka.ErrTopicEmpty {
			return errMsg{err}
		}
		sizes := make([]int, len(records))
		for i, r := range records {
			size := len(r.Key) + len(r.Value)
			for _, h := range r.Headers {
				size += len(h.Key) + len(h.Value)
			}
			sizes[i] = size
		}
		res := topicsize.Estimate(counts.Total, topicsize.Stats(sizes))
		row := database.EstimateRow{
			Topic: topic, EstimatedAt: time.Now().UTC(), Partitions: int32(counts.Partitions),
			TotalMessages: res.TotalMessages, SampleCount: int32(res.SampleCount),
			AvgMessageBytes: res.AvgMessageBytes, StdDevBytes: res.StdDevBytes,
			MinMessageBytes: int32(res.MinMessageBytes), MaxMessageBytes: int32(res.MaxMessageBytes),
			EstimatedBytes: res.EstimatedBytes, LowBytes: res.LowBytes, HighBytes: res.HighBytes,
		}
		if deps.DB != nil {
			if err := database.UpsertEstimateRows(ctx, deps.DB, []database.EstimateRow{row}); err != nil {
				return errMsg{err}
			}
		}
		return sizeLoadedMsg{row: row}
	}
}

func (m detailModel) View() string {
	var b strings.Builder
	b.WriteString(renderTabs(m.tab))
	b.WriteString("\n")
	if m.loading[m.tab] {
		b.WriteString(m.spinner.View())
		b.WriteString(" loading...\n")
	}
	switch m.tab {
	case tabMetadata:
		b.WriteString(m.metaVP.View())
	case tabSchema:
		b.WriteString(m.schema.View())
	case tabSample:
		b.WriteString(m.sampleVP.View())
	case tabFetch:
		b.WriteString(m.fetchVP.View())
	case tabTail:
		state := "stopped"
		style := tailStoppedStyle
		if m.tailRunning {
			state = "running"
			style = tailRunningStyle
			if m.tailPaused {
				state = "paused"
				style = tailStoppedStyle
			}
		}
		b.WriteString(style.Render(fmt.Sprintf("[%s]", state)) + " ")
		b.WriteString(m.tailVP.View())
	case tabSize:
		b.WriteString(m.sizeVP.View())
	}
	b.WriteString("\n")
	b.WriteString(helpStyle.Render(detailHelp(m.tab)))
	return b.String()
}

func detailHelp(tab detailTab) string {
	base := "tab/shift+tab/h/l switch tabs · s sample · e estimate · r refresh-schema · m message schema · esc back"
	switch tab {
	case tabSchema:
		return "↑/↓/pgup/pgdn scroll fields · " + base
	case tabFetch:
		return "f fetch (on-demand, all partitions, newest 50/partition) · " + base
	case tabTail:
		return "t start/stop tail · p pause/resume · " + base
	}
	return base
}

func renderTabs(active detailTab) string {
	var parts []string
	for t := detailTab(0); t < tabCount; t++ {
		if t == active {
			parts = append(parts, tabActiveStyle.Render(t.String()))
		} else {
			parts = append(parts, tabInactiveStyle.Render(t.String()))
		}
	}
	return strings.Join(parts, " ")
}

func renderMeta(d kafka.TopicDescription) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Partitions: %d   Replication factor: %d\n\n", d.Partitions, d.ReplicationFactor)
	fmt.Fprintf(&b, "%-10s %-8s %-8s %-12s %-12s %-12s\n", "PARTITION", "LEADER", "REPLICAS", "START", "END", "MESSAGES")
	for _, p := range d.PartitionDetails {
		fmt.Fprintf(&b, "%-10d %-8d %-8d %-12d %-12d %-12d\n", p.Partition, p.Leader, p.Replicas, p.Start, p.End, p.Messages)
	}
	if len(d.Configs) > 0 {
		b.WriteString("\nConfigs:\n")
		for _, c := range d.Configs {
			fmt.Fprintf(&b, "  %s = %s\n", c.Key, c.Value)
		}
	}
	return b.String()
}

func renderSample(summary database.SampleSummary, ok bool, rows []database.SampleRow) string {
	var b strings.Builder
	if ok {
		fmt.Fprintf(&b, "format=%s message=%s cut=%v matched=%d/%d\n\n",
			summary.Format, summary.MessageName, summary.CutConfluencePrefix, summary.MatchedCount, summary.SampleCount)
	}
	for _, r := range rows {
		fmt.Fprintf(&b, "[p%d@%d] %s\n", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339))
		for _, h := range r.Headers {
			fmt.Fprintf(&b, "  header %s=%q\n", h.Key, h.Value)
		}
		if r.DecodeOK {
			b.WriteString("  " + prettyJSON(r.DecodedJSON) + "\n")
		} else {
			fmt.Fprintf(&b, "  <%d raw bytes, not decoded>\n", len(r.Value))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func renderFetch(records []kafka.SampledRecord, decoded []topicsample.Row) string {
	var b strings.Builder
	var partition int32 = -1
	for i, r := range records {
		if r.Partition != partition {
			fmt.Fprintf(&b, "--- partition %d ---\n", r.Partition)
			partition = r.Partition
		}
		fmt.Fprintf(&b, "[@%d] %s  key=%q (%d bytes)\n", r.Offset, r.Timestamp.Format(time.RFC3339), r.Key, len(r.Value))
		for _, h := range r.Headers {
			fmt.Fprintf(&b, "  header %s=%q\n", h.Key, h.Value)
		}
		writeDecodedRow(&b, decoded, i, r.Value)
	}
	return b.String()
}

func renderTail(records []kafka.SampledRecord, decoded []topicsample.Row) string {
	var b strings.Builder
	for i, r := range records {
		fmt.Fprintf(&b, "[p%d@%d] %s  key=%q (%d bytes)\n", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339), r.Key, len(r.Value))
		writeDecodedRow(&b, decoded, i, r.Value)
	}
	return b.String()
}

// writeDecodedRow appends the decoded JSON for decoded[i] if available and
// decoded, else falls back to treating the raw value itself as JSON (per
// record, independent of the batch-level format verdict, which can fail to
// commit when the sampled batch mixes shapes, as e.g. ksqlDB command topics
// do), else a raw-bytes note.
func writeDecodedRow(b *strings.Builder, decoded []topicsample.Row, i int, value []byte) {
	switch {
	case i < len(decoded) && decoded[i].DecodeOK:
		b.WriteString("  " + prettyJSON(decoded[i].Decoded) + "\n")
	case len(value) > 0 && json.Valid(value):
		b.WriteString("  " + prettyJSON(string(value)) + "\n")
	default:
		fmt.Fprintf(b, "  <%d raw bytes, not decoded>\n", len(value))
	}
	b.WriteString("\n")
}

func renderSize(r database.EstimateRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Total messages: %d\n", r.TotalMessages)
	fmt.Fprintf(&b, "Sample count:   %d\n", r.SampleCount)
	fmt.Fprintf(&b, "Avg size:       %s\n", humanBytes(int64(r.AvgMessageBytes)))
	fmt.Fprintf(&b, "StdDev:         %.1f bytes\n", r.StdDevBytes)
	fmt.Fprintf(&b, "Min/Max:        %d / %d bytes\n", r.MinMessageBytes, r.MaxMessageBytes)
	fmt.Fprintf(&b, "Estimated:      %s\n", humanBytes(r.EstimatedBytes))
	fmt.Fprintf(&b, "Low..High:      %s .. %s\n", humanBytes(r.LowBytes), humanBytes(r.HighBytes))
	fmt.Fprintf(&b, "Estimated at:   %s\n", r.EstimatedAt.Format(time.RFC3339))
	return b.String()
}

func prettyJSON(s string) string {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	b, err := json.MarshalIndent(v, "  ", "  ")
	if err != nil {
		return s
	}
	return string(b)
}

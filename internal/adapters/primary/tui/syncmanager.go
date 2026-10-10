package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/pkg/timeutil"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SyncStatus describes the runtime status of a topic sync job.
type SyncStatus struct {
	Topic        string
	Active       bool
	MessageCount int64
	DiskBytes    int64
	Err          error
}

type syncJob struct {
	topic  string
	cancel context.CancelFunc
	done   chan struct{}
}

// SyncManager coordinates background consumer routines for cached topics.
type SyncManager struct {
	deps Deps
	// pathsMu guards importPaths, a live override of deps.ProtoImportPaths
	// set when the user edits them in settings > schema sources.
	pathsMu      sync.RWMutex
	importPaths  []string
	includePaths []string
	pathsSet     bool
	mu           sync.Mutex
	jobs         map[string]*syncJob
	errors       map[string]string   // topic -> last error string
	logs         map[string][]string // topic -> ring buffer of recent logs/errors
}

// SetImportPaths replaces the protobuf root and include (import-only)
// paths used by jobs started from now on (running jobs keep the paths they
// started with).
func (m *SyncManager) SetImportPaths(roots, include []string) {
	m.pathsMu.Lock()
	defer m.pathsMu.Unlock()
	m.importPaths = append([]string(nil), roots...)
	m.includePaths = append([]string(nil), include...)
	m.pathsSet = true
}

// protoImportPaths returns the paths a job compiles with: the roots, then
// the include paths.
func (m *SyncManager) protoImportPaths() []string {
	m.pathsMu.RLock()
	defer m.pathsMu.RUnlock()
	roots, include := m.deps.ProtoImportPaths, m.deps.ProtoIncludePaths
	if m.pathsSet {
		roots, include = m.importPaths, m.includePaths
	}
	return append(append([]string(nil), roots...), include...)
}

// NewSyncManager initializes a SyncManager.
func NewSyncManager(deps Deps) *SyncManager {
	return &SyncManager{
		deps:   deps,
		jobs:   make(map[string]*syncJob),
		errors: make(map[string]string),
		logs:   make(map[string][]string),
	}
}

// StartActiveJobs starts background consumer goroutines for all topics configured as Active.
func (m *SyncManager) StartActiveJobs(ctx context.Context) {
	if m.deps.DB == nil {
		return
	}
	configs, err := database.ReadCacheConfigs(ctx, m.deps.DB)
	if err != nil {
		return
	}
	for _, c := range configs {
		if c.Active {
			_ = m.Start(ctx, c.Topic)
		}
	}
}

// Start launches a background sync job for topic.
func (m *SyncManager) Start(ctx context.Context, topic string) error {
	m.mu.Lock()
	if _, exists := m.jobs[topic]; exists {
		m.mu.Unlock()
		return nil // already running
	}

	if m.deps.DB == nil {
		m.mu.Unlock()
		return fmt.Errorf("duckdb is not configured")
	}

	configs, err := database.ReadCacheConfigs(ctx, m.deps.DB)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("read cache configs: %w", err)
	}

	var cfg *database.CacheConfigRow
	for _, c := range configs {
		if c.Topic == topic {
			cfg = &c
			break
		}
	}
	if cfg == nil {
		m.mu.Unlock()
		return fmt.Errorf("topic %q not found in cache configs", topic)
	}

	tableName, err := kafkaingest.TableName(topic)
	if err != nil {
		m.mu.Unlock()
		return err
	}

	// A limited sync (duration or message count) that starts from "now" is
	// re-run on resume: it consumes the configured window starting at the
	// current end of the topic instead of catching up from the last cached
	// offsets, so each resume yields the configured duration/count of new
	// messages.
	var offsets map[int32]int64
	if !rerunsOnResume(*cfg) {
		offsets, err = database.GetResumeOffsets(ctx, m.deps.DB, tableName)
		if err != nil {
			m.mu.Unlock()
			return fmt.Errorf("get resume offsets: %w", err)
		}
		stored, err := database.ReadCacheOffsets(ctx, m.deps.DB, topic)
		if err != nil {
			m.mu.Unlock()
			return fmt.Errorf("read cache offsets: %w", err)
		}
		for p, off := range stored {
			if cur, ok := offsets[p]; !ok || off > cur {
				offsets[p] = off
			}
		}
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	job := &syncJob{
		topic:  topic,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	m.jobs[topic] = job
	m.setTopicErrorLocked(topic, nil)
	m.appendLogLocked(topic, "sync started")
	m.mu.Unlock()

	go func() {
		defer close(job.done)
		defer func() {
			m.mu.Lock()
			delete(m.jobs, topic)
			m.mu.Unlock()
		}()

		if err := m.runJob(jobCtx, *cfg, tableName, offsets); err != nil && jobCtx.Err() == nil {
			m.setTopicError(topic, err)
			m.appendLog(topic, fmt.Sprintf("error: %v", err))
		} else {
			m.appendLog(topic, "sync stopped")
		}
	}()

	return nil
}

// rerunsOnResume reports whether cfg is a limited (duration or count) sync
// starting from "now", which is re-run from the topic end rather than resumed
// from the last cached offsets.
func rerunsOnResume(cfg database.CacheConfigRow) bool {
	if cfg.StartFrom == "beginning" || cfg.LimitValue == "" {
		return false
	}
	return cfg.LimitType == "duration" || cfg.LimitType == "count"
}

// Stop stops the background consumer for topic.
func (m *SyncManager) Stop(topic string) error {
	m.mu.Lock()
	job, exists := m.jobs[topic]
	m.mu.Unlock()

	if !exists {
		return nil
	}

	job.cancel()
	<-job.done
	return nil
}

// StopAll stops all currently active sync jobs.
func (m *SyncManager) StopAll() {
	m.mu.Lock()
	jobs := make([]*syncJob, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()

	for _, j := range jobs {
		j.cancel()
		<-j.done
	}
}

// IsActive returns whether topic has a running sync goroutine.
func (m *SyncManager) IsActive(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.jobs[topic]
	return exists
}

// Error returns the last error for topic, if any.
func (m *SyncManager) Error(topic string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.errors[topic]
}

// Logs returns recent log lines for topic.
func (m *SyncManager) Logs(topic string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.logs[topic]
	cp := make([]string, len(entries))
	copy(cp, entries)
	return cp
}

func (m *SyncManager) appendLog(topic string, line string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendLogLocked(topic, line)
}

func (m *SyncManager) appendLogLocked(topic string, line string) {
	entries := m.logs[topic]
	if len(entries) > 200 {
		entries = entries[1:]
	}
	formatted := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), line)
	m.logs[topic] = append(entries, formatted)
}

func (m *SyncManager) setTopicError(topic string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setTopicErrorLocked(topic, err)
}

func (m *SyncManager) setTopicErrorLocked(topic string, err error) {
	if err == nil {
		delete(m.errors, topic)
		return
	}
	m.errors[topic] = err.Error()
}

// TotalActive returns the count of running syncs and total estimated disk usage.
func (m *SyncManager) TotalActive(ctx context.Context) (int, int64) {
	if m.deps.DB == nil {
		return 0, 0
	}
	configs, err := database.ReadCacheConfigs(ctx, m.deps.DB)
	if err != nil {
		return 0, 0
	}

	m.mu.Lock()
	activeCount := 0
	m.mu.Unlock()

	var totalDisk int64
	for _, c := range configs {
		if m.IsActive(c.Topic) {
			activeCount++
		}
		tbl, _ := kafkaingest.TableName(c.Topic)
		_, disk, _ := database.GetTableStats(ctx, m.deps.DB, tbl)
		totalDisk += disk
	}
	return activeCount, totalDisk
}

func (m *SyncManager) runJob(ctx context.Context, cfg database.CacheConfigRow, table string, resumeOffsets map[int32]int64) error {
	writer, err := database.NewArrowTableWriter(m.deps.DB)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close() }()

	var limitDuration time.Duration
	var limitCount int64
	if cfg.LimitType == "duration" && cfg.LimitValue != "" {
		limitDuration, _ = timeutil.ParseDuration(cfg.LimitValue)
	} else if cfg.LimitType == "count" && cfg.LimitValue != "" {
		limitCount, _ = strconv.ParseInt(cfg.LimitValue, 10, 64)
	}

	var startTime time.Time
	if limitDuration > 0 {
		startTime = time.Now()
	}

	batches := make(chan kafkaingest.TopicBatch, 4)
	var runIngestor func(context.Context, <-chan kafka.Message)
	cutConf := cfg.CutConfluencePrefix

	if cfg.Format == "json" {
		ing := kafkaingest.NewJSONIngestor(cfg.Topic, 100, 500*time.Millisecond, batches)
		runIngestor = ing.Run
	} else {
		protoFile := cfg.ProtoFile
		messageName := cfg.MessageName

		// If ProtoFile is missing from config, look up in DuckDB kafka_topic_schemas
		if (protoFile == "" || messageName == "") && m.deps.DB != nil {
			if schemas, err := database.ReadTopicSchemas(context.Background(), m.deps.DB); err == nil {
				if sc, ok := schemas[cfg.Topic]; ok {
					if protoFile == "" {
						protoFile = sc.ProtoFile
					}
					if messageName == "" {
						messageName = sc.MessageName
					}
					cutConf = sc.CutConfluencePrefix
				}
			}
		}

		topicCfg := config.KafkaTopicConfig{
			Topic:               cfg.Topic,
			ProtoFile:           protoFile,
			MessageName:         messageName,
			ImportPaths:         m.protoImportPaths(),
			CutConfluencePrefix: cutConf,
		}
		ing, err := kafkaingest.NewIngestor([]config.KafkaTopicConfig{topicCfg}, 100, 500*time.Millisecond, batches)
		if err != nil {
			// If bufarrowlib failed due to schema incompatibility (cyclic types, empty structs with no children, etc.),
			// gracefully fallback to dynamic protobuf JSON serialization
			errStr := err.Error()
			if strings.Contains(errStr, "cyclic message type") ||
				strings.Contains(errStr, "with no children") ||
				strings.Contains(errStr, "cannot write struct type") {
				m.appendLog(cfg.Topic, fmt.Sprintf("incompatible protobuf Arrow schema (%v), falling back to JSON encoding", err))
				md, mdErr := kafkaingest.LoadProtoMessageDescriptor(protoFile, m.protoImportPaths(), messageName)
				if mdErr != nil {
					return fmt.Errorf("compile proto descriptor for fallback: %w", mdErr)
				}
				dynIng := kafkaingest.NewDynamicProtoJSONIngestor(cfg.Topic, md, 100, 500*time.Millisecond, batches)
				runIngestor = dynIng.Run
			} else {
				return err
			}
		} else {
			runIngestor = ing.Run
		}
	}

	// Sink writer
	sinkDone := make(chan struct{})
	go func() {
		defer close(sinkDone)
		kafkaingest.RunSink(ctx, batches, writer, map[string]string{cfg.Topic: table}, func(t string, writeErr error) {
			m.setTopicError(t, writeErr)
			m.appendLog(t, fmt.Sprintf("write error: %v", writeErr))
		})
	}()

	msgChan := make(chan kafka.Message, 1000)
	go func() {
		runIngestor(ctx, msgChan)
		close(batches)
	}()

	// Consumer options using adminOpts for seed & auth configuration (TLS/SASL)
	opts := kafka.AdminOpts(m.deps.KafkaConf)

	// Partition offset resolution
	starts, ends, err := kafka.PartitionBounds(ctx, m.deps.KafkaConf, cfg.Topic)
	if err != nil {
		close(msgChan)
		<-sinkDone
		return fmt.Errorf("get partition bounds: %w", err)
	}

	partOffsets := make(map[string]map[int32]kgo.Offset)
	partOffsets[cfg.Topic] = make(map[int32]kgo.Offset, len(ends))
	for p, end := range ends {
		if off, exists := resumeOffsets[p]; exists {
			partOffsets[cfg.Topic][p] = kgo.NewOffset().At(off + 1)
		} else if cfg.StartFrom == "beginning" {
			partOffsets[cfg.Topic][p] = kgo.NewOffset().At(starts[p])
		} else {
			partOffsets[cfg.Topic][p] = kgo.NewOffset().At(end)
		}
	}
	opts = append(opts, kgo.ConsumePartitions(partOffsets))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		close(msgChan)
		<-sinkDone
		return err
	}
	defer client.Close()

	m.appendLog(cfg.Topic, fmt.Sprintf("consumer connected (%d partitions), listening for messages", len(ends)))

	var munger func([]byte) []byte
	if cutConf {
		munger = kafka.WithMessageCutConfluencePrefix
	}

	// Track the last consumed offset per partition so a resume continues
	// after it (protobuf cache tables do not store offsets themselves).
	consumed := make(map[int32]int64)
	saveOffsets := func() {
		if len(consumed) == 0 {
			return
		}
		if err := database.SaveCacheOffsets(context.Background(), m.deps.DB, cfg.Topic, consumed); err != nil {
			m.appendLog(cfg.Topic, fmt.Sprintf("save offsets: %v", err))
		}
	}
	lastSave := time.Now()

	var totalConsumed int64
	for ctx.Err() == nil {
		if time.Since(lastSave) >= 5*time.Second {
			saveOffsets()
			lastSave = time.Now()
		}
		if limitDuration > 0 && time.Since(startTime) >= limitDuration {
			break
		}
		if limitCount > 0 && totalConsumed >= limitCount {
			break
		}

		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}

		var stop bool
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			val := r.Value
			if munger != nil {
				val = munger(val)
			}
			msg := kafka.Message{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     val,
				Timestamp: r.Timestamp,
			}
			select {
			case msgChan <- msg:
				consumed[r.Partition] = r.Offset
				totalConsumed++
				if limitCount > 0 && totalConsumed >= limitCount {
					stop = true
				}
			case <-ctx.Done():
				stop = true
			}
		})

		if stop {
			break
		}
	}

	close(msgChan)
	<-sinkDone
	saveOffsets()
	m.appendLog(cfg.Topic, fmt.Sprintf("consumed %d messages", totalConsumed))
	return nil
}

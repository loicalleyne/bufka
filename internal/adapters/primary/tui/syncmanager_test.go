package tui

import (
	"context"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
)

func TestSyncManagerLifecycleWithoutKafka(t *testing.T) {
	deps := Deps{
		KafkaConf: kafka.NewKafkaConfig(),
	}
	deps.KafkaConf.Seeds = []string{"127.0.0.1:9092"}

	mgr := NewSyncManager(deps)
	if mgr.IsActive("test-topic") {
		t.Fatal("expected topic not active")
	}

	active, disk := mgr.TotalActive(context.Background())
	if active != 0 || disk != 0 {
		t.Fatalf("expected 0 active and 0 disk, got %d, %d", active, disk)
	}

	mgr.StopAll()
}

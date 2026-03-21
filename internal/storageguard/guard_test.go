package storageguard

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

func TestEnforcePrunesLedgerWhenOverBudget(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{
		MachineID:        "test-machine",
		StatePath:        filepath.Join(dir, "state.db"),
		LedgerPath:       filepath.Join(dir, "ledger.db"),
		MaxStorageBytes:  200000,
		PruneTargetBytes: 150000,
	}

	ledgerStore, err := ledger.Open(cfg.LedgerPath)
	if err != nil {
		t.Fatalf("ledger.Open() error = %v", err)
	}
	defer ledgerStore.Close()

	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	defer stateStore.Close()

	const recordCount = 50
	for i := 0; i < recordCount; i++ {
		payload, _ := json.Marshal(map[string]any{"n": i, "blob": string(make([]byte, 4096))})
		_, err := ledgerStore.Append(schema.RawEnvelope{
			EnvelopeVersion:  1,
			SourceType:       "codex_local",
			SourceInstanceID: "source-1",
			ArtifactID:       "artifact-1",
			Cursor:           schema.Cursor{Kind: "line", Value: string(rune('a' + (i % 26)))},
			ObservedAt:       time.Now().UTC(),
			RawKind:          "message",
			RawPayload:       payload,
			ContentHash:      schema.HashBytes(payload),
		})
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := stateStore.SetNormalizationOffset("canonical_v1", recordCount); err != nil {
		t.Fatalf("SetNormalizationOffset() error = %v", err)
	}
	if err := stateStore.PutSinkCheckpoint(schema.SinkCheckpoint{
		SinkID:           "stdout",
		LastLedgerOffset: recordCount,
		LastEventID:      "evt-final",
		AckedAt:          time.Now().UTC(),
		DeliveryCount:    recordCount,
	}); err != nil {
		t.Fatalf("PutSinkCheckpoint() error = %v", err)
	}

	guard := New(cfg, ledgerStore, stateStore)
	report, err := guard.Enforce(context.Background(), recordCount)
	if !report.Enforced {
		t.Fatal("expected enforcement")
	}
	if report.PrunedRecords == 0 {
		t.Fatal("expected records to be pruned")
	}
	if err == nil {
		t.Log("Enforce satisfied the configured budget")
	} else {
		t.Logf("Enforce returned post-prune budget error: %v", err)
	}

	records, err := ledgerStore.ListAfter(0, 10)
	if err != nil {
		t.Fatalf("ListAfter() error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0 after prune", len(records))
	}
}

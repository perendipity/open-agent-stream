package delivery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestSelectReadyItemsWindowOverridesAge(t *testing.T) {
	t.Parallel()

	now := time.Unix(3605, 0).UTC()
	items := []state.DeliveryItem{
		{
			ID:           1,
			SinkID:       "remote",
			LedgerOffset: 1,
			EventCount:   1,
			PayloadBytes: 128,
			CreatedAt:    time.Unix(10, 0).UTC(),
		},
	}
	cfg := sinkapi.DeliveryConfig{
		MaxBatchEvents: 1,
		MaxBatchAge:    "1s",
		WindowEvery:    "1h",
	}

	if selected, ready := selectReadyItems(items, cfg, time.Unix(3599, 0).UTC()); ready || len(selected) != 0 {
		t.Fatalf("expected batch to wait for window boundary, got ready=%v selected=%d", ready, len(selected))
	}
	if selected, ready := selectReadyItems(items, cfg, now); !ready || len(selected) != 1 {
		t.Fatalf("expected batch to be ready once window opened, got ready=%v selected=%d", ready, len(selected))
	}
}

func TestDispatchDueMarksBlockedBatchesWithoutQuarantine(t *testing.T) {
	t.Parallel()

	manager, store := testDeliveryManager(t, &testSink{
		id:  "remote",
		typ: "http",
		err: NewBlockedError(BlockedKindAuth, "auth_expired", "env", "abcd1234", errors.New("env:abcd1234: authentication has expired")),
	})
	defer store.Close()

	if err := manager.PrepareBatch(context.Background(), sinkapi.Batch{
		Events: []schema.CanonicalEvent{{EventID: "evt-1"}},
	}, 1); err != nil {
		t.Fatal(err)
	}

	delivered, err := manager.DispatchDue(context.Background())
	if err != nil {
		t.Fatalf("DispatchDue() error = %v", err)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d, want 0", delivered)
	}

	blocked, err := store.ListDeliveryBatches("remote", []string{state.DeliveryBatchStatusBlocked}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(blocked), 1; got != want {
		t.Fatalf("len(blocked) = %d, want %d", got, want)
	}

	summary, err := store.DeliverySummary("remote", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := summary.BlockedBatchCount, 1; got != want {
		t.Fatalf("BlockedBatchCount = %d, want %d", got, want)
	}
	if got, want := summary.QuarantinedBatchCount, 0; got != want {
		t.Fatalf("QuarantinedBatchCount = %d, want %d", got, want)
	}
	if summary.TerminalContiguousOffset != 0 {
		t.Fatalf("TerminalContiguousOffset = %d, want 0", summary.TerminalContiguousOffset)
	}
}

func TestDispatchDueQuarantinesPermanentFailures(t *testing.T) {
	t.Parallel()

	manager, store := testDeliveryManager(t, &testSink{
		id:  "remote",
		typ: "http",
		err: NewPermanentError(errors.New("bad request")),
	})
	defer store.Close()

	if err := manager.PrepareBatch(context.Background(), sinkapi.Batch{
		Events: []schema.CanonicalEvent{{EventID: "evt-1"}},
	}, 1); err != nil {
		t.Fatal(err)
	}

	delivered, err := manager.DispatchDue(context.Background())
	if err != nil {
		t.Fatalf("DispatchDue() error = %v", err)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d, want 0", delivered)
	}

	quarantined, err := store.ListDeliveryBatches("remote", []string{state.DeliveryBatchStatusQuarantined}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(quarantined), 1; got != want {
		t.Fatalf("len(quarantined) = %d, want %d", got, want)
	}

	summary, err := store.DeliverySummary("remote", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := summary.QuarantinedBatchCount, 1; got != want {
		t.Fatalf("QuarantinedBatchCount = %d, want %d", got, want)
	}
	if summary.TerminalContiguousOffset != 1 {
		t.Fatalf("TerminalContiguousOffset = %d, want 1", summary.TerminalContiguousOffset)
	}
}

type testSink struct {
	id  string
	typ string
	err error
}

func (s *testSink) ID() string   { return s.id }
func (s *testSink) Type() string { return s.typ }
func (s *testSink) Init(context.Context) error {
	return nil
}
func (s *testSink) SendBatch(context.Context, sinkapi.Batch) (sinkapi.Result, error) {
	return sinkapi.Result{}, s.err
}
func (s *testSink) Flush(context.Context) error  { return nil }
func (s *testSink) Health(context.Context) error { return nil }
func (s *testSink) Close(context.Context) error  { return nil }

func testDeliveryManager(t *testing.T, sink sinkapi.Sink) (*Manager, *state.Store) {
	t.Helper()

	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Sinks: []sinkapi.Config{{
			ID:   sink.ID(),
			Type: sink.Type(),
			Delivery: sinkapi.DeliveryConfig{
				MaxBatchEvents:      1,
				RetryInitialBackoff: "1s",
				RetryMaxBackoff:     "1s",
				PoisonAfterFailures: 5,
			},
		}},
	}
	manager := New(cfg, []sinkapi.Sink{sink}, store, redact.NewEngine(cfg))
	if err := manager.Init(); err != nil {
		store.Close()
		t.Fatal(err)
	}
	return manager, store
}

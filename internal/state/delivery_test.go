package state

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestDeliveryProgressTracksAckedAndTerminalOffsetsSeparately(t *testing.T) {
	t.Parallel()

	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch1 := sinkapi.Batch{Events: []schema.CanonicalEvent{{EventID: "evt-1"}}}
	batch3 := sinkapi.Batch{Events: []schema.CanonicalEvent{{EventID: "evt-3"}}}

	if err := store.EnqueueDeliveryItem("remote", 1, batch1, DeliveryItemStatusPending, now); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueDeliveryItem("remote", 2, sinkapi.Batch{}, DeliveryItemStatusSkipped, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	items, err := store.ListPendingDeliveryItems("remote", 10)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := json.Marshal(map[string]any{"payload": "sealed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SealDeliveryBatch(DeliveryBatch{
		BatchID:         "batch-1",
		SinkID:          "remote",
		PreparedJSON:    prepared,
		PayloadBytes:    len(prepared),
		LedgerMinOffset: 1,
		LedgerMaxOffset: 1,
		EventCount:      1,
		Status:          DeliveryBatchStatusPending,
		NextAttemptAt:   now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, []int64{items[0].ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeliveryBatchSucceeded("batch-1", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	summary, err := store.DeliverySummary("remote", now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if summary.AckedContiguousOffset != 2 || summary.TerminalContiguousOffset != 2 || summary.GapCount != 0 {
		t.Fatalf("after success: acked=%d terminal=%d gaps=%d", summary.AckedContiguousOffset, summary.TerminalContiguousOffset, summary.GapCount)
	}

	if err := store.EnqueueDeliveryItem("remote", 3, batch3, DeliveryItemStatusPending, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	items, err = store.ListPendingDeliveryItems("remote", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SealDeliveryBatch(DeliveryBatch{
		BatchID:         "batch-2",
		SinkID:          "remote",
		PreparedJSON:    prepared,
		PayloadBytes:    len(prepared),
		LedgerMinOffset: 3,
		LedgerMaxOffset: 3,
		EventCount:      1,
		Status:          DeliveryBatchStatusPending,
		NextAttemptAt:   now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, []int64{items[0].ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.QuarantineDeliveryBatch("batch-2", "permanent failure", now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}

	summary, err = store.DeliverySummary("remote", now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if summary.AckedContiguousOffset != 2 || summary.TerminalContiguousOffset != 3 || summary.GapCount != 1 {
		t.Fatalf("after quarantine: acked=%d terminal=%d gaps=%d", summary.AckedContiguousOffset, summary.TerminalContiguousOffset, summary.GapCount)
	}
	if summary.LastTerminalError == "" {
		t.Fatal("expected last terminal error to be recorded")
	}
}

func TestRetryDeliveryBatchRequeuesQuarantinedBatch(t *testing.T) {
	t.Parallel()

	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	if err := store.EnqueueDeliveryItem("remote", 1, sinkapi.Batch{Events: []schema.CanonicalEvent{{EventID: "evt-1"}}}, DeliveryItemStatusPending, now); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListPendingDeliveryItems("remote", 10)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := json.Marshal(map[string]any{"payload": "sealed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SealDeliveryBatch(DeliveryBatch{
		BatchID:         "batch-1",
		SinkID:          "remote",
		PreparedJSON:    prepared,
		PayloadBytes:    len(prepared),
		LedgerMinOffset: 1,
		LedgerMaxOffset: 1,
		EventCount:      1,
		Status:          DeliveryBatchStatusPending,
		NextAttemptAt:   now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, []int64{items[0].ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.QuarantineDeliveryBatch("batch-1", "permanent failure", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	batches, err := store.ListDeliveryBatches("remote", []string{DeliveryBatchStatusQuarantined}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(batches), 1; got != want {
		t.Fatalf("len(batches)=%d, want %d", got, want)
	}

	if err := store.RetryDeliveryBatch("batch-1", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	retried, err := store.GetDeliveryBatch("batch-1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := retried.Status, DeliveryBatchStatusRetrying; got != want {
		t.Fatalf("status=%q, want %q", got, want)
	}
	if retried.LastError != "" {
		t.Fatalf("last_error=%q, want cleared", retried.LastError)
	}
}

func TestBlockedDeliveryBatchTracksBlockedStateWithoutAdvancingTerminalOffset(t *testing.T) {
	t.Parallel()

	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	if err := store.EnqueueDeliveryItem("remote", 1, sinkapi.Batch{Events: []schema.CanonicalEvent{{EventID: "evt-1"}}}, DeliveryItemStatusPending, now); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListPendingDeliveryItems("remote", 10)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := json.Marshal(map[string]any{"payload": "sealed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SealDeliveryBatch(DeliveryBatch{
		BatchID:         "batch-1",
		SinkID:          "remote",
		PreparedJSON:    prepared,
		PayloadBytes:    len(prepared),
		LedgerMinOffset: 1,
		LedgerMaxOffset: 1,
		EventCount:      1,
		Status:          DeliveryBatchStatusPending,
		NextAttemptAt:   now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, []int64{items[0].ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeliveryBatchBlocked("batch-1", "auth", "auth_expired", "env", "env:abcd: authentication has expired", now.Add(time.Second), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	summary, err := store.DeliverySummary("remote", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := summary.BlockedBatchCount, 1; got != want {
		t.Fatalf("BlockedBatchCount = %d, want %d", got, want)
	}
	if got, want := summary.BlockedKind, "auth"; got != want {
		t.Fatalf("BlockedKind = %q, want %q", got, want)
	}
	if got, want := summary.AuthFailureAttempts, 1; got != want {
		t.Fatalf("AuthFailureAttempts = %d, want %d", got, want)
	}
	if got, want := summary.QuarantinedBatchCount, 0; got != want {
		t.Fatalf("QuarantinedBatchCount = %d, want %d", got, want)
	}
	if summary.AckedContiguousOffset != 0 || summary.TerminalContiguousOffset != 0 {
		t.Fatalf("blocked batch advanced offsets: acked=%d terminal=%d", summary.AckedContiguousOffset, summary.TerminalContiguousOffset)
	}
	if got, want := len(summary.SecretProviderStats), 1; got != want {
		t.Fatalf("len(SecretProviderStats) = %d, want %d", got, want)
	}
	if got, want := summary.SecretProviderStats[0].Provider, "env"; got != want {
		t.Fatalf("provider = %q, want %q", got, want)
	}
	if got, want := summary.SecretProviderStats[0].AuthFailures, 1; got != want {
		t.Fatalf("auth failures = %d, want %d", got, want)
	}

	if err := store.MarkDeliveryBatchSucceeded("batch-1", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	summary, err = store.DeliverySummary("remote", now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := summary.BlockedBatchCount, 0; got != want {
		t.Fatalf("BlockedBatchCount after success = %d, want %d", got, want)
	}
	if summary.BlockedKind != "" {
		t.Fatalf("BlockedKind after success = %q, want empty", summary.BlockedKind)
	}
	if summary.BlockedSince.IsZero() == false {
		t.Fatalf("BlockedSince after success = %v, want zero", summary.BlockedSince)
	}
	if summary.AckedContiguousOffset != 1 || summary.TerminalContiguousOffset != 1 {
		t.Fatalf("after success: acked=%d terminal=%d, want 1/1", summary.AckedContiguousOffset, summary.TerminalContiguousOffset)
	}
}

func TestRecordSecretResolutionsUpdatesSummaryAndProviderStats(t *testing.T) {
	t.Parallel()

	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	if err := store.RecordSecretResolutions("remote", []sinkapi.AuthMetric{
		{Provider: "env", ResolvedSecretCount: 2},
		{Provider: "op", ResolvedSecretCount: 1},
	}, now); err != nil {
		t.Fatal(err)
	}

	summary, err := store.DeliverySummary("remote", now)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := summary.SuccessfulSecretResolutions, 3; got != want {
		t.Fatalf("SuccessfulSecretResolutions = %d, want %d", got, want)
	}
	if got, want := len(summary.SecretProviderStats), 2; got != want {
		t.Fatalf("len(SecretProviderStats) = %d, want %d", got, want)
	}
	for _, stat := range summary.SecretProviderStats {
		switch stat.Provider {
		case "env":
			if got, want := stat.SuccessfulResolutions, 2; got != want {
				t.Fatalf("env successful_resolutions = %d, want %d", got, want)
			}
		case "op":
			if got, want := stat.SuccessfulResolutions, 1; got != want {
				t.Fatalf("op successful_resolutions = %d, want %d", got, want)
			}
		default:
			t.Fatalf("unexpected provider stat: %+v", stat)
		}
	}
}

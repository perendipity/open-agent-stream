package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
)

func TestParseDeliveryStatuses(t *testing.T) {
	t.Parallel()

	got, err := parseDeliveryStatuses("retrying,quarantined,pending")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		state.DeliveryBatchStatusPending,
		state.DeliveryBatchStatusQuarantined,
		state.DeliveryBatchStatusRetrying,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses=%v, want %v", got, want)
	}
}

func TestBuildDeliveryBatchViewIncludesPreparedMetadata(t *testing.T) {
	t.Parallel()

	prepared, err := json.Marshal(delivery.PreparedDispatch{
		SinkID:          "remote-http",
		BatchID:         "batch-1",
		PayloadSHA256:   "sha256:abc",
		ContentType:     "application/json",
		Headers:         map[string]string{"Content-Type": "application/json"},
		PayloadFormat:   "oas_batch_json",
		LedgerMinOffset: 1,
		LedgerMaxOffset: 2,
		EventCount:      2,
		CreatedAt:       time.Date(2026, 3, 25, 22, 0, 0, 0, time.UTC),
		Destination:     map[string]any{"url": "https://example.com/ingest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := buildDeliveryBatchView(state.DeliveryBatch{
		BatchID:         "batch-1",
		SinkID:          "remote-http",
		PreparedJSON:    prepared,
		PayloadBytes:    123,
		LedgerMinOffset: 1,
		LedgerMaxOffset: 2,
		EventCount:      2,
		Status:          state.DeliveryBatchStatusQuarantined,
		AttemptCount:    3,
		LastError:       "boom",
		CreatedAt:       time.Date(2026, 3, 25, 22, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 3, 25, 22, 1, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := view.Prepared.PayloadSHA256, "sha256:abc"; got != want {
		t.Fatalf("payload_sha256=%q, want %q", got, want)
	}
	if got, want := view.Destination["url"], "https://example.com/ingest"; got != want {
		t.Fatalf("destination url=%v, want %q", got, want)
	}
}

func TestWriteDeliveryInspectReportIncludesSealedMetadata(t *testing.T) {
	t.Parallel()

	view := deliveryBatchView{
		BatchID:         "batch-1",
		SinkID:          "remote-http",
		Status:          state.DeliveryBatchStatusQuarantined,
		AttemptCount:    3,
		LedgerMinOffset: 1,
		LedgerMaxOffset: 2,
		EventCount:      2,
		PayloadBytes:    123,
		LastError:       "boom",
		Prepared: preparedView{
			PayloadSHA256: "sha256:abc",
			ContentType:   "application/json",
			PayloadFormat: "oas_batch_json",
			Headers:       map[string]string{"Content-Type": "application/json"},
		},
		Destination: map[string]any{"url": "https://example.com/ingest"},
	}
	var out bytes.Buffer
	if err := writeDeliveryInspectReport(&out, view); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"Batch:", "Prepared payload:", "Payload sha256", "Headers:", "Destination:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
	}
}

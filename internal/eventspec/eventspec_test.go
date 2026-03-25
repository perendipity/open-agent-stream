package eventspec

import (
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestBatchTransformsCanonicalEventsToV2(t *testing.T) {
	t.Parallel()

	original := sinkapi.Batch{
		Events: []schema.CanonicalEvent{{
			EventVersion:     schema.CanonicalEventVersionV1,
			SourceType:       "codex_local",
			SourceInstanceID: "shared-instance",
			SessionKey:       schema.StableSessionKey("shared-instance", "session-1"),
			Sequence:         1,
			Timestamp:        time.Date(2026, 3, 25, 20, 0, 0, 0, time.UTC),
			Kind:             "message.user",
			Context: schema.EventContext{
				SourceType:       "codex_local",
				SourceSessionKey: "session-1",
			},
			Payload: map[string]any{"text": "hello"},
			RawRef:  schema.RawRef{LedgerOffset: 1, EnvelopeID: "env-1"},
		}},
		RawEnvelopes: []schema.RawEnvelope{{
			EnvelopeID: "env-1",
			MachineID:  "machine-a",
		}},
	}
	original.Events[0] = schema.EnsureEventID(original.Events[0])

	transformed, err := Batch(original, schema.EventSpecV2)
	if err != nil {
		t.Fatal(err)
	}

	event := transformed.Events[0]
	if got, want := event.EventVersion, schema.CanonicalEventVersionV2; got != want {
		t.Fatalf("event_version=%d, want %d", got, want)
	}
	if got, want := event.MachineID, "machine-a"; got != want {
		t.Fatalf("machine_id=%q, want %q", got, want)
	}
	wantSessionKey := schema.StableSessionKeyForVersion(schema.EventSpecV2, "machine-a", "shared-instance", "session-1")
	if got := event.SessionKey; got != wantSessionKey {
		t.Fatalf("session_key=%q, want %q", got, wantSessionKey)
	}
	if event.EventID == original.Events[0].EventID {
		t.Fatal("expected v2 event id to change when machine identity is added")
	}
}

func TestBatchTransformsCanonicalEventsBackToV1(t *testing.T) {
	t.Parallel()

	batch := sinkapi.Batch{
		Events: []schema.CanonicalEvent{{
			EventVersion:     schema.CanonicalEventVersionV2,
			SourceType:       "codex_local",
			SourceInstanceID: "shared-instance",
			MachineID:        "machine-a",
			SessionKey:       schema.StableSessionKeyForVersion(schema.EventSpecV2, "machine-a", "shared-instance", "session-1"),
			Sequence:         1,
			Timestamp:        time.Date(2026, 3, 25, 20, 0, 0, 0, time.UTC),
			Kind:             "message.user",
			Context: schema.EventContext{
				SourceType:       "codex_local",
				SourceSessionKey: "session-1",
			},
			Payload: map[string]any{"text": "hello"},
			RawRef:  schema.RawRef{LedgerOffset: 1, EnvelopeID: "env-1"},
		}},
	}
	batch.Events[0] = schema.EnsureEventID(batch.Events[0])

	transformed, err := Batch(batch, schema.EventSpecV1)
	if err != nil {
		t.Fatal(err)
	}

	event := transformed.Events[0]
	if got, want := event.EventVersion, schema.CanonicalEventVersionV1; got != want {
		t.Fatalf("event_version=%d, want %d", got, want)
	}
	if event.MachineID != "" {
		t.Fatalf("machine_id=%q, want empty", event.MachineID)
	}
	wantSessionKey := schema.StableSessionKey("shared-instance", "session-1")
	if got := event.SessionKey; got != wantSessionKey {
		t.Fatalf("session_key=%q, want %q", got, wantSessionKey)
	}
}

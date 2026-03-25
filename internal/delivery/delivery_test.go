package delivery

import (
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/state"
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

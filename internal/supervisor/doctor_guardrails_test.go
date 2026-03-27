package supervisor

import (
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestSharedBootstrapChecksWarnOnFreshSharedState(t *testing.T) {
	cfg := config.Config{
		Sinks: []sinkapi.Config{{
			ID:   "shared-command",
			Type: "command",
		}},
	}

	checks := sharedBootstrapChecks(cfg, ledger.Bounds{}, map[string]delivery.SinkStatus{
		"shared-command": {Summary: state.DeliverySummary{}},
	})
	if len(checks) != 1 {
		t.Fatalf("len(checks) = %d, want 1", len(checks))
	}
	if got, want := checks[0].Status, "warn"; got != want {
		t.Fatalf("checks[0].Status = %q, want %q", got, want)
	}
	if !strings.Contains(checks[0].Detail, "first run will bootstrap from the current source roots") {
		t.Fatalf("checks[0].Detail = %q, want bootstrap warning", checks[0].Detail)
	}
}

func TestSharedBootstrapChecksSkipAfterSuccessfulDelivery(t *testing.T) {
	cfg := config.Config{
		Sinks: []sinkapi.Config{{
			ID:   "shared-command",
			Type: "command",
		}},
	}

	checks := sharedBootstrapChecks(cfg, ledger.Bounds{
		MinOffset:  1,
		MaxOffset:  64,
		HasRecords: true,
	}, map[string]delivery.SinkStatus{
		"shared-command": {Summary: state.DeliverySummary{SuccessfulBatches: 1}},
	})
	if len(checks) != 0 {
		t.Fatalf("len(checks) = %d, want 0", len(checks))
	}
}

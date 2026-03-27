package supervisor

import (
	"fmt"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/health"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkmeta"
)

func sharedBootstrapChecks(cfg config.Config, bounds ledger.Bounds, deliveryStatus map[string]delivery.SinkStatus) []health.Check {
	var checks []health.Check
	for _, sinkCfg := range cfg.Sinks {
		if !sinkmeta.IsSharedDestinationType(sinkCfg.Type) {
			continue
		}
		summary, ok := deliveryStatus[sinkCfg.ID]
		if !ok {
			continue
		}
		if summary.Summary.SuccessfulBatches > 0 || summary.Summary.AckedContiguousOffset > 0 {
			continue
		}

		detail := "fresh local state: no ledger records or acknowledged deliveries yet. The first run will bootstrap from the current source roots and may redeliver historical data. If this machine already shipped data, restore the original state directory or start with a smaller recent subtree first"
		if bounds.HasRecords {
			recordCount := bounds.MaxOffset - bounds.MinOffset + 1
			detail = fmt.Sprintf("fresh local state: ledger currently holds %d record(s) but sink has no acknowledged deliveries yet. If this machine already shipped data, verify you restored the original state+ledger pair before continuing", recordCount)
		}
		checks = append(checks, health.Check{
			Name:   "shared-bootstrap:" + sinkCfg.ID,
			Status: "warn",
			Detail: detail,
		})
	}
	return checks
}

package storageguard

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
)

type Guard struct {
	cfg    config.Config
	ledger *ledger.Store
	state  *state.Store
}

type Report struct {
	UsageBytes        int64
	FreeBytes         uint64
	PrunedRecords     int64
	Enforced          bool
	NeedsEnforcement  bool
	Reason            string
	ManagedPaths      []string
	SafePruneOffset   int64
	MaxStorageBytes   int64
	PruneTargetBytes  int64
	DesiredUsageBytes int64
	MinFreeBytes      int64
}

func New(cfg config.Config, ledgerStore *ledger.Store, stateStore *state.Store) *Guard {
	return &Guard{cfg: cfg, ledger: ledgerStore, state: stateStore}
}

func (g *Guard) Inspect() (Report, error) {
	report := Report{}
	if g == nil {
		return report, nil
	}
	managed := g.managedPaths()
	sort.Strings(managed)
	report.ManagedPaths = append(report.ManagedPaths, managed...)
	usage, err := totalSize(managed)
	if err != nil {
		return report, err
	}
	report.UsageBytes = usage

	free, err := freeBytes(filepath.Dir(g.cfg.StatePath))
	if err == nil {
		report.FreeBytes = free
	}

	maxBytes, _ := g.cfg.MaxStorageBytesValue()
	pruneTarget, _ := g.cfg.PruneTargetBytesValue()
	minFree, _ := g.cfg.MinFreeBytesValue()
	desiredUsage := maxBytes
	if pruneTarget > 0 {
		desiredUsage = pruneTarget
	}
	report.MaxStorageBytes = maxBytes
	report.PruneTargetBytes = pruneTarget
	report.DesiredUsageBytes = desiredUsage
	report.MinFreeBytes = minFree

	var reasons []string
	if maxBytes > 0 && usage > maxBytes {
		report.NeedsEnforcement = true
		reasons = append(reasons, fmt.Sprintf("usage %d > max %d", usage, maxBytes))
	}
	if minFree > 0 && report.FreeBytes > 0 && report.FreeBytes < uint64(minFree) {
		report.NeedsEnforcement = true
		reasons = append(reasons, fmt.Sprintf("free %d < min %d", report.FreeBytes, minFree))
	}
	report.Reason = strings.Join(reasons, "; ")
	return report, nil
}

func (g *Guard) Enforce(_ context.Context, normalizationOffset int64) (Report, error) {
	report, err := g.Inspect()
	if err != nil {
		return report, err
	}
	if !report.NeedsEnforcement {
		return report, nil
	}

	report.Enforced = true

	minSinkOffset, ok, err := g.state.MinimumSinkTerminalOffset()
	if err != nil {
		return report, err
	}
	if !ok {
		return report, fmt.Errorf("storage budget exceeded but no sink terminal offsets exist yet; cannot prune safely")
	}

	safeOffset := normalizationOffset
	if minSinkOffset < safeOffset {
		safeOffset = minSinkOffset
	}
	if safeOffset <= 0 {
		return report, fmt.Errorf("storage budget exceeded but safe prune offset is %d", safeOffset)
	}
	report.SafePruneOffset = safeOffset

	pruned, err := g.ledger.DeleteThrough(safeOffset)
	if err != nil {
		return report, err
	}
	report.PrunedRecords = pruned
	if err := g.ledger.Compact(); err != nil {
		return report, err
	}
	if err := g.state.Compact(); err != nil {
		return report, err
	}

	usage, err := totalSize(report.ManagedPaths)
	if err == nil {
		report.UsageBytes = usage
	}
	if free, err := freeBytes(filepath.Dir(g.cfg.StatePath)); err == nil {
		report.FreeBytes = free
	}
	if report.DesiredUsageBytes > 0 && report.UsageBytes > report.DesiredUsageBytes {
		return report, fmt.Errorf("storage usage remains above target after pruning: usage=%d target=%d max=%d pruned_records=%d", report.UsageBytes, report.DesiredUsageBytes, report.MaxStorageBytes, report.PrunedRecords)
	}
	if report.MinFreeBytes > 0 && report.FreeBytes > 0 && report.FreeBytes < uint64(report.MinFreeBytes) {
		return report, fmt.Errorf("free bytes remain below minimum after pruning: free=%d min=%d pruned_records=%d", report.FreeBytes, report.MinFreeBytes, report.PrunedRecords)
	}
	return report, nil
}

func (g *Guard) managedPaths() []string {
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		for _, candidate := range sidecarPaths(path) {
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
		}
	}

	add(g.cfg.StatePath)
	add(g.cfg.LedgerPath)
	for _, sink := range g.cfg.Sinks {
		if p := strings.TrimSpace(sink.Options["path"]); p != "" {
			add(p)
		}
	}

	dir := filepath.Dir(g.cfg.StatePath)
	entries, err := os.ReadDir(dir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasPrefix(name, "oas-daemon-") && (strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".pid")) {
				add(filepath.Join(dir, name))
			}
		}
	}

	out := make([]string, 0, len(seen))
	for path := range seen {
		out = append(out, path)
	}
	return out
}

func sidecarPaths(path string) []string {
	return []string{path, path + "-wal", path + "-shm"}
}

func totalSize(paths []string) (int64, error) {
	var total int64
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		total += fileOrDirSize(path, info)
	}
	return total, nil
}

func fileOrDirSize(path string, info fs.FileInfo) int64 {
	if !info.IsDir() {
		return info.Size()
	}
	var total int64
	_ = filepath.Walk(path, func(_ string, walkInfo os.FileInfo, err error) error {
		if err != nil || walkInfo == nil || walkInfo.IsDir() {
			return nil
		}
		total += walkInfo.Size()
		return nil
	})
	return total
}

func freeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/sinkmeta"
)

func Warnings(cfg Config) []string {
	warnings := append([]string(nil), MachineIDWarnings(cfg.MachineID)...)
	warnings = append(warnings, sharedDestinationBootstrapWarnings(cfg)...)
	return warnings
}

func sourceRootOverlapErrors(cfg Config) []error {
	type sourceRoot struct {
		index      int
		instanceID string
		root       string
		resolved   string
	}

	roots := make([]sourceRoot, 0, len(cfg.Sources))
	for idx, source := range cfg.Sources {
		root := strings.TrimSpace(source.Root)
		if root == "" {
			continue
		}
		roots = append(roots, sourceRoot{
			index:      idx,
			instanceID: source.InstanceID,
			root:       root,
			resolved:   comparablePath(root),
		})
	}

	var errs []error
	for i := 0; i < len(roots); i++ {
		for j := i + 1; j < len(roots); j++ {
			left := roots[i]
			right := roots[j]
			if !pathsOverlap(left.resolved, right.resolved) {
				continue
			}
			errs = append(errs, fmt.Errorf(
				"sources[%d].root: overlaps sources[%d].root (%s); overlapping source roots can ingest the same files twice (%s=%s, %s=%s)",
				left.index,
				right.index,
				right.root,
				left.instanceID,
				left.root,
				right.instanceID,
				right.root,
			))
		}
	}
	return errs
}

func sharedDestinationBootstrapWarnings(cfg Config) []string {
	sinkIDs := sharedDestinationSinkIDs(cfg)
	if len(sinkIDs) == 0 {
		return nil
	}

	stateExists, stateErr := pathExists(cfg.StatePath)
	ledgerExists, ledgerErr := pathExists(cfg.LedgerPath)

	var warnings []string
	if stateErr != nil {
		warnings = append(warnings, fmt.Sprintf("inspect state_path for shared destination safety: %v", stateErr))
	}
	if ledgerErr != nil {
		warnings = append(warnings, fmt.Sprintf("inspect ledger_path for shared destination safety: %v", ledgerErr))
	}
	if stateErr != nil || ledgerErr != nil {
		return warnings
	}

	label := strings.Join(sinkIDs, ", ")
	switch {
	case !stateExists && !ledgerExists:
		warnings = append(warnings, fmt.Sprintf(
			"shared sink(s) %s are configured but neither state_path nor ledger_path exists yet; the first run will bootstrap from the current source roots and may redeliver historical data. If this machine already shipped data, restore the original state directory or start with a smaller recent subtree first",
			label,
		))
	case stateExists && !ledgerExists:
		warnings = append(warnings, fmt.Sprintf(
			"shared sink(s) %s are configured but ledger_path is missing while state_path exists; restore or delete both local files together before you run OAS to avoid split-state replay and accidental re-bootstrap",
			label,
		))
	case !stateExists && ledgerExists:
		warnings = append(warnings, fmt.Sprintf(
			"shared sink(s) %s are configured but state_path is missing while ledger_path exists; restore or delete both local files together before you run OAS to avoid split-state replay and accidental re-bootstrap",
			label,
		))
	}
	return warnings
}

func sharedDestinationSinkIDs(cfg Config) []string {
	var ids []string
	for _, sink := range cfg.Sinks {
		if sinkmeta.IsSharedDestinationType(sink.Type) {
			ids = append(ids, sink.ID)
		}
	}
	return ids
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(strings.TrimSpace(path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func comparablePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(abs)
}

func pathsOverlap(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	return pathContains(left, right) || pathContains(right, left)
}

func pathContains(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	prefix := ".." + string(filepath.Separator)
	return rel != ".." && !strings.HasPrefix(rel, prefix)
}

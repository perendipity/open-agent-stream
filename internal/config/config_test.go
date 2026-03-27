package config

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestApplyDefaultsSetsContinuousRunDefaults(t *testing.T) {
	cfg := Config{}
	applyDefaults(&cfg, nil)

	if cfg.PollInterval != (3 * time.Second).String() {
		t.Fatalf("PollInterval = %q, want %q", cfg.PollInterval, (3 * time.Second).String())
	}
	if cfg.ErrorBackoff != (10 * time.Second).String() {
		t.Fatalf("ErrorBackoff = %q, want %q", cfg.ErrorBackoff, (10 * time.Second).String())
	}
	if cfg.MaxConsecutiveErrors != 10 {
		t.Fatalf("MaxConsecutiveErrors = %d, want 10", cfg.MaxConsecutiveErrors)
	}
	if cfg.DataDir == "" {
		t.Fatal("DataDir should be set")
	}
	if cfg.StatePath == "" || cfg.LedgerPath == "" {
		t.Fatal("StatePath and LedgerPath should be set")
	}
}

func TestApplyDefaultsDoesNotOverrideExplicitMaxConsecutiveErrors(t *testing.T) {
	cfg := Config{MaxConsecutiveErrors: 0}
	applyDefaults(&cfg, map[string]json.RawMessage{
		"max_consecutive_errors": []byte("0"),
	})
	if cfg.MaxConsecutiveErrors != 0 {
		t.Fatalf("MaxConsecutiveErrors = %d, want explicit 0 preserved for validation", cfg.MaxConsecutiveErrors)
	}
}

func TestValidateRejectsNonPositiveMaxConsecutiveErrors(t *testing.T) {
	cfg := Config{
		Version:              "0.1",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 0,
		Sources: []sourceapi.Config{{
			InstanceID: "source-1",
			Type:       "codex_local",
			Root:       "/tmp/source",
		}},
		Sinks: []sinkapi.Config{{
			ID:   "stdout",
			Type: "stdout",
		}},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to fail")
	}
	if !strings.Contains(err.Error(), "max_consecutive_errors") {
		t.Fatalf("Validate error = %v, want mention of max_consecutive_errors", err)
	}
}

func TestValidateRejectsPruneTargetNotBelowMaxStorage(t *testing.T) {
	cfg := Config{
		Version:              "0.1",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		MaxStorageBytes:      100,
		PruneTargetBytes:     100,
		Sources: []sourceapi.Config{{
			InstanceID: "source-1",
			Type:       "codex_local",
			Root:       "/tmp/source",
		}},
		Sinks: []sinkapi.Config{{
			ID:   "stdout",
			Type: "stdout",
		}},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to fail")
	}
	if !strings.Contains(err.Error(), "prune_target_bytes") {
		t.Fatalf("Validate error = %v, want mention of prune_target_bytes", err)
	}
}

func TestValidateUsesIndexedFieldPathsForNestedConfigErrors(t *testing.T) {
	cfg := Config{
		Version:              "0.1",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		Sources: []sourceapi.Config{{
			Type: "codex_local",
		}},
		Sinks: []sinkapi.Config{{
			Type: "stdout",
		}},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to fail")
	}
	for _, want := range []string{
		"sources[0].instance_id",
		"sources[0].root",
		"sinks[0].id",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate error = %v, want mention of %q", err, want)
		}
	}
}

func TestValidateRejectsInvalidSinkAuthConfigWithIndexedPath(t *testing.T) {
	cfg := Config{
		Version:              "0.1",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		Sources: []sourceapi.Config{{
			InstanceID: "source-1",
			Type:       "codex_local",
			Root:       "/tmp/source",
		}},
		Sinks: []sinkapi.Config{{
			ID:   "remote",
			Type: "http",
			Settings: map[string]any{
				"bearer_token_ref": "not-a-ref",
			},
		}},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to fail")
	}
	for _, want := range []string{"sinks[0]", "bearer_token_ref"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate error = %v, want mention of %q", err, want)
		}
	}
}

func TestValidateRejectsOverlappingSourceRoots(t *testing.T) {
	cfg := Config{
		Version:              "0.1",
		MachineID:            "machine-123",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		Sources: []sourceapi.Config{
			{
				InstanceID: "claude-root",
				Type:       "claude_local",
				Root:       "/tmp/claude/projects",
			},
			{
				InstanceID: "claude-kairanora",
				Type:       "claude_local",
				Root:       "/tmp/claude/projects/kairanora",
			},
		},
		Sinks: []sinkapi.Config{{
			ID:   "stdout",
			Type: "stdout",
		}},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected Validate to fail")
	}
	for _, want := range []string{
		"sources[0].root",
		"sources[1].root",
		"overlapping source roots can ingest the same files twice",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate error = %v, want mention of %q", err, want)
		}
	}
}

func TestWarningsIncludeSharedDestinationBootstrapRiskWhenStateIsMissing(t *testing.T) {
	base := t.TempDir()
	cfg := Config{
		Version:              "0.1",
		MachineID:            "machine-123",
		StatePath:            filepath.Join(base, "state.db"),
		LedgerPath:           filepath.Join(base, "ledger.db"),
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		Sources: []sourceapi.Config{{
			InstanceID: "codex-local",
			Type:       "codex_local",
			Root:       "/tmp/codex",
		}},
		Sinks: []sinkapi.Config{{
			ID:   "shared-archive-s3",
			Type: "s3",
		}},
	}

	warnings := Warnings(cfg)
	if len(warnings) == 0 {
		t.Fatal("Warnings() returned no warnings, want shared bootstrap warning")
	}
	text := strings.Join(warnings, "\n")
	for _, want := range []string{
		"shared sink(s) shared-archive-s3 are configured",
		"neither state_path nor ledger_path exists yet",
		"may redeliver historical data",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Warnings() = %q, want mention of %q", text, want)
		}
	}
}

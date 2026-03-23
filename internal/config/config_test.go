package config

import (
	"encoding/json"
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

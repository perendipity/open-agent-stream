package config

import (
	"strings"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestApplyDefaultsSetsContinuousRunDefaults(t *testing.T) {
	cfg := Config{}
	applyDefaults(&cfg)

	if cfg.PollInterval != (3 * time.Second).String() {
		t.Fatalf("PollInterval = %q, want %q", cfg.PollInterval, (3 * time.Second).String())
	}
	if cfg.ErrorBackoff != (10 * time.Second).String() {
		t.Fatalf("ErrorBackoff = %q, want %q", cfg.ErrorBackoff, (10 * time.Second).String())
	}
	if cfg.MaxConsecutiveErrors != 10 {
		t.Fatalf("MaxConsecutiveErrors = %d, want 10", cfg.MaxConsecutiveErrors)
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

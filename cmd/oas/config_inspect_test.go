package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestBuildConfigInspectViewIncludesResolvedPaths(t *testing.T) {
	base := t.TempDir()
	configPath := filepath.Join(base, "oas.json")
	cfg := config.Config{
		Version:              "0.1",
		MachineID:            "test-machine",
		DataDir:              filepath.Join(base, "data"),
		StatePath:            filepath.Join(base, "data", "state.db"),
		LedgerPath:           filepath.Join(base, "data", "ledger.db"),
		BatchSize:            64,
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		MaxStorageBytes:      100,
		PruneTargetBytes:     80,
		MinFreeBytes:         50,
		Sources: []sourceapi.Config{{
			InstanceID: "codex-local",
			Type:       "codex_local",
			Root:       filepath.Join(base, "fixtures", "codex"),
		}},
		Sinks: []sinkapi.Config{{
			ID:   "stdout",
			Type: "stdout",
		}},
	}

	view := buildConfigInspectView(configPath, cfg)

	if got, want := view.ConfigPath, configPath; got != want {
		t.Fatalf("ConfigPath = %q, want %q", got, want)
	}
	for label, value := range map[string]string{
		"data_dir":         view.Resolved.DataDir,
		"state_path":       view.Resolved.StatePath,
		"ledger_path":      view.Resolved.LedgerPath,
		"daemon_pid_path":  view.Resolved.DaemonPIDPath,
		"daemon_log_path":  view.Resolved.DaemonLogPath,
		"daemon_meta_path": view.Resolved.DaemonMetaPath,
	} {
		if !filepath.IsAbs(value) {
			t.Fatalf("%s = %q, want absolute path", label, value)
		}
	}
	if got, want := len(view.Resolved.Sources), 1; got != want {
		t.Fatalf("len(Resolved.Sources) = %d, want %d", got, want)
	}
	if got, want := view.Resolved.Sources[0].Root, cfg.Sources[0].Root; got != want {
		t.Fatalf("Resolved.Sources[0].Root = %q, want %q", got, want)
	}
}

func TestWriteConfigInspectReportIncludesInspectabilitySections(t *testing.T) {
	view := configInspectView{
		ConfigPath: "/tmp/oas.json",
		ConfigDir:  "/tmp",
		Config: config.Config{
			Version:              "0.1",
			MachineID:            "example-machine",
			BatchSize:            64,
			PollInterval:         "3s",
			ErrorBackoff:         "10s",
			MaxConsecutiveErrors: 10,
			MaxStorageBytes:      100,
			PruneTargetBytes:     80,
			MinFreeBytes:         50,
			Sinks: []sinkapi.Config{{
				ID:   "stdout",
				Type: "stdout",
			}},
			Privacy: config.PrivacyConfig{
				Default: config.Policy{
					RedactKeys: []string{"api_key", "token"},
					Regexes:    []config.RegexRule{{Pattern: "x", Replacement: "y"}},
				},
				PerSink: map[string]config.Policy{
					"stdout": {},
				},
			},
		},
		Resolved: configResolvedPaths{
			DataDir:        "/tmp/data",
			StatePath:      "/tmp/data/state.db",
			LedgerPath:     "/tmp/data/ledger.db",
			DaemonPIDPath:  "/tmp/data/oas.pid",
			DaemonLogPath:  "/tmp/data/oas.log",
			DaemonMetaPath: "/tmp/data/oas.json",
			Sources: []configResolvedSourceRoot{{
				InstanceID: "codex-local",
				Type:       "codex_local",
				Root:       "/tmp/fixtures/codex",
			}},
		},
	}

	var out bytes.Buffer
	if err := writeConfigInspectReport(&out, view); err != nil {
		t.Fatalf("writeConfigInspectReport() error = %v", err)
	}

	text := out.String()
	for _, want := range []string{
		"Config:",
		"Resolved paths:",
		"State DB",
		"Ledger DB",
		"Daemon log",
		"Sources:",
		"codex-local",
		"Sinks:",
		"stdout",
		"Privacy:",
		"Per-sink overrides",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
	}
}

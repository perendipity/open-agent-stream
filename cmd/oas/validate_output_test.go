package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildValidationReportRequiresConfigPath(t *testing.T) {
	report := buildValidationReport("", repoRootForTest(t))
	if report.OK() {
		t.Fatal("report.OK() = true, want false")
	}
	if len(report.ConfigIssues) == 0 || report.ConfigIssues[0] != "config path is required" {
		t.Fatalf("ConfigIssues = %#v, want required config path message", report.ConfigIssues)
	}
}

func TestFormatValidationFailureIncludesIndexedConfigIssues(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(configPath, []byte(`{"version":"0.1","sources":[{"type":"codex_local"}],"sinks":[{"type":"stdout"}]}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	report := buildValidationReport(configPath, repoRootForTest(t))
	if report.OK() {
		t.Fatal("report.OK() = true, want false")
	}

	text := formatValidationFailure(report)
	for _, want := range []string{
		"validation failed",
		"Config: " + configPath,
		"Config issues:",
		"sources[0].instance_id: is required",
		"sources[0].root: is required",
		"sinks[0].id: is required",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted validation failure missing %q:\n%s", want, text)
		}
	}
}

func TestFormatValidationFailureIncludesRepoRootHintForMissingFixtures(t *testing.T) {
	report := validationReport{
		ConfigPath: "/tmp/oas.json",
		RootPath:   "/tmp/not-a-repo",
		FixtureIssues: []string{
			"stat /tmp/not-a-repo/fixtures/expected/codex_events.json: no such file or directory",
		},
	}

	text := formatValidationFailure(report)
	want := "fixture validation expects an open-agent-stream repo checkout"
	if !strings.Contains(text, want) {
		t.Fatalf("formatted validation failure missing repo-root hint %q:\n%s", want, text)
	}
}

func TestBuildValidationReportIncludesSharedBootstrapWarnings(t *testing.T) {
	base := t.TempDir()
	configPath := filepath.Join(base, "oas.json")
	if err := os.WriteFile(configPath, []byte(`{
  "version":"0.1",
  "machine_id":"machine-123",
  "state_path":"`+filepath.Join(base, "state.db")+`",
  "ledger_path":"`+filepath.Join(base, "ledger.db")+`",
  "sources":[{"instance_id":"codex-local","type":"codex_local","root":"`+base+`"}],
  "sinks":[{"id":"shared-archive-s3","type":"s3","settings":{"bucket":"demo","region":"us-west-2"}}]
}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	report := buildValidationReport(configPath, repoRootForTest(t))
	if !report.OK() {
		t.Fatalf("report.OK() = false, want true; issues=%#v warnings=%#v", report.ConfigIssues, report.ConfigWarnings)
	}

	text := strings.Join(report.ConfigWarnings, "\n")
	if !strings.Contains(text, "neither state_path nor ledger_path exists yet") {
		t.Fatalf("ConfigWarnings = %#v, want shared bootstrap warning", report.ConfigWarnings)
	}
}

func repoRootForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("Abs(repo root) error = %v", err)
	}
	return root
}

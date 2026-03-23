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

func repoRootForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("Abs(repo root) error = %v", err)
	}
	return root
}

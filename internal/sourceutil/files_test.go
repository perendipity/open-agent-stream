package sourceutil

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestReadArtifactHandlesLargeJSONLines(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "large.jsonl")
	huge := strings.Repeat("x", 5*1024*1024)
	record := map[string]any{
		"timestamp":  "2026-03-19T17:31:18Z",
		"type":       "message.user",
		"session_id": "sess-1",
		"project":    "repo://demo",
		"text":       huge,
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	artifacts, err := DiscoverJSONArtifacts(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(artifacts))
	}

	envelopes, _, err := ReadArtifact("codex_local", sourceapi.Config{
		InstanceID: "codex-local",
		Type:       "codex_local",
		Root:       dir,
	}, artifacts[0], sourceapi.Checkpoint{}, func(fields map[string]any) RecordInfo {
		return RecordInfo{
			RawKind:          stringValue(fields, "type"),
			SourceSessionKey: stringValue(fields, "session_id"),
			ProjectLocator:   stringValue(fields, "project"),
		}
	}, []string{"messages"})
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envelopes))
	}
	if envelopes[0].RawKind != "message.user" {
		t.Fatalf("unexpected raw kind: %s", envelopes[0].RawKind)
	}
}

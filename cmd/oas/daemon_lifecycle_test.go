package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
)

func TestDaemonPathsForIncludesStatePathIdentity(t *testing.T) {
	base := t.TempDir()
	cfgA := config.Config{
		MachineID: "shared",
		StatePath: filepath.Join(base, "state-a.db"),
	}
	cfgB := config.Config{
		MachineID: "shared",
		StatePath: filepath.Join(base, "state-b.db"),
	}

	pathsA := daemonPathsFor(cfgA)
	pathsB := daemonPathsFor(cfgB)

	if pathsA.pidPath == pathsB.pidPath {
		t.Fatalf("pidPath collision: %q", pathsA.pidPath)
	}
	if pathsA.logPath == pathsB.logPath {
		t.Fatalf("logPath collision: %q", pathsA.logPath)
	}
	if pathsA.metaPath == pathsB.metaPath {
		t.Fatalf("metaPath collision: %q", pathsA.metaPath)
	}
}

func TestDaemonStateKeyIncludesReadableBaseName(t *testing.T) {
	key := daemonStateKey("/tmp/oas/state-a.db")
	if !strings.HasPrefix(key, "state-a-") {
		t.Fatalf("daemonStateKey = %q, want readable state basename prefix", key)
	}
}

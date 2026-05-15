package hermes

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestNewTypeAndCapabilities(t *testing.T) {
	adapter := New()

	if got, want := adapter.Type(), "hermes_local"; got != want {
		t.Fatalf("Type() = %q, want %q", got, want)
	}

	want := []sourceapi.Capability{
		sourceapi.CapabilityMessages,
		sourceapi.CapabilityCommands,
		sourceapi.CapabilityToolCalls,
		sourceapi.CapabilityFileOps,
		sourceapi.CapabilityUsage,
	}
	if got := adapter.Capabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Capabilities() = %#v, want %#v", got, want)
	}
}

func TestDiscoverExplicitDBPathDiscoversOneArtifact(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "custom", "state.db"))

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"db_path": dbPath},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("Discover() returned %d artifacts, want 1: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", dbPath, root)
}

func TestDiscoverDefaultRootStateDB(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "state.db"))

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{Root: root})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("Discover() returned %d artifacts, want 1: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", dbPath, root)
}

func TestDiscoverProfilesAllIncludesDefaultAndProfileDBs(t *testing.T) {
	root := t.TempDir()
	defaultDB := createStateDB(t, filepath.Join(root, "state.db"))
	alphaDB := createStateDB(t, filepath.Join(root, "profiles", "alpha", "state.db"))
	zetaDB := createStateDB(t, filepath.Join(root, "profiles", "zeta", "state.db"))
	if err := os.MkdirAll(filepath.Join(root, "profiles", "missing"), 0o755); err != nil {
		t.Fatal(err)
	}

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"profiles": "all"},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 3 {
		t.Fatalf("Discover() returned %d artifacts, want 3: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", defaultDB, root)
	assertArtifact(t, artifacts[1], "alpha", alphaDB, root)
	assertArtifact(t, artifacts[2], "zeta", zetaDB, root)
}

func TestDiscoverCommaSeparatedProfilesIncludesSelectedProfileDBs(t *testing.T) {
	root := t.TempDir()
	defaultDB := createStateDB(t, filepath.Join(root, "state.db"))
	betaDB := createStateDB(t, filepath.Join(root, "profiles", "beta", "state.db"))
	gammaDB := createStateDB(t, filepath.Join(root, "profiles", "gamma", "state.db"))
	_ = createStateDB(t, filepath.Join(root, "profiles", "alpha", "state.db"))

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"profiles": " beta, missing, gamma "},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 3 {
		t.Fatalf("Discover() returned %d artifacts, want 3: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", defaultDB, root)
	assertArtifact(t, artifacts[1], "beta", betaDB, root)
	assertArtifact(t, artifacts[2], "gamma", gammaDB, root)
}

func TestDiscoverMissingDBReturnsNoArtifacts(t *testing.T) {
	root := t.TempDir()

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"profiles": "all"},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("Discover() returned %d artifacts, want 0: %#v", len(artifacts), artifacts)
	}
}

func TestDiscoverProfilesAllHandlesRootWithGlobCharacters(t *testing.T) {
	root := filepath.Join(t.TempDir(), "hermes[root]")
	defaultDB := createStateDB(t, filepath.Join(root, "state.db"))
	workDB := createStateDB(t, filepath.Join(root, "profiles", "work", "state.db"))

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"profiles": "all"},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("Discover() returned %d artifacts, want 2: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", defaultDB, root)
	assertArtifact(t, artifacts[1], "work", workDB, root)
}

func TestDiscoverCommaSeparatedProfilesSkipsInvalidProfileNames(t *testing.T) {
	root := t.TempDir()
	defaultDB := createStateDB(t, filepath.Join(root, "state.db"))
	validDB := createStateDB(t, filepath.Join(root, "profiles", "valid", "state.db"))
	_ = createStateDB(t, filepath.Join(root, "escape", "state.db"))

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"profiles": "../escape, valid, nested/name, ., .."},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("Discover() returned %d artifacts, want 2: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", defaultDB, root)
	assertArtifact(t, artifacts[1], "valid", validDB, root)
}

func createStateDB(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("sqlite db placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func assertArtifact(t *testing.T, artifact sourceapi.Artifact, profile, dbPath, root string) {
	t.Helper()
	if artifact.Locator != dbPath {
		t.Fatalf("artifact.Locator = %q, want %q", artifact.Locator, dbPath)
	}
	if artifact.ProjectLocator != root {
		t.Fatalf("artifact.ProjectLocator = %q, want %q", artifact.ProjectLocator, root)
	}
	if artifact.ID == "" {
		t.Fatal("artifact.ID is empty")
	}
	if artifact.Fingerprint == "" {
		t.Fatal("artifact.Fingerprint is empty")
	}
	if got := artifact.Metadata["profile"]; got != profile {
		t.Fatalf("artifact.Metadata[profile] = %q, want %q", got, profile)
	}
}

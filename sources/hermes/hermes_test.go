package hermes

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	_ "modernc.org/sqlite"
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

func TestDiscoverSkipsImplicitSymlinkedStateDB(t *testing.T) {
	root := t.TempDir()
	outsideDB := createStateDB(t, filepath.Join(t.TempDir(), "state.db"))
	if err := os.Symlink(outsideDB, filepath.Join(root, "state.db")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{Root: root})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("Discover() returned %d artifacts, want 0: %#v", len(artifacts), artifacts)
	}
}

func TestDiscoverExplicitDBPathAllowsSymlink(t *testing.T) {
	root := t.TempDir()
	outsideDB := createStateDB(t, filepath.Join(t.TempDir(), "state.db"))
	linkPath := filepath.Join(root, "explicit.db")
	if err := os.Symlink(outsideDB, linkPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	artifacts, err := New().Discover(context.Background(), sourceapi.Config{
		Root:    root,
		Options: map[string]string{"db_path": linkPath},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("Discover() returned %d artifacts, want 1: %#v", len(artifacts), artifacts)
	}
	assertArtifact(t, artifacts[0], "default", linkPath, root)
}

func TestReadReturnsExplicitUnimplementedError(t *testing.T) {
	dbPath := createStateDB(t, filepath.Join(t.TempDir(), "state.db"))

	_, checkpoint, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{Cursor: "cursor-1"})
	if err == nil {
		t.Fatal("Read() error = nil, want explicit unimplemented error")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("Read() error = %q, want not implemented", err.Error())
	}
	if checkpoint.Cursor != "cursor-1" {
		t.Fatalf("Read() checkpoint cursor = %q, want cursor-1", checkpoint.Cursor)
	}
}

func TestReadReturnsInvalidSQLiteErrorBeforeUnimplemented(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-sqlite.db")
	if err := os.WriteFile(path, []byte("this is not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: path}, sourceapi.Checkpoint{})
	if err == nil {
		t.Fatal("Read() error = nil, want invalid SQLite error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "sqlite") {
		t.Fatalf("Read() error = %q, want mention sqlite", err.Error())
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("Read() error = %q, want schema/open error before unimplemented", err.Error())
	}
}

func TestReadValidatesHermesSchemaBeforeUnimplemented(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), `CREATE TABLE sessions (id TEXT PRIMARY KEY);`)

	_, _, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{})
	if err == nil {
		t.Fatal("Read() error = nil, want missing messages table error")
	}
	if !strings.Contains(err.Error(), "messages") {
		t.Fatalf("Read() error = %q, want mention missing messages table", err.Error())
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("Read() error = %q, want schema error before unimplemented", err.Error())
	}
}

func TestValidateHermesSchemaReportsMissingMessagesColumn(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), `
CREATE TABLE messages (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id TEXT,
	role TEXT
);`)
	db := openTestSQLiteDB(t, dbPath)

	err := validateHermesSchema(context.Background(), db)
	if err == nil {
		t.Fatal("validateHermesSchema() error = nil, want missing column error")
	}
	if !strings.Contains(err.Error(), "content") {
		t.Fatalf("validateHermesSchema() error = %q, want mention missing content column", err.Error())
	}
}

func TestParseReadOptionsDefaults(t *testing.T) {
	got := parseReadOptions(nil)
	want := readOptions{includeSystemPrompt: false, includeReasoning: false, includeRawToolOutput: true}
	if got != want {
		t.Fatalf("parseReadOptions(nil) = %#v, want %#v", got, want)
	}
}

func TestParseReadOptionsExplicitTruthyValues(t *testing.T) {
	got := parseReadOptions(map[string]string{
		"include_system_prompt": "true",
		"include_reasoning":     "1",
	})
	if !got.includeSystemPrompt {
		t.Fatalf("includeSystemPrompt = false, want true")
	}
	if !got.includeReasoning {
		t.Fatalf("includeReasoning = false, want true")
	}
}

func TestParseReadOptionsIncludeRawToolOutputDefaultAndFalse(t *testing.T) {
	if got := parseReadOptions(nil); !got.includeRawToolOutput {
		t.Fatalf("includeRawToolOutput default = false, want true")
	}
	got := parseReadOptions(map[string]string{"include_raw_tool_output": "off"})
	if got.includeRawToolOutput {
		t.Fatalf("includeRawToolOutput = true, want false")
	}
}

func TestBoolOptionUnknownAndEmptyValuesFallBackToDefault(t *testing.T) {
	options := map[string]string{
		"empty":   "",
		"unknown": "sure",
		"yes":     "YES",
		"no":      "n",
	}
	if !boolOption(options, "empty", true) {
		t.Fatalf("empty value did not fall back to true default")
	}
	if boolOption(options, "unknown", false) {
		t.Fatalf("unknown value did not fall back to false default")
	}
	if !boolOption(options, "yes", false) {
		t.Fatalf("truthy value did not parse true")
	}
	if boolOption(options, "no", true) {
		t.Fatalf("falsey value did not parse false")
	}
}

type hermesStateFixture struct{}

func createStateDB(t *testing.T, path string) string {
	t.Helper()
	return createHermesStateDB(t, path, hermesStateFixture{})
}

func createHermesStateDB(t *testing.T, path string, fixture hermesStateFixture) string {
	t.Helper()
	resolved := createSQLiteDB(t, path, hermesStateSchema)
	db := openTestSQLiteDB(t, resolved)
	_ = fixture
	verifyHermesStateDB(t, db)
	return resolved
}

func createSQLiteDB(t *testing.T, path, schema string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	db := openTestSQLiteDB(t, resolved)
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	return resolved
}

func openTestSQLiteDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test SQLite DB: %v", err)
		}
	})
	return db
}

const hermesStateSchema = `
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	source TEXT,
	model TEXT,
	model_config TEXT,
	system_prompt TEXT,
	parent_session_id TEXT,
	started_at REAL,
	ended_at REAL,
	end_reason TEXT,
	message_count INTEGER,
	tool_call_count INTEGER,
	input_tokens INTEGER,
	output_tokens INTEGER,
	cache_read_tokens INTEGER,
	cache_write_tokens INTEGER,
	reasoning_tokens INTEGER,
	billing_provider TEXT,
	billing_base_url TEXT,
	billing_mode TEXT,
	estimated_cost_usd REAL,
	actual_cost_usd REAL,
	cost_status TEXT,
	cost_source TEXT,
	pricing_version TEXT,
	title TEXT,
	api_call_count INTEGER,
	handoff_state TEXT,
	handoff_platform TEXT,
	handoff_error TEXT
);

CREATE TABLE IF NOT EXISTS messages (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id TEXT,
	role TEXT,
	content TEXT,
	tool_call_id TEXT,
	tool_calls TEXT,
	tool_name TEXT,
	timestamp REAL,
	token_count INTEGER,
	finish_reason TEXT,
	reasoning TEXT,
	reasoning_content TEXT,
	reasoning_details TEXT,
	codex_reasoning_items TEXT,
	codex_message_items TEXT
);`

func verifyHermesStateDB(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"sessions", "messages"} {
		var name string
		if err := db.QueryRow("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?", table).Scan(&name); err != nil {
			t.Fatalf("verify Hermes state DB table %q: %v", table, err)
		}
	}
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

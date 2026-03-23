package analytics

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

func TestBuildStatusAndQuery(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "session.started",
	})
	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "message.user",
		Extra:      map[string]any{"text": "hello"},
	})
	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "command.started",
		Extra:      map[string]any{"call_id": "call-1"},
	})
	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "command.finished",
		Extra:      map[string]any{"call_id": "call-1", "exit_code": 1, "duration_ms": 1200},
	})
	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "tool.failed",
		Extra:      map[string]any{"tool": "web", "exit_code": 2, "stderr": "timeout"},
	})

	status, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if got, want := status.CoverageMode, string(CoverageCompleteHistory); got != want {
		t.Fatalf("CoverageMode = %q, want %q", got, want)
	}
	if got, want := status.RowCounts["event_facts"], int64(5); got != want {
		t.Fatalf("event_facts rows = %d, want %d", got, want)
	}
	if got, want := status.RowCounts["command_rollups"], int64(1); got != want {
		t.Fatalf("command_rollups rows = %d, want %d", got, want)
	}

	columns, rows, _, err := svc.Query(ctx, QueryOptions{
		Preset: "sessions",
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(columns) == 0 || len(rows) != 1 {
		t.Fatalf("query result = %d columns / %d rows, want one row", len(columns), len(rows))
	}

	db, err := openDuckDB(status.Path)
	if err != nil {
		t.Fatalf("openDuckDB() error = %v", err)
	}
	defer db.Close()
	queryColumns, _, err := runQuery(ctx, db, `SELECT * FROM event_facts LIMIT 1`)
	if err != nil {
		t.Fatalf("runQuery() error = %v", err)
	}
	for _, forbidden := range []string{"payload", "text", "command", "stdout", "stderr", "raw_payload"} {
		for _, column := range queryColumns {
			if column == forbidden {
				t.Fatalf("event_facts unexpectedly exposed forbidden column %q", forbidden)
			}
		}
	}

	row := db.QueryRow(`SELECT failed_command_rollup_count, failure_signal_count, user_message_count FROM session_rollups`)
	var failedCommands, failures, userMessages int64
	if err := row.Scan(&failedCommands, &failures, &userMessages); err != nil {
		t.Fatalf("Scan(session_rollups) error = %v", err)
	}
	if failedCommands != 1 || failures != 2 || userMessages != 1 {
		t.Fatalf("session rollup mismatch: failed=%d failures=%d user_messages=%d", failedCommands, failures, userMessages)
	}
}

func TestBuildBackfillsMachineIDWhenLedgerHistoryIsEmptyOfMachineIDs(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "session.started",
		NoMachine:  true,
	})

	status, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if got, want := status.MachineIDOrigin, string(MachineIDOriginConfigBackfill); got != want {
		t.Fatalf("MachineIDOrigin = %q, want %q", got, want)
	}
}

func TestBuildRequiresRebuildWhenMachineIDChanges(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-1",
		Kind:       "session.started",
	})
	if _, err := svc.Build(ctx, BuildOptions{}); err != nil {
		t.Fatalf("initial Build() error = %v", err)
	}

	second := New(config.Config{
		MachineID: "machine-b",
		DataDir:   svc.cfg.DataDir,
		BatchSize: 2,
	}, svc.ledger)
	_, err := second.Build(ctx, BuildOptions{Path: DefaultPath(svc.cfg)})
	if err == nil || !strings.Contains(err.Error(), "requires a rebuild") {
		t.Fatalf("Build() error = %v, want rebuild-required error", err)
	}
}

func TestCoverageTransitionsForPruning(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.user", Extra: map[string]any{"text": "hi"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.agent", Extra: map[string]any{"text": "ok"}})

	status, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if status.CoverageMode != string(CoverageCompleteHistory) {
		t.Fatalf("CoverageMode = %q, want %q", status.CoverageMode, CoverageCompleteHistory)
	}

	if _, err := svc.ledger.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough() error = %v", err)
	}
	status, err = svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() after prune error = %v", err)
	}
	if status.CoverageMode != string(CoverageCachePreservedHistory) {
		t.Fatalf("CoverageMode after prune = %q, want %q", status.CoverageMode, CoverageCachePreservedHistory)
	}

	second, secondCleanup := newTestService(t, "machine-a")
	defer secondCleanup()
	appendTestEnvelope(t, second.ledger, testEnvelope{SessionKey: "sess-2", Kind: "session.started"})
	appendTestEnvelope(t, second.ledger, testEnvelope{SessionKey: "sess-2", Kind: "message.user", Extra: map[string]any{"text": "hi"}})
	appendTestEnvelope(t, second.ledger, testEnvelope{SessionKey: "sess-2", Kind: "message.agent", Extra: map[string]any{"text": "ok"}})
	if _, err := second.ledger.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough(second) error = %v", err)
	}
	status, err = second.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build(second) error = %v", err)
	}
	if status.CoverageMode != string(CoverageRetainedWindowOnly) {
		t.Fatalf("CoverageMode second = %q, want %q", status.CoverageMode, CoverageRetainedWindowOnly)
	}
}

func TestExportWritesManifestAndParquetFiles(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "command.started", Extra: map[string]any{"call_id": "call-1"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "command.finished", Extra: map[string]any{"call_id": "call-1", "exit_code": 0}})

	outputDir := filepath.Join(t.TempDir(), "export")
	manifest, status, err := svc.Export(ctx, ExportOptions{OutputDir: outputDir})
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if status.RowCounts["event_facts"] != 3 {
		t.Fatalf("event_facts rows = %d, want 3", status.RowCounts["event_facts"])
	}
	if len(manifest.Tables) != 4 {
		t.Fatalf("len(manifest.Tables) = %d, want 4", len(manifest.Tables))
	}
	for _, table := range manifest.Tables {
		if table.SHA256 == "" {
			t.Fatalf("table %s missing SHA256", table.Name)
		}
		if _, err := os.Stat(filepath.Join(outputDir, table.File)); err != nil {
			t.Fatalf("Stat(%s) error = %v", table.File, err)
		}
	}
	manifestPath := filepath.Join(outputDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile(manifest) error = %v", err)
	}
	var decoded Manifest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(manifest) error = %v", err)
	}
	if decoded.CoverageMode == "" {
		t.Fatal("manifest coverage_mode unexpectedly empty")
	}
}

func TestBuildRequiresRebuildAfterInterruptedState(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	status, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	db, err := openDuckDB(status.Path)
	if err != nil {
		t.Fatalf("openDuckDB() error = %v", err)
	}
	defer db.Close()
	meta, ok, err := loadMeta(ctx, db)
	if err != nil || !ok {
		t.Fatalf("loadMeta() = (%v, %v, %v), want meta", meta, ok, err)
	}
	meta.BuildState = string(BuildStateBuilding)
	if err := replaceMeta(ctx, db, meta); err != nil {
		t.Fatalf("replaceMeta() error = %v", err)
	}

	_, err = svc.Build(ctx, BuildOptions{})
	if err == nil || !strings.Contains(err.Error(), "requires a rebuild") {
		t.Fatalf("Build() error = %v, want rebuild-required error", err)
	}
}

type testEnvelope struct {
	SessionKey string
	Kind       string
	Extra      map[string]any
	NoMachine  bool
}

func newTestService(t *testing.T, machineID string) (*Service, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := ledger.Open(filepath.Join(dir, "ledger.db"))
	if err != nil {
		t.Fatalf("ledger.Open() error = %v", err)
	}
	cfg := config.Config{
		MachineID: machineID,
		DataDir:   dir,
		BatchSize: 2,
	}
	return New(cfg, store), func() {
		_ = store.Close()
	}
}

func appendTestEnvelope(t *testing.T, store *ledger.Store, item testEnvelope) {
	t.Helper()
	payload := map[string]any{
		"type":       item.Kind,
		"session_id": item.SessionKey,
		"project":    "repo://project",
	}
	for key, value := range item.Extra {
		payload[key] = value
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	machineID := "machine-a"
	if item.NoMachine {
		machineID = ""
	}
	_, err = store.Append(schema.RawEnvelope{
		EnvelopeVersion:  schema.RawEnvelopeVersion,
		SourceType:       "test_local",
		SourceInstanceID: "source-1",
		ArtifactID:       "artifact-1",
		ArtifactLocator:  "/tmp/artifact.jsonl",
		ProjectLocator:   "repo://project",
		MachineID:        machineID,
		Cursor:           schema.Cursor{Kind: "line", Value: strconvForOffset(time.Now().UnixNano())},
		ObservedAt:       time.Now().UTC(),
		SourceTimestamp:  timePtr(time.Now().UTC()),
		RawKind:          item.Kind,
		RawPayload:       data,
		ContentHash:      schema.HashBytes(data),
		ParseHints: schema.ParseHints{
			SourceSessionKey: item.SessionKey,
			ProjectHint:      "repo://project",
			Confidence:       0.8,
			Capabilities:     []string{"messages", "commands"},
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func strconvForOffset(value int64) string {
	return strings.ReplaceAll(time.Unix(0, value).UTC().Format(time.RFC3339Nano), ":", "_")
}

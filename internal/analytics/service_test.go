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

func TestQueryPresetAliasesAndColumns(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.user", Extra: map[string]any{"text": "hi"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "command.finished", Extra: map[string]any{"exit_code": 2}})

	columns, rows, _, err := svc.Query(ctx, QueryOptions{Preset: "failures"})
	if err != nil {
		t.Fatalf("Query(failures) error = %v", err)
	}
	if got, want := strings.Join(columns, ","), "session,source,project,status,failures,failed_cmds,incomplete_cmds,last_seen,age,span"; got != want {
		t.Fatalf("attention alias columns = %q, want %q", got, want)
	}
	if len(rows) != 1 {
		t.Fatalf("Query(failures) rows = %d, want 1", len(rows))
	}

	columns, rows, _, err = svc.Query(ctx, QueryOptions{Preset: "sessions"})
	if err != nil {
		t.Fatalf("Query(sessions) error = %v", err)
	}
	if got, want := strings.Join(columns, ","), "session,source,project,events,user_msgs,agent_msgs,commands,last_seen,age,span"; got != want {
		t.Fatalf("recent_sessions alias columns = %q, want %q", got, want)
	}
	if len(rows) != 1 {
		t.Fatalf("Query(sessions) rows = %d, want 1", len(rows))
	}
}

func TestAttentionPresetExcludesCleanSessions(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-clean", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-clean", Kind: "message.user", Extra: map[string]any{"text": "hi"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-clean", Kind: "message.agent", Extra: map[string]any{"text": "ok"}})

	columns, rows, _, err := svc.Query(ctx, QueryOptions{Preset: "failures"})
	if err != nil {
		t.Fatalf("Query(failures) error = %v", err)
	}
	if got, want := strings.Join(columns, ","), "session,source,project,status,failures,failed_cmds,incomplete_cmds,last_seen,age,span"; got != want {
		t.Fatalf("attention alias columns = %q, want %q", got, want)
	}
	if len(rows) != 0 {
		t.Fatalf("Query(failures) rows = %d, want 0 for a clean-only fixture", len(rows))
	}
}

func TestAttentionPresetIncludesIncompleteSessions(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-incomplete", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{
		SessionKey: "sess-incomplete",
		Kind:       "command.started",
		Extra:      map[string]any{"call_id": "call-1"},
	})

	columns, rows, _, err := svc.Query(ctx, QueryOptions{Preset: "attention"})
	if err != nil {
		t.Fatalf("Query(attention) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Query(attention) rows = %d, want 1 for an incomplete-only session", len(rows))
	}
	row := queryRowsToMaps(columns, rows)[0]
	if got, want := mapString(row["status"]), "incomplete"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := mapInt64(row["incomplete_cmds"]), int64(1); got != want {
		t.Fatalf("incomplete_cmds = %d, want %d", got, want)
	}
}

func TestQueryProjectsAndSourcesAggregateCounts(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.user", Extra: map[string]any{"text": "hello"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "command.finished", Extra: map[string]any{"exit_code": 1}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-2", Kind: "session.started"})

	columns, rows, _, err := svc.Query(ctx, QueryOptions{Preset: "projects"})
	if err != nil {
		t.Fatalf("Query(projects) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Query(projects) rows = %d, want 1", len(rows))
	}
	project := queryRowsToMaps(columns, rows)[0]
	if got, want := mapInt64(project["sessions"]), int64(2); got != want {
		t.Fatalf("projects sessions = %d, want %d", got, want)
	}
	if got, want := mapInt64(project["events"]), int64(4); got != want {
		t.Fatalf("projects events = %d, want %d", got, want)
	}
	if got, want := mapInt64(project["commands"]), int64(1); got != want {
		t.Fatalf("projects commands = %d, want %d", got, want)
	}
	if got, want := mapInt64(project["failures"]), int64(1); got != want {
		t.Fatalf("projects failures = %d, want %d", got, want)
	}

	columns, rows, _, err = svc.Query(ctx, QueryOptions{Preset: "sources"})
	if err != nil {
		t.Fatalf("Query(sources) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Query(sources) rows = %d, want 1", len(rows))
	}
	source := queryRowsToMaps(columns, rows)[0]
	if got, want := mapInt64(source["sessions"]), int64(2); got != want {
		t.Fatalf("sources sessions = %d, want %d", got, want)
	}
	if got, want := mapInt64(source["events"]), int64(4); got != want {
		t.Fatalf("sources events = %d, want %d", got, want)
	}
	if got, want := mapInt64(source["commands"]), int64(1); got != want {
		t.Fatalf("sources commands = %d, want %d", got, want)
	}
	if got, want := mapInt64(source["failures"]), int64(1); got != want {
		t.Fatalf("sources failures = %d, want %d", got, want)
	}
}

func TestQuerySensitivePreviewsAreTransientAndRespectRedaction(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestServiceWithConfig(t, config.Config{
		MachineID: "machine-a",
		BatchSize: 2,
		Privacy: config.PrivacyConfig{
			PerSink: map[string]config.Policy{
				analyticsSensitivePolicyID: {
					RedactKeys: []string{"text"},
				},
			},
		},
	})
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.user", Extra: map[string]any{"text": "hello world"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.agent", Extra: map[string]any{"text": "done"}})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "command.finished", Extra: map[string]any{"exit_code": 1}})

	columns, rows, status, err := svc.Query(ctx, QueryOptions{
		Preset:    "attention",
		Sensitive: true,
	})
	if err != nil {
		t.Fatalf("Query(attention -sensitive) error = %v", err)
	}
	if got, want := strings.Join(columns, ","), "session,source,project,status,failures,failed_cmds,incomplete_cmds,last_seen,age,span,content_status,first_user_preview,last_agent_preview"; got != want {
		t.Fatalf("sensitive columns = %q, want %q", got, want)
	}
	if len(rows) != 1 {
		t.Fatalf("sensitive rows = %d, want 1", len(rows))
	}
	row := queryRowsToMaps(columns, rows)[0]
	if got, want := mapString(row["content_status"]), "redacted"; got != want {
		t.Fatalf("content_status = %q, want %q", got, want)
	}
	if got, want := mapString(row["first_user_preview"]), "[REDACTED]"; got != want {
		t.Fatalf("first_user_preview = %q, want %q", got, want)
	}
	if got, want := mapString(row["last_agent_preview"]), "[REDACTED]"; got != want {
		t.Fatalf("last_agent_preview = %q, want %q", got, want)
	}

	db, err := openDuckDB(status.Path)
	if err != nil {
		t.Fatalf("openDuckDB() error = %v", err)
	}
	defer db.Close()
	queryColumns, _, err := runQuery(ctx, db, `SELECT * FROM event_facts LIMIT 1`)
	if err != nil {
		t.Fatalf("runQuery(event_facts) error = %v", err)
	}
	for _, forbidden := range []string{"content_status", "first_user_preview", "last_agent_preview"} {
		for _, column := range queryColumns {
			if column == forbidden {
				t.Fatalf("event_facts unexpectedly exposed sensitive column %q", forbidden)
			}
		}
	}
}

func TestQuerySensitiveFilteredByPolicy(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestServiceWithConfig(t, config.Config{
		MachineID: "machine-a",
		BatchSize: 2,
		Privacy: config.PrivacyConfig{
			PerSink: map[string]config.Policy{
				analyticsSensitivePolicyID: {
					DeniedPaths: []string{"repo://project"},
				},
			},
		},
	})
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})
	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "message.user", Extra: map[string]any{"text": "hello world"}})

	columns, rows, _, err := svc.Query(ctx, QueryOptions{
		Preset:    "recent_sessions",
		Sensitive: true,
	})
	if err != nil {
		t.Fatalf("Query(recent_sessions -sensitive) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("sensitive filtered rows = %d, want 1", len(rows))
	}
	row := queryRowsToMaps(columns, rows)[0]
	if got, want := mapString(row["content_status"]), "filtered"; got != want {
		t.Fatalf("content_status = %q, want %q", got, want)
	}
	if got := mapString(row["first_user_preview"]); got != "-" {
		t.Fatalf("first_user_preview = %q, want -", got)
	}
}

func TestQueryRejectsSensitiveSQLNegativeLimitAndWritableSQL(t *testing.T) {
	ctx := context.Background()
	svc, cleanup := newTestService(t, "machine-a")
	defer cleanup()

	appendTestEnvelope(t, svc.ledger, testEnvelope{SessionKey: "sess-1", Kind: "session.started"})

	status, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if _, _, _, err := svc.Query(ctx, QueryOptions{
		SQL:       "select 1",
		Sensitive: true,
	}); err == nil || !strings.Contains(err.Error(), "-sensitive") {
		t.Fatalf("Query(sql+sensitive) error = %v, want -sensitive conflict", err)
	}

	if _, _, _, err := svc.Query(ctx, QueryOptions{
		Preset: "overview",
		Limit:  -1,
	}); err == nil || !strings.Contains(err.Error(), "-limit") {
		t.Fatalf("Query(limit=-1) error = %v, want limit validation", err)
	}

	if _, _, _, err := svc.Query(ctx, QueryOptions{
		Path: status.Path,
		SQL:  "delete from analytics_meta",
	}); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Query(delete) error = %v, want read-only validation", err)
	}

	db, err := openDuckDB(status.Path)
	if err != nil {
		t.Fatalf("openDuckDB() error = %v", err)
	}
	defer db.Close()
	var metaCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM analytics_meta`).Scan(&metaCount); err != nil {
		t.Fatalf("Scan(analytics_meta) error = %v", err)
	}
	if got, want := metaCount, 1; got != want {
		t.Fatalf("analytics_meta rows = %d, want %d after rejected writable query", got, want)
	}
}

func TestValidateReadOnlyQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "select allowed", query: "select * from event_facts limit 1"},
		{name: "with select allowed", query: "with x as (select 1 as n) select n from x"},
		{name: "delete rejected", query: "delete from analytics_meta", wantErr: "read-only"},
		{name: "multiple statements rejected", query: "select 1; delete from analytics_meta", wantErr: "single statement"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateReadOnlyQuery(tc.query)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateReadOnlyQuery() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateReadOnlyQuery() error = %v, want substring %q", err, tc.wantErr)
			}
		})
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
	return newTestServiceWithConfig(t, config.Config{
		MachineID: machineID,
		BatchSize: 2,
	})
}

func newTestServiceWithConfig(t *testing.T, cfg config.Config) (*Service, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := ledger.Open(filepath.Join(dir, "ledger.db"))
	if err != nil {
		t.Fatalf("ledger.Open() error = %v", err)
	}
	cfg.DataDir = dir
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 2
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

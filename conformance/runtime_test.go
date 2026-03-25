package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/router"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/internal/supervisor"
	"github.com/open-agent-stream/open-agent-stream/pkg/conformance"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	claudesource "github.com/open-agent-stream/open-agent-stream/sources/claude"
	codexsource "github.com/open-agent-stream/open-agent-stream/sources/codex"
	_ "modernc.org/sqlite"
)

func TestFixtureNormalization(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	testCases := []struct {
		name         string
		adapter      sourceapi.Adapter
		sourceConfig sourceapi.Config
		expectedFile string
		sourceRoot   string
	}{
		{
			name:         "codex",
			adapter:      codexsource.New(),
			sourceConfig: sourceapi.Config{InstanceID: "codex-local", Type: "codex_local", Root: filepath.Join(root, "fixtures", "sources", "codex")},
			expectedFile: filepath.Join(root, "fixtures", "expected", "codex_events.json"),
			sourceRoot:   filepath.Join(root, "fixtures", "sources", "codex"),
		},
		{
			name:         "claude",
			adapter:      claudesource.New(),
			sourceConfig: sourceapi.Config{InstanceID: "claude-local", Type: "claude_local", Root: filepath.Join(root, "fixtures", "sources", "claude")},
			expectedFile: filepath.Join(root, "fixtures", "expected", "claude_events.json"),
			sourceRoot:   filepath.Join(root, "fixtures", "sources", "claude"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			artifacts, err := tc.adapter.Discover(context.Background(), tc.sourceConfig)
			if err != nil {
				t.Fatal(err)
			}
			if len(artifacts) != 1 {
				t.Fatalf("expected 1 artifact, got %d", len(artifacts))
			}
			envelopes, _, err := tc.adapter.Read(context.Background(), tc.sourceConfig, artifacts[0], sourceapi.Checkpoint{})
			if err != nil {
				t.Fatal(err)
			}
			normalizer := normalize.NewService(normalize.NewMemorySequenceStore())
			events := make([]schema.CanonicalEvent, 0, len(envelopes))
			for idx, envelope := range envelopes {
				event, err := normalizer.Normalize(ledger.Record{
					Offset:   int64(idx + 1),
					Envelope: envelope,
				})
				if err != nil {
					t.Fatal(err)
				}
				events = append(events, event)
			}

			expected, err := conformance.LoadExpectedEvents(tc.expectedFile)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expected, events) {
				t.Fatalf("fixture mismatch\nexpected: %#v\nactual: %#v", expected, events)
			}
		})
	}
}

func TestReplayDeterminism(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	adapter := codexsource.New()
	cfg := sourceapi.Config{InstanceID: "codex-local", Type: "codex_local", Root: filepath.Join(root, "fixtures", "sources", "codex")}
	artifacts, err := adapter.Discover(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	envelopes, _, err := adapter.Read(context.Background(), cfg, artifacts[0], sourceapi.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	run := func() []schema.CanonicalEvent {
		service := normalize.NewService(normalize.NewMemorySequenceStore())
		events := make([]schema.CanonicalEvent, 0, len(envelopes))
		for idx, envelope := range envelopes {
			event, err := service.Normalize(ledger.Record{Offset: int64(idx + 1), Envelope: envelope})
			if err != nil {
				t.Fatal(err)
			}
			events = append(events, event)
		}
		return events
	}

	first := run()
	second := run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replay mismatch\nfirst: %#v\nsecond: %#v", first, second)
	}
}

func TestRuntimeReplaySkipsAppendOnlySinksByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := runtimeFixtureConfig(t, t.TempDir())

	runtime := mustRuntimeForTest(t, cfg)
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, runtime)

	initialLines := countJSONLLines(t, cfg.Sinks[0].Options["path"])
	if initialLines != 3 {
		t.Fatalf("expected 3 jsonl lines after run, got %d", initialLines)
	}
	initialEvents := countSQLiteEvents(t, cfg.Sinks[1].Options["path"])
	if initialEvents != 3 {
		t.Fatalf("expected 3 sqlite events after run, got %d", initialEvents)
	}

	replayRuntime := mustRuntimeForTest(t, cfg)
	plan, err := replayRuntime.ReplayPlan(supervisor.ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Decisions) != 2 {
		t.Fatalf("expected 2 replay decisions, got %d", len(plan.Decisions))
	}
	if plan.Decisions[0].SinkID != "jsonl-local" || plan.Decisions[0].Action != "skip" {
		t.Fatalf("expected jsonl-local to be skipped by default, got %#v", plan.Decisions[0])
	}
	if plan.Decisions[1].SinkID != "sqlite-local" || plan.Decisions[1].Action != "include" {
		t.Fatalf("expected sqlite-local to be included by default, got %#v", plan.Decisions[1])
	}
	if err := replayRuntime.ReplayWithOptions(ctx, supervisor.ReplayOptions{}); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, replayRuntime)

	linesAfterReplay := countJSONLLines(t, cfg.Sinks[0].Options["path"])
	if linesAfterReplay != 3 {
		t.Fatalf("expected jsonl sink to remain at 3 lines after default replay, got %d", linesAfterReplay)
	}
	eventsAfterReplay := countSQLiteEvents(t, cfg.Sinks[1].Options["path"])
	if eventsAfterReplay != 3 {
		t.Fatalf("expected sqlite sink to remain at 3 events after replay, got %d", eventsAfterReplay)
	}
}

func TestRuntimeReplayIncludesAppendOnlyWhenRequested(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := runtimeFixtureConfig(t, t.TempDir())

	runtime := mustRuntimeForTest(t, cfg)
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, runtime)

	replayRuntime := mustRuntimeForTest(t, cfg)
	options := supervisor.ReplayOptions{IncludeNonIdempotent: true}
	plan, err := replayRuntime.ReplayPlan(options)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Decisions[0].Action != "include" {
		t.Fatalf("expected jsonl-local to be included when explicitly requested, got %#v", plan.Decisions[0])
	}
	if err := replayRuntime.ReplayWithOptions(ctx, options); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, replayRuntime)

	linesAfterReplay := countJSONLLines(t, cfg.Sinks[0].Options["path"])
	if linesAfterReplay != 6 {
		t.Fatalf("expected jsonl sink to contain duplicate deliveries after explicit replay, got %d lines", linesAfterReplay)
	}
	eventsAfterReplay := countSQLiteEvents(t, cfg.Sinks[1].Options["path"])
	if eventsAfterReplay != 3 {
		t.Fatalf("expected sqlite sink to stay idempotent after explicit replay, got %d events", eventsAfterReplay)
	}
}

func TestRuntimeExportStable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := runtimeFixtureConfig(t, t.TempDir())

	runtime := mustRuntimeForTest(t, cfg)
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, runtime)

	exportRuntime := mustRuntimeForTest(t, cfg)
	var first bytes.Buffer
	var second bytes.Buffer
	if err := exportRuntime.Export(ctx, &first); err != nil {
		t.Fatal(err)
	}
	if err := exportRuntime.Export(ctx, &second); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, exportRuntime)

	if first.String() != second.String() {
		t.Fatalf("expected export output to be stable across repeated runs\nfirst: %s\nsecond: %s", first.String(), second.String())
	}
}

func TestRuntimeV2EventSpecSeparatesSharedDestinationsByMachineID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := repoRoot(t)
	sharedPath := filepath.Join(t.TempDir(), "events.jsonl")

	cfgForMachine := func(machineID, stateDir string) config.Config {
		return config.Config{
			Version:    "0.1",
			MachineID:  machineID,
			StatePath:  filepath.Join(stateDir, "state.db"),
			LedgerPath: filepath.Join(stateDir, "ledger.db"),
			BatchSize:  64,
			Sources: []sourceapi.Config{{
				InstanceID: "shared-codex",
				Type:       "codex_local",
				Root:       filepath.Join(root, "fixtures", "sources", "codex"),
			}},
			Sinks: []sinkapi.Config{{
				ID:               "jsonl-shared",
				Type:             "jsonl",
				EventSpecVersion: schema.EventSpecV2,
				Options: map[string]string{
					"path": sharedPath,
				},
			}},
		}
	}

	runtimeA := mustRuntimeForTest(t, cfgForMachine("machine-a", filepath.Join(t.TempDir(), "a")))
	if err := runtimeA.Run(ctx); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, runtimeA)

	runtimeB := mustRuntimeForTest(t, cfgForMachine("machine-b", filepath.Join(t.TempDir(), "b")))
	if err := runtimeB.Run(ctx); err != nil {
		t.Fatal(err)
	}
	closeRuntimeForTest(t, runtimeB)

	data, err := os.ReadFile(sharedPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if got, want := len(lines), 6; got != want {
		t.Fatalf("line count=%d, want %d", got, want)
	}
	sessionKeys := map[string]struct{}{}
	eventIDs := map[string]struct{}{}
	machines := map[string]struct{}{}
	for _, line := range lines {
		var event schema.CanonicalEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if got, want := event.EventVersion, schema.CanonicalEventVersionV2; got != want {
			t.Fatalf("event_version=%d, want %d", got, want)
		}
		sessionKeys[event.SessionKey] = struct{}{}
		eventIDs[event.EventID] = struct{}{}
		machines[event.MachineID] = struct{}{}
	}
	if got, want := len(machines), 2; got != want {
		t.Fatalf("machine ids=%d, want %d", got, want)
	}
	if got, want := len(sessionKeys), 2; got != want {
		t.Fatalf("session keys=%d, want %d", got, want)
	}
	if got, want := len(eventIDs), len(lines); got != want {
		t.Fatalf("unique event ids=%d, want %d", got, want)
	}
}

func TestCodexArchivedShapeNormalization(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "session.jsonl")
	lines := []string{
		`{"timestamp":"2026-03-19T17:32:18.295Z","type":"session_meta","payload":{"id":"codex-session-archived","cwd":"/tmp/project","originator":"Codex Desktop","source":"vscode","model_provider":"openai"}}`,
		`{"timestamp":"2026-03-19T17:32:18.297Z","type":"event_msg","payload":{"type":"user_message","message":"please run tests","images":[],"text_elements":[]}}`,
		`{"timestamp":"2026-03-19T17:32:18.350Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Inspecting the repo."}]}}`,
		`{"timestamp":"2026-03-19T17:32:18.400Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"go test ./...\"}","call_id":"call-1"}}`,
		`{"timestamp":"2026-03-19T17:32:18.500Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"Command: /bin/zsh -lc 'go test ./...'\nProcess exited with code 0\nOutput:\nok\n"}}`,
		`{"timestamp":"2026-03-19T17:32:18.600Z","type":"event_msg","payload":{"type":"agent_message","message":"Tests passed.","phase":"commentary"}}`,
		`{"timestamp":"2026-03-19T17:32:18.700Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3},"total_token_usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}}`,
		`{"timestamp":"2026-03-19T17:32:18.800Z","type":"turn_context","payload":{"cwd":"/tmp/project","approval_policy":"on-request","sandbox_policy":{"type":"workspace-write"},"model":"gpt-5.4"}}`,
		`{"timestamp":"2026-03-19T17:32:18.900Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":"done"}}`,
	}
	if err := os.WriteFile(sourcePath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	adapter := codexsource.New()
	cfg := sourceapi.Config{InstanceID: "codex-local", Type: "codex_local", Root: dir}
	artifacts, err := adapter.Discover(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(artifacts))
	}
	envelopes, _, err := adapter.Read(context.Background(), cfg, artifacts[0], sourceapi.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	service := normalize.NewService(normalize.NewMemorySequenceStore())
	events := make([]schema.CanonicalEvent, 0, len(envelopes))
	for idx, envelope := range envelopes {
		event, err := service.Normalize(ledger.Record{
			Offset:   int64(idx + 1),
			Envelope: envelope,
		})
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}

	wantKinds := []string{
		"session.started",
		"message.user",
		"message.agent",
		"command.started",
		"command.finished",
		"message.agent",
		"usage.reported",
		"model.changed",
		"message.system",
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("expected %d events, got %d", len(wantKinds), len(events))
	}
	for idx, event := range events {
		if event.Kind != wantKinds[idx] {
			t.Fatalf("event %d kind mismatch: want %s got %s", idx, wantKinds[idx], event.Kind)
		}
		if event.ParseStatus != "ok" {
			t.Fatalf("event %d parse status mismatch: %s", idx, event.ParseStatus)
		}
		if event.Context.ProjectLocator != "/tmp/project" {
			t.Fatalf("event %d project locator mismatch: %s", idx, event.Context.ProjectLocator)
		}
		if event.Context.SourceSessionKey != "codex-session-archived" {
			t.Fatalf("event %d source session key mismatch: %s", idx, event.Context.SourceSessionKey)
		}
		if event.SessionKey != events[0].SessionKey {
			t.Fatalf("event %d session key mismatch: %s", idx, event.SessionKey)
		}
	}
	if events[2].Payload["text"] != "Inspecting the repo." {
		t.Fatalf("expected assistant text extraction, got %#v", events[2].Payload["text"])
	}
	if events[3].Payload["command"] != "go test ./..." {
		t.Fatalf("expected command extraction, got %#v", events[3].Payload["command"])
	}
	if events[4].Payload["exit_code"] != 0 {
		t.Fatalf("expected exit code extraction, got %#v", events[4].Payload["exit_code"])
	}
}

func TestRedactionPolicy(t *testing.T) {
	t.Parallel()

	engine := redact.NewEngine(config.Config{
		Privacy: config.PrivacyConfig{
			Default: config.Policy{
				RedactKeys: []string{"token"},
				Regexes: []config.RegexRule{
					{Pattern: "Bearer\\s+[A-Za-z0-9._-]+", Replacement: "Bearer [REDACTED]"},
				},
			},
		},
	})

	batch := sinkapi.Batch{
		Events: []schema.CanonicalEvent{
			{
				EventID: "evt-1",
				Context: schema.EventContext{ProjectLocator: "repo://open-agent-stream"},
				Payload: map[string]any{
					"token":  "secret-token",
					"stdout": "Authorization: Bearer abc123",
				},
			},
		},
	}

	filtered, _, err := engine.Apply("stdout", batch)
	if err != nil {
		t.Fatal(err)
	}
	got := filtered.Events[0].Payload
	if got["token"] != "[REDACTED]" {
		t.Fatalf("expected token redacted, got %#v", got["token"])
	}
	if got["stdout"] != "Authorization: Bearer [REDACTED]" {
		t.Fatalf("expected regex redaction, got %#v", got["stdout"])
	}
}

func TestSinkIsolation(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	stateStore, err := state.Open(filepath.Join(tempDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()

	good := &captureSink{id: "good", sinkType: "stdout"}
	bad := &captureSink{id: "bad", sinkType: "webhook", fail: true}
	rt := router.New([]sinkapi.Config{
		{ID: "good", Type: "stdout"},
		{ID: "bad", Type: "webhook"},
	}, []sinkapi.Sink{good, bad}, stateStore, redact.NewEngine(config.Config{MachineID: "test-machine"}))

	event := schema.CanonicalEvent{
		EventVersion:     1,
		EventID:          "evt-1",
		SourceType:       "codex_local",
		SourceInstanceID: "codex-local",
		SessionKey:       "sess-1",
		Sequence:         1,
		Kind:             "message.user",
		Context:          schema.EventContext{SourceType: "codex_local"},
		Payload:          map[string]any{"text": "hello"},
	}
	envelope := schema.RawEnvelope{
		EnvelopeVersion:  1,
		EnvelopeID:       "env-1",
		SourceType:       "codex_local",
		SourceInstanceID: "codex-local",
		ArtifactID:       "art-1",
		Cursor:           schema.Cursor{Kind: "line", Value: "1"},
		ObservedAt:       event.Timestamp,
		RawKind:          "message.user",
		RawPayload:       []byte(`{"text":"hello"}`),
		ContentHash:      "sha256:test",
	}

	err = rt.Route(context.Background(), sinkapi.Batch{
		Events:       []schema.CanonicalEvent{event},
		RawEnvelopes: []schema.RawEnvelope{envelope},
	}, 1, 1)
	if err == nil {
		t.Fatal("expected route error from failing sink")
	}
	if good.calls != 1 {
		t.Fatalf("expected good sink to receive batch, got %d calls", good.calls)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type captureSink struct {
	id       string
	sinkType string
	fail     bool
	calls    int
}

func runtimeFixtureConfig(t *testing.T, dir string) config.Config {
	t.Helper()

	root := repoRoot(t)
	return config.Config{
		Version:    "0.1",
		MachineID:  "test-machine",
		StatePath:  filepath.Join(dir, "state.db"),
		LedgerPath: filepath.Join(dir, "ledger.db"),
		BatchSize:  64,
		Sources: []sourceapi.Config{
			{
				InstanceID: "codex-local",
				Type:       "codex_local",
				Root:       filepath.Join(root, "fixtures", "sources", "codex"),
			},
		},
		Sinks: []sinkapi.Config{
			{
				ID:   "jsonl-local",
				Type: "jsonl",
				Options: map[string]string{
					"path": filepath.Join(dir, "events.jsonl"),
				},
			},
			{
				ID:   "sqlite-local",
				Type: "sqlite",
				Options: map[string]string{
					"path": filepath.Join(dir, "canonical.db"),
				},
			},
		},
	}
}

func mustRuntimeForTest(t *testing.T, cfg config.Config) *supervisor.Runtime {
	t.Helper()

	runtime, err := supervisor.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func closeRuntimeForTest(t *testing.T, runtime *supervisor.Runtime) {
	t.Helper()

	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func countJSONLLines(t *testing.T, path string) int {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		return 0
	}
	return strings.Count(string(data), "\n")
}

func countSQLiteEvents(t *testing.T, path string) int {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(DISTINCT event_id) FROM canonical_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (c *captureSink) ID() string   { return c.id }
func (c *captureSink) Type() string { return c.sinkType }
func (c *captureSink) Init(context.Context) error {
	return nil
}

func (c *captureSink) SendBatch(context.Context, sinkapi.Batch) (sinkapi.Result, error) {
	c.calls++
	if c.fail {
		return sinkapi.Result{}, assertErr("fail")
	}
	return sinkapi.Result{}, nil
}

func (c *captureSink) Flush(context.Context) error  { return nil }
func (c *captureSink) Health(context.Context) error { return nil }
func (c *captureSink) Close(context.Context) error  { return nil }

type assertErr string

func (e assertErr) Error() string { return string(e) }

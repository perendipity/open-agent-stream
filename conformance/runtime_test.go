package conformance

import (
	"context"
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
	"github.com/open-agent-stream/open-agent-stream/pkg/conformance"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	claudesource "github.com/open-agent-stream/open-agent-stream/sources/claude"
	codexsource "github.com/open-agent-stream/open-agent-stream/sources/codex"
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
	rt := router.New([]sinkapi.Sink{good, bad}, stateStore, redact.NewEngine(config.Config{}))

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
	pending, err := stateStore.ListSinkBatches("bad", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending batch, got %d", len(pending))
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

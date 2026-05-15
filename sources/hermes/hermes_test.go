package hermes

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
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

func TestReadValidEmptyDatabaseReturnsNoEnvelopes(t *testing.T) {
	dbPath := createStateDB(t, filepath.Join(t.TempDir(), "state.db"))

	envelopes, checkpoint, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{Cursor: "1"})
	if err != nil {
		t.Fatalf("Read() error = %v, want nil", err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("Read() returned %d envelopes, want 0", len(envelopes))
	}
	if checkpoint.Cursor != "1" {
		t.Fatalf("Read() checkpoint cursor = %q, want 1", checkpoint.Cursor)
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

func TestReadReturnsMissingSessionsTableErrorBeforeUnimplemented(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), messagesTableSchema)

	_, _, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{})
	if err == nil {
		t.Fatal("Read() error = nil, want missing sessions table error")
	}
	if !strings.Contains(err.Error(), "sessions") {
		t.Fatalf("Read() error = %q, want mention missing sessions table", err.Error())
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("Read() error = %q, want schema error before unimplemented", err.Error())
	}
}

func TestReadValidatesHermesSchemaBeforeUnimplemented(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), sessionsTableSchema)

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

func TestReadValidPathWithSpecialCharactersReturnsNoEnvelopes(t *testing.T) {
	dbPath := createStateDB(t, filepath.Join(t.TempDir(), "state [with] spaces & symbols", "state.db"))

	envelopes, _, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("Read() error = %v, want nil", err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("Read() returned %d envelopes, want 0", len(envelopes))
	}
}

func TestReadEmitsRawHermesMessageEnvelopes(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "state.db"))
	db := openTestSQLiteDB(t, dbPath)

	insertHermesSession(t, db, "sess-1", "discord", "gpt-test", "Test Session", "secret system prompt", 100.5)
	insertHermesMessage(t, db, 2, "sess-1", "assistant", "hi back", 124.75, map[string]any{
		"token_count":   7,
		"finish_reason": "stop",
		"reasoning":     "hidden chain",
	})
	insertHermesMessage(t, db, 1, "sess-1", "user", "hello", 123.4, map[string]any{
		"token_count": 3,
		"tool_calls":  `[{"id":"call-1","name":"lookup"}]`,
	})
	insertHermesMessage(t, db, 3, "sess-1", "tool", "tool output", 125.25, map[string]any{
		"tool_call_id": "call-1",
		"tool_name":    "lookup",
	})

	artifact := sourceapi.Artifact{
		ID:             "art-test",
		Locator:        dbPath,
		ProjectLocator: root,
	}
	cfg := sourceapi.Config{InstanceID: "instance-1"}
	envelopes, checkpoint, err := New().Read(context.Background(), cfg, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(envelopes) != 3 {
		t.Fatalf("Read() returned %d envelopes, want 3: %#v", len(envelopes), envelopes)
	}
	if checkpoint.Cursor != "3" {
		t.Fatalf("checkpoint.Cursor = %q, want 3", checkpoint.Cursor)
	}

	wantKinds := []string{"message.user", "message.assistant", "message.tool"}
	for i, envelope := range envelopes {
		wantID := strconv.Itoa(i + 1)
		if envelope.EnvelopeVersion != schema.RawEnvelopeVersion {
			t.Fatalf("envelope[%d].EnvelopeVersion = %d, want %d", i, envelope.EnvelopeVersion, schema.RawEnvelopeVersion)
		}
		if envelope.SourceType != "hermes_local" || envelope.SourceInstanceID != cfg.InstanceID {
			t.Fatalf("envelope[%d] source = %q/%q, want hermes_local/%q", i, envelope.SourceType, envelope.SourceInstanceID, cfg.InstanceID)
		}
		if envelope.ArtifactID != artifact.ID || envelope.ArtifactLocator != artifact.Locator || envelope.ProjectLocator != artifact.ProjectLocator {
			t.Fatalf("envelope[%d] artifact fields = %#v, want %#v", i, envelope, artifact)
		}
		if envelope.RawKind != wantKinds[i] {
			t.Fatalf("envelope[%d].RawKind = %q, want %q", i, envelope.RawKind, wantKinds[i])
		}
		if envelope.Cursor.Kind != "message_id" || envelope.Cursor.Value != wantID {
			t.Fatalf("envelope[%d].Cursor = %#v, want message_id/%s", i, envelope.Cursor, wantID)
		}
		if envelope.ParseHints.SourceSessionKey != "sess-1" || envelope.ParseHints.ProjectHint != root {
			t.Fatalf("envelope[%d].ParseHints = %#v", i, envelope.ParseHints)
		}
		if envelope.SourceTimestamp == nil || envelope.SourceTimestamp.Location() != time.UTC {
			t.Fatalf("envelope[%d].SourceTimestamp = %#v, want UTC timestamp", i, envelope.SourceTimestamp)
		}
		if envelope.ContentHash == "" || envelope.EnvelopeID == "" {
			t.Fatalf("envelope[%d] ContentHash/EnvelopeID empty: %q/%q", i, envelope.ContentHash, envelope.EnvelopeID)
		}
	}
	if !envelopes[0].SourceTimestamp.Equal(time.Unix(123, 400_000_000).UTC()) {
		t.Fatalf("first SourceTimestamp = %s, want 123.4 epoch UTC", envelopes[0].SourceTimestamp.Format(time.RFC3339Nano))
	}

	var payload struct {
		Message map[string]any `json:"message"`
		Session map[string]any `json:"session"`
	}
	if err := json.Unmarshal(envelopes[0].RawPayload, &payload); err != nil {
		t.Fatalf("unmarshal first raw payload: %v", err)
	}
	if payload.Message["id"] != float64(1) || payload.Message["session_id"] != "sess-1" || payload.Message["role"] != "user" || payload.Message["content"] != "hello" {
		t.Fatalf("first payload message = %#v", payload.Message)
	}
	if payload.Message["timestamp"] != 123.4 || payload.Message["token_count"] != float64(3) || payload.Message["tool_calls"] == nil {
		t.Fatalf("first payload missing timestamp/token/tool fields: %#v", payload.Message)
	}
	if payload.Session["id"] != "sess-1" || payload.Session["source"] != "discord" || payload.Session["model"] != "gpt-test" || payload.Session["title"] != "Test Session" {
		t.Fatalf("first payload session = %#v", payload.Session)
	}
	if _, ok := payload.Session["system_prompt"]; ok {
		t.Fatalf("session system_prompt included by default: %#v", payload.Session)
	}

	fromCursor, cursorCheckpoint, err := New().Read(context.Background(), cfg, artifact, sourceapi.Checkpoint{Cursor: "1"})
	if err != nil {
		t.Fatalf("cursor Read() error = %v", err)
	}
	if len(fromCursor) != 2 || fromCursor[0].Cursor.Value != "2" || cursorCheckpoint.Cursor != "3" {
		t.Fatalf("cursor Read() got %d envelopes first cursor %q checkpoint %q, want 2/2/3", len(fromCursor), fromCursor[0].Cursor.Value, cursorCheckpoint.Cursor)
	}

	firstID, firstHash := envelopes[0].EnvelopeID, envelopes[0].ContentHash
	envelopesAgain, _, err := New().Read(context.Background(), cfg, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("second Read() error = %v", err)
	}
	if envelopesAgain[0].EnvelopeID != firstID || envelopesAgain[0].ContentHash != firstHash {
		t.Fatalf("stable ids changed: got %q/%q want %q/%q", envelopesAgain[0].EnvelopeID, envelopesAgain[0].ContentHash, firstID, firstHash)
	}
}

func TestReadCheckpointIsArtifactSafeAndIncremental(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "state.db"))
	db := openTestSQLiteDB(t, dbPath)
	insertHermesSession(t, db, "sess-1", "discord", "gpt-test", "Test Session", "secret system prompt", 100.5)
	insertHermesMessage(t, db, 1, "sess-1", "user", "one", 101, nil)
	insertHermesMessage(t, db, 2, "sess-1", "assistant", "two", 102, nil)

	artifact := discoveredArtifactForPath(t, root, dbPath)
	cfg := sourceapi.Config{InstanceID: "instance-1"}
	adapter := New()

	first, checkpoint, err := adapter.Read(context.Background(), cfg, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("first Read() returned %d envelopes, want 2", len(first))
	}
	assertCheckpoint(t, checkpoint, "2", artifact.Fingerprint, dbPath)

	second, secondCheckpoint, err := adapter.Read(context.Background(), cfg, artifact, checkpoint)
	if err != nil {
		t.Fatalf("second Read() error = %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second Read() returned %d envelopes, want 0", len(second))
	}
	assertCheckpoint(t, secondCheckpoint, "2", artifact.Fingerprint, dbPath)

	insertHermesMessage(t, db, 3, "sess-1", "user", "three", 103, nil)
	afterAppend, appendCheckpoint, err := adapter.Read(context.Background(), cfg, artifact, secondCheckpoint)
	if err != nil {
		t.Fatalf("append Read() error = %v", err)
	}
	if len(afterAppend) != 1 || afterAppend[0].Cursor.Value != "3" {
		t.Fatalf("append Read() got %d envelopes cursor %q, want one cursor 3", len(afterAppend), firstCursorValue(afterAppend))
	}
	assertCheckpoint(t, appendCheckpoint, "3", artifact.Fingerprint, dbPath)

	otherRoot := t.TempDir()
	otherDBPath := createStateDB(t, filepath.Join(otherRoot, "state.db"))
	otherDB := openTestSQLiteDB(t, otherDBPath)
	insertHermesSession(t, otherDB, "sess-other", "discord", "gpt-test", "Other", "prompt", 200)
	insertHermesMessage(t, otherDB, 1, "sess-other", "user", "other one", 201, nil)
	otherArtifact := discoveredArtifactForPath(t, otherRoot, otherDBPath)
	crossArtifact, crossCheckpoint, err := adapter.Read(context.Background(), cfg, otherArtifact, appendCheckpoint)
	if err != nil {
		t.Fatalf("cross-artifact Read() error = %v", err)
	}
	if len(crossArtifact) != 1 || crossArtifact[0].Cursor.Value != "1" {
		t.Fatalf("cross-artifact Read() got %d envelopes cursor %q, want one cursor 1", len(crossArtifact), firstCursorValue(crossArtifact))
	}
	assertCheckpoint(t, crossCheckpoint, "1", otherArtifact.Fingerprint, otherDBPath)
}

func TestReadInvalidCheckpointCursorReturnsError(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "state.db"))
	artifact := discoveredArtifactForPath(t, root, dbPath)

	_, _, err := New().Read(context.Background(), sourceapi.Config{}, artifact, sourceapi.Checkpoint{Cursor: "not-an-int"})
	if err == nil {
		t.Fatal("Read() error = nil, want invalid cursor error")
	}
	if !strings.Contains(err.Error(), "checkpoint cursor") || !strings.Contains(err.Error(), "not-an-int") {
		t.Fatalf("Read() error = %q, want clear checkpoint cursor parse error", err.Error())
	}
}

func TestReadNullHeavyMessageWithoutJoinedSessionUsesUnknownKind(t *testing.T) {
	dbPath := createStateDB(t, filepath.Join(t.TempDir(), "state.db"))
	db := openTestSQLiteDB(t, dbPath)
	if _, err := db.Exec(`INSERT INTO messages (id, session_id, role, content, timestamp) VALUES (1, 'missing-session', '', NULL, NULL)`); err != nil {
		t.Fatal(err)
	}

	envelopes, _, err := New().Read(context.Background(), sourceapi.Config{}, sourceapi.Artifact{Locator: dbPath}, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("Read() returned %d envelopes, want 1", len(envelopes))
	}
	if envelopes[0].RawKind != "message.unknown" {
		t.Fatalf("RawKind = %q, want message.unknown", envelopes[0].RawKind)
	}
	if envelopes[0].SourceTimestamp != nil {
		t.Fatalf("SourceTimestamp = %#v, want nil", envelopes[0].SourceTimestamp)
	}
	payload := decodeRawPayload(t, envelopes[0])
	if payload["session"] != nil {
		t.Fatalf("payload session = %#v, want omitted for missing joined session", payload["session"])
	}
	message := payload["message"].(map[string]any)
	if message["id"] != float64(1) || message["session_id"] != "missing-session" || message["role"] != "" {
		t.Fatalf("payload message = %#v, want id/session_id/empty role", message)
	}
	if _, ok := message["content"]; ok {
		t.Fatalf("payload message included NULL content: %#v", message)
	}
}

func TestReadPrivacyOptionsControlRawPayloadFields(t *testing.T) {
	root := t.TempDir()
	dbPath := createStateDB(t, filepath.Join(root, "state.db"))
	db := openTestSQLiteDB(t, dbPath)
	insertHermesSession(t, db, "sess-1", "discord", "gpt-test", "Privacy", "secret system prompt", 100)
	insertHermesMessage(t, db, 1, "sess-1", "assistant", "answer", 101, map[string]any{
		"reasoning":             "hidden reasoning",
		"reasoning_content":     "hidden content",
		"reasoning_details":     `[{"type":"summary"}]`,
		"codex_reasoning_items": `[{"text":"think"}]`,
		"codex_message_items":   `[{"text":"answer"}]`,
	})
	insertHermesMessage(t, db, 2, "sess-1", "tool", "raw tool output", 102, map[string]any{
		"tool_call_id": "call-1",
		"tool_calls":   `[{"id":"call-1"}]`,
		"tool_name":    "lookup",
	})
	artifact := sourceapi.Artifact{ID: "art", Locator: dbPath, ProjectLocator: root}

	defaultEnvelopes, _, err := New().Read(context.Background(), sourceapi.Config{}, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("default Read() error = %v", err)
	}
	assistantDefault := decodeRawPayload(t, defaultEnvelopes[0])
	defaultMessage := assistantDefault["message"].(map[string]any)
	defaultSession := assistantDefault["session"].(map[string]any)
	if _, ok := defaultSession["system_prompt"]; ok {
		t.Fatalf("default payload included system_prompt: %#v", defaultSession)
	}
	for _, key := range []string{"reasoning", "reasoning_content", "reasoning_details", "codex_reasoning_items"} {
		if _, ok := defaultMessage[key]; ok {
			t.Fatalf("default payload included reasoning key %s: %#v", key, defaultMessage)
		}
	}
	defaultTool := decodeRawPayload(t, defaultEnvelopes[1])["message"].(map[string]any)
	if defaultTool["content"] != "raw tool output" || defaultTool["tool_call_id"] != "call-1" || defaultTool["tool_name"] != "lookup" || defaultTool["tool_calls"] == nil {
		t.Fatalf("default tool payload did not preserve raw tool fields: %#v", defaultTool)
	}

	withSystemPrompt, _, err := New().Read(context.Background(), sourceapi.Config{Options: map[string]string{"include_system_prompt": "true"}}, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("include_system_prompt Read() error = %v", err)
	}
	if got := decodeRawPayload(t, withSystemPrompt[0])["session"].(map[string]any)["system_prompt"]; got != "secret system prompt" {
		t.Fatalf("system_prompt = %#v, want secret system prompt", got)
	}

	withReasoning, _, err := New().Read(context.Background(), sourceapi.Config{Options: map[string]string{"include_reasoning": "true"}}, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("include_reasoning Read() error = %v", err)
	}
	reasoningMessage := decodeRawPayload(t, withReasoning[0])["message"].(map[string]any)
	for _, key := range []string{"reasoning", "reasoning_content", "reasoning_details", "codex_reasoning_items"} {
		if reasoningMessage[key] == nil {
			t.Fatalf("include_reasoning payload missing %s: %#v", key, reasoningMessage)
		}
	}

	withoutToolOutput, _, err := New().Read(context.Background(), sourceapi.Config{Options: map[string]string{"include_raw_tool_output": "false"}}, artifact, sourceapi.Checkpoint{})
	if err != nil {
		t.Fatalf("include_raw_tool_output=false Read() error = %v", err)
	}
	suppressedTool := decodeRawPayload(t, withoutToolOutput[1])["message"].(map[string]any)
	if suppressedTool["id"] != float64(2) || suppressedTool["role"] != "tool" || suppressedTool["session_id"] != "sess-1" {
		t.Fatalf("suppressed tool payload lost event structure: %#v", suppressedTool)
	}
	for _, key := range []string{"content", "tool_call_id", "tool_calls", "tool_name"} {
		if _, ok := suppressedTool[key]; ok {
			t.Fatalf("include_raw_tool_output=false included %s: %#v", key, suppressedTool)
		}
	}
}

func TestValidateHermesSchemaReportsMissingSessionsColumn(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), `
CREATE TABLE sessions (
	id TEXT PRIMARY KEY,
	source TEXT
);
`+messagesTableSchema)
	db := openTestSQLiteDB(t, dbPath)

	err := validateHermesSchema(context.Background(), db)
	if err == nil {
		t.Fatal("validateHermesSchema() error = nil, want missing column error")
	}
	if !strings.Contains(err.Error(), "model") {
		t.Fatalf("validateHermesSchema() error = %q, want mention missing model column", err.Error())
	}
}

func TestValidateHermesSchemaRequiresRealTables(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), `
CREATE TABLE sessions_base (
	id TEXT PRIMARY KEY,
	source TEXT,
	model TEXT,
	system_prompt TEXT,
	started_at REAL,
	ended_at REAL,
	title TEXT,
	input_tokens INTEGER,
	output_tokens INTEGER,
	reasoning_tokens INTEGER,
	estimated_cost_usd REAL,
	actual_cost_usd REAL
);
CREATE VIEW sessions AS SELECT * FROM sessions_base;
`+messagesTableSchema)
	db := openTestSQLiteDB(t, dbPath)

	err := validateHermesSchema(context.Background(), db)
	if err == nil {
		t.Fatal("validateHermesSchema() error = nil, want missing table error for view")
	}
	if !strings.Contains(err.Error(), "sessions") || !strings.Contains(err.Error(), "table") {
		t.Fatalf("validateHermesSchema() error = %q, want mention missing sessions table", err.Error())
	}
}

func TestValidateHermesSchemaReportsMissingMessagesColumn(t *testing.T) {
	dbPath := createSQLiteDB(t, filepath.Join(t.TempDir(), "state.db"), sessionsTableSchema+`
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

func insertHermesSession(t *testing.T, db *sql.DB, id, source, model, title, systemPrompt string, startedAt float64) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO sessions (id, source, model, system_prompt, started_at, title, input_tokens, output_tokens, reasoning_tokens, estimated_cost_usd, actual_cost_usd)
VALUES (?, ?, ?, ?, ?, ?, 11, 13, 0, 0.01, 0.02)`, id, source, model, systemPrompt, startedAt, title)
	if err != nil {
		t.Fatal(err)
	}
}

func insertHermesMessage(t *testing.T, db *sql.DB, id int64, sessionID, role, content string, timestamp float64, fields map[string]any) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO messages (id, session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp, token_count, finish_reason, reasoning, reasoning_content, reasoning_details, codex_reasoning_items, codex_message_items)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		sessionID,
		role,
		content,
		fields["tool_call_id"],
		fields["tool_calls"],
		fields["tool_name"],
		timestamp,
		fields["token_count"],
		fields["finish_reason"],
		fields["reasoning"],
		fields["reasoning_content"],
		fields["reasoning_details"],
		fields["codex_reasoning_items"],
		fields["codex_message_items"],
	)
	if err != nil {
		t.Fatal(err)
	}
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

const sessionsTableSchema = `
CREATE TABLE sessions (
	id TEXT PRIMARY KEY,
	source TEXT,
	model TEXT,
	system_prompt TEXT,
	started_at REAL,
	ended_at REAL,
	title TEXT,
	input_tokens INTEGER,
	output_tokens INTEGER,
	reasoning_tokens INTEGER,
	estimated_cost_usd REAL,
	actual_cost_usd REAL
);
`

const messagesTableSchema = `
CREATE TABLE messages (
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
);
`

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

func discoveredArtifactForPath(t *testing.T, root, dbPath string) sourceapi.Artifact {
	t.Helper()
	artifact, ok, err := artifactForDB(root, "default", dbPath, false)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("artifactForDB(%q) ok = false, want true", dbPath)
	}
	return artifact
}

func decodeRawPayload(t *testing.T, envelope schema.RawEnvelope) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(envelope.RawPayload, &payload); err != nil {
		t.Fatalf("unmarshal raw payload: %v", err)
	}
	return payload
}

func firstCursorValue(envelopes []schema.RawEnvelope) string {
	if len(envelopes) == 0 {
		return ""
	}
	return envelopes[0].Cursor.Value
}

func assertCheckpoint(t *testing.T, checkpoint sourceapi.Checkpoint, cursor, fingerprint, dbPath string) {
	t.Helper()
	if checkpoint.Cursor != cursor {
		t.Fatalf("checkpoint.Cursor = %q, want %q", checkpoint.Cursor, cursor)
	}
	if checkpoint.ArtifactFingerprint != fingerprint {
		t.Fatalf("checkpoint.ArtifactFingerprint = %q, want %q", checkpoint.ArtifactFingerprint, fingerprint)
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.LastObservedSize != info.Size() {
		t.Fatalf("checkpoint.LastObservedSize = %d, want %d", checkpoint.LastObservedSize, info.Size())
	}
	if !checkpoint.LastObservedModTime.Equal(info.ModTime().UTC()) {
		t.Fatalf("checkpoint.LastObservedModTime = %s, want %s", checkpoint.LastObservedModTime.Format(time.RFC3339Nano), info.ModTime().UTC().Format(time.RFC3339Nano))
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

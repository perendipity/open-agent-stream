package hermes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	_ "modernc.org/sqlite"
)

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() string {
	return "hermes_local"
}

func (a *Adapter) Capabilities() []sourceapi.Capability {
	return []sourceapi.Capability{
		sourceapi.CapabilityMessages,
		sourceapi.CapabilityCommands,
		sourceapi.CapabilityToolCalls,
		sourceapi.CapabilityFileOps,
		sourceapi.CapabilityUsage,
	}
}

func capabilitiesToStrings(values []sourceapi.Capability) []string {
	out := make([]string, len(values))
	for idx := range values {
		out[idx] = string(values[idx])
	}
	return out
}

func (a *Adapter) Discover(_ context.Context, cfg sourceapi.Config) ([]sourceapi.Artifact, error) {
	return discoverDBArtifacts(cfg.Root, cfg.Options)
}

func discoverDBArtifacts(root string, options map[string]string) ([]sourceapi.Artifact, error) {
	resolvedRoot := expandPath(root)
	if options == nil {
		options = map[string]string{}
	}

	if dbPath := strings.TrimSpace(options["db_path"]); dbPath != "" {
		artifact, ok, err := artifactForDB(resolvedRoot, "default", expandPath(dbPath), false)
		if err != nil || !ok {
			return nil, err
		}
		return []sourceapi.Artifact{artifact}, nil
	}

	var artifacts []sourceapi.Artifact
	if artifact, ok, err := artifactForDB(resolvedRoot, "default", filepath.Join(resolvedRoot, "state.db"), true); err != nil {
		return nil, err
	} else if ok {
		artifacts = append(artifacts, artifact)
	}

	profilesOption := strings.TrimSpace(options["profiles"])
	switch {
	case strings.EqualFold(profilesOption, "all"):
		entries, err := os.ReadDir(filepath.Join(resolvedRoot, "profiles"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, entry := range entries {
			profile := entry.Name()
			if !entry.IsDir() || !isProfileName(profile) {
				continue
			}
			path := filepath.Join(resolvedRoot, "profiles", profile, "state.db")
			artifact, ok, err := artifactForDB(resolvedRoot, profile, path, true)
			if err != nil {
				return nil, err
			}
			if ok {
				artifacts = append(artifacts, artifact)
			}
		}
	case profilesOption != "":
		seen := make(map[string]struct{})
		for _, profile := range splitProfiles(profilesOption) {
			if !isProfileName(profile) {
				continue
			}
			if _, ok := seen[profile]; ok {
				continue
			}
			seen[profile] = struct{}{}
			path := filepath.Join(resolvedRoot, "profiles", profile, "state.db")
			artifact, ok, err := artifactForDB(resolvedRoot, profile, path, true)
			if err != nil {
				return nil, err
			}
			if ok {
				artifacts = append(artifacts, artifact)
			}
		}
	}

	return artifacts, nil
}

func artifactForDB(root, profile, path string, enforceUnderRoot bool) (sourceapi.Artifact, bool, error) {
	resolvedPath := expandPath(path)
	info, err := os.Lstat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sourceapi.Artifact{}, false, nil
		}
		return sourceapi.Artifact{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		if enforceUnderRoot {
			return sourceapi.Artifact{}, false, nil
		}
		info, err = os.Stat(resolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return sourceapi.Artifact{}, false, nil
			}
			return sourceapi.Artifact{}, false, err
		}
	}
	if info.IsDir() {
		return sourceapi.Artifact{}, false, nil
	}
	if enforceUnderRoot && !isPathWithinRoot(root, resolvedPath) {
		return sourceapi.Artifact{}, false, nil
	}
	if profile == "" {
		profile = "default"
	}
	return sourceapi.Artifact{
		ID:             schema.StableID("art", "hermes", profile, resolvedPath),
		Locator:        resolvedPath,
		ProjectLocator: root,
		Fingerprint:    schema.StableID("fp", resolvedPath, strconv.FormatInt(info.Size(), 10), info.ModTime().UTC().Format(time.RFC3339Nano)),
		Metadata: map[string]string{
			"profile": profile,
		},
	}, true, nil
}

func expandPath(path string) string {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			switch {
			case path == "~":
				path = home
			case strings.HasPrefix(path, "~/"):
				path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
			}
		}
	}
	if resolved, err := filepath.Abs(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}

func splitProfiles(value string) []string {
	parts := strings.Split(value, ",")
	profiles := make([]string, 0, len(parts))
	for _, part := range parts {
		profile := strings.TrimSpace(part)
		if profile != "" {
			profiles = append(profiles, profile)
		}
	}
	return profiles
}

func isProfileName(profile string) bool {
	return profile != "" && profile != "." && profile != ".." && filepath.Base(profile) == profile && !strings.ContainsAny(profile, `/\\`)
}

func isPathWithinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

type readOptions struct {
	includeSystemPrompt  bool
	includeReasoning     bool
	includeRawToolOutput bool
}

func parseReadOptions(options map[string]string) readOptions {
	return readOptions{
		includeSystemPrompt:  boolOption(options, "include_system_prompt", false),
		includeReasoning:     boolOption(options, "include_reasoning", false),
		includeRawToolOutput: boolOption(options, "include_raw_tool_output", true),
	}
}

func boolOption(options map[string]string, key string, defaultValue bool) bool {
	value := strings.TrimSpace(strings.ToLower(options[key]))
	switch value {
	case "true", "1", "yes", "y", "on":
		return true
	case "false", "0", "no", "n", "off":
		return false
	default:
		return defaultValue
	}
}

func validateHermesSchema(ctx context.Context, db *sql.DB) error {
	if err := validateHermesTable(ctx, db, "sessions", requiredSessionsColumns()); err != nil {
		return err
	}
	if err := validateHermesTable(ctx, db, "messages", requiredMessagesColumns()); err != nil {
		return err
	}
	return nil
}

func validateHermesTable(ctx context.Context, db *sql.DB, table string, requiredColumns []string) error {
	var tableName string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?", table).Scan(&tableName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("hermes SQLite schema missing required table %s", table)
		}
		return fmt.Errorf("invalid Hermes SQLite database: inspect %s table: %w", table, err)
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT name FROM pragma_table_info(%q)", table))
	if err != nil {
		return fmt.Errorf("invalid Hermes SQLite database: inspect %s columns: %w", table, err)
	}
	defer rows.Close()

	columns := make(map[string]struct{})
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return fmt.Errorf("invalid Hermes SQLite database: scan %s column: %w", table, err)
		}
		columns[column] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("invalid Hermes SQLite database: inspect %s columns: %w", table, err)
	}

	for _, column := range requiredColumns {
		if _, ok := columns[column]; !ok {
			return fmt.Errorf("hermes SQLite schema missing required %s column %s", table, column)
		}
	}
	return nil
}

func requiredSessionsColumns() []string {
	return []string{
		"id",
		"source",
		"model",
		"system_prompt",
		"started_at",
		"ended_at",
		"title",
		"input_tokens",
		"output_tokens",
		"reasoning_tokens",
		"estimated_cost_usd",
		"actual_cost_usd",
	}
}

func requiredMessagesColumns() []string {
	return []string{
		"id",
		"session_id",
		"role",
		"content",
		"tool_call_id",
		"tool_calls",
		"tool_name",
		"timestamp",
		"token_count",
		"finish_reason",
		"reasoning",
		"reasoning_content",
		"reasoning_details",
		"codex_reasoning_items",
		"codex_message_items",
	}
}

func (a *Adapter) Read(ctx context.Context, cfg sourceapi.Config, artifact sourceapi.Artifact, checkpoint sourceapi.Checkpoint) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	options := parseReadOptions(cfg.Options)
	artifactInfo, err := os.Stat(expandPath(artifact.Locator))
	if err != nil {
		return nil, checkpoint, fmt.Errorf("stat Hermes SQLite database: %w", err)
	}

	dbURL := url.URL{Scheme: "file", Path: expandPath(artifact.Locator)}
	query := dbURL.Query()
	query.Set("mode", "ro")
	dbURL.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", dbURL.String())
	if err != nil {
		return nil, checkpoint, fmt.Errorf("open Hermes SQLite database read-only: %w", err)
	}
	defer db.Close()

	if err := validateHermesSchema(ctx, db); err != nil {
		return nil, checkpoint, err
	}

	lastMessageID := int64(0)
	artifactCheckpointToken := artifact.ID
	checkpointCursor := strings.TrimSpace(checkpoint.Cursor)
	useCheckpointCursor := checkpointCursor != "" && artifactCheckpointToken != "" && checkpoint.ArtifactFingerprint == artifactCheckpointToken
	if checkpointCursor != "" && checkpoint.ArtifactFingerprint == "" {
		return nil, checkpoint, fmt.Errorf("Hermes checkpoint cursor %q is missing artifact token", checkpoint.Cursor)
	}
	if useCheckpointCursor {
		parsed, err := strconv.ParseInt(checkpointCursor, 10, 64)
		if err != nil {
			return nil, checkpoint, fmt.Errorf("parse Hermes checkpoint cursor %q as message id: %w", checkpoint.Cursor, err)
		}
		lastMessageID = parsed
	}

	rows, err := db.QueryContext(ctx, `
SELECT
	m.id, m.session_id, m.role, m.content, m.tool_call_id, m.tool_calls, m.tool_name,
	m.timestamp, m.token_count, m.finish_reason, m.reasoning, m.reasoning_content,
	m.reasoning_details, m.codex_reasoning_items, m.codex_message_items,
	s.id, s.source, s.model, s.system_prompt, s.started_at, s.ended_at, s.title,
	s.input_tokens, s.output_tokens, s.reasoning_tokens, s.estimated_cost_usd, s.actual_cost_usd
FROM messages m
LEFT JOIN sessions s ON s.id = m.session_id
WHERE m.id > ?
ORDER BY m.id ASC`, lastMessageID)
	if err != nil {
		return nil, checkpoint, fmt.Errorf("query Hermes messages: %w", err)
	}
	defer rows.Close()

	observedAt := time.Now().UTC()
	envelopes := []schema.RawEnvelope{}
	latestMessageID := lastMessageID
	capabilities := capabilitiesToStrings(a.Capabilities())
	for rows.Next() {
		var row hermesMessageRow
		if err := rows.Scan(
			&row.id,
			&row.sessionID,
			&row.role,
			&row.content,
			&row.toolCallID,
			&row.toolCalls,
			&row.toolName,
			&row.timestamp,
			&row.tokenCount,
			&row.finishReason,
			&row.reasoning,
			&row.reasoningContent,
			&row.reasoningDetails,
			&row.codexReasoningItems,
			&row.codexMessageItems,
			&row.session.id,
			&row.session.source,
			&row.session.model,
			&row.session.systemPrompt,
			&row.session.startedAt,
			&row.session.endedAt,
			&row.session.title,
			&row.session.inputTokens,
			&row.session.outputTokens,
			&row.session.reasoningTokens,
			&row.session.estimatedCostUSD,
			&row.session.actualCostUSD,
		); err != nil {
			return nil, checkpoint, fmt.Errorf("scan Hermes message: %w", err)
		}

		rawPayload, err := json.Marshal(buildHermesRawPayload(row, options))
		if err != nil {
			return nil, checkpoint, fmt.Errorf("marshal Hermes raw payload for message %d: %w", row.id, err)
		}

		var sourceTimestamp *time.Time
		if row.timestamp.Valid {
			timestamp := hermesEpochSecondsToTime(row.timestamp.Float64)
			sourceTimestamp = &timestamp
		}

		envelope := schema.RawEnvelope{
			EnvelopeVersion:  schema.RawEnvelopeVersion,
			SourceType:       a.Type(),
			SourceInstanceID: cfg.InstanceID,
			ArtifactID:       artifact.ID,
			ArtifactLocator:  artifact.Locator,
			ProjectLocator:   artifact.ProjectLocator,
			Cursor: schema.Cursor{
				Kind:  "message_id",
				Value: strconv.FormatInt(row.id, 10),
			},
			ObservedAt:      observedAt,
			SourceTimestamp: sourceTimestamp,
			RawKind:         hermesRawKind(row.role),
			RawPayload:      rawPayload,
			ContentHash:     schema.HashBytes(rawPayload),
			ParseHints: schema.ParseHints{
				SourceSessionKey: nullStringValue(row.sessionID),
				ProjectHint:      artifact.ProjectLocator,
				Capabilities:     append([]string(nil), capabilities...),
			},
		}
		envelopes = append(envelopes, schema.EnsureEnvelopeID(envelope))
		latestMessageID = row.id
	}
	if err := rows.Err(); err != nil {
		return nil, checkpoint, fmt.Errorf("iterate Hermes messages: %w", err)
	}

	nextCheckpoint := checkpoint
	if !useCheckpointCursor {
		nextCheckpoint = sourceapi.Checkpoint{}
	}
	if latestMessageID != lastMessageID {
		nextCheckpoint.Cursor = strconv.FormatInt(latestMessageID, 10)
	}
	nextCheckpoint.ArtifactFingerprint = artifactCheckpointToken
	nextCheckpoint.LastObservedSize = artifactInfo.Size()
	nextCheckpoint.LastObservedModTime = artifactInfo.ModTime().UTC()
	return envelopes, nextCheckpoint, nil
}

type hermesMessageRow struct {
	id                  int64
	sessionID           sql.NullString
	role                sql.NullString
	content             sql.NullString
	toolCallID          sql.NullString
	toolCalls           sql.NullString
	toolName            sql.NullString
	timestamp           sql.NullFloat64
	tokenCount          sql.NullInt64
	finishReason        sql.NullString
	reasoning           sql.NullString
	reasoningContent    sql.NullString
	reasoningDetails    sql.NullString
	codexReasoningItems sql.NullString
	codexMessageItems   sql.NullString
	session             hermesSessionRow
}

type hermesSessionRow struct {
	id               sql.NullString
	source           sql.NullString
	model            sql.NullString
	systemPrompt     sql.NullString
	startedAt        sql.NullFloat64
	endedAt          sql.NullFloat64
	title            sql.NullString
	inputTokens      sql.NullInt64
	outputTokens     sql.NullInt64
	reasoningTokens  sql.NullInt64
	estimatedCostUSD sql.NullFloat64
	actualCostUSD    sql.NullFloat64
}

func buildHermesRawPayload(row hermesMessageRow, options readOptions) map[string]any {
	message := map[string]any{"id": row.id}
	putString(message, "session_id", row.sessionID)
	putString(message, "role", row.role)
	isToolMessage := strings.EqualFold(nullStringValue(row.role), "tool")
	includeToolOutput := options.includeRawToolOutput || !isToolMessage
	if includeToolOutput {
		putString(message, "content", row.content)
		putString(message, "tool_call_id", row.toolCallID)
		putString(message, "tool_calls", row.toolCalls)
		putString(message, "tool_name", row.toolName)
	}
	putFloat(message, "timestamp", row.timestamp)
	putInt(message, "token_count", row.tokenCount)
	putString(message, "finish_reason", row.finishReason)
	if options.includeReasoning {
		putString(message, "reasoning", row.reasoning)
		putString(message, "reasoning_content", row.reasoningContent)
		putString(message, "reasoning_details", row.reasoningDetails)
		putString(message, "codex_reasoning_items", row.codexReasoningItems)
	}
	if includeToolOutput {
		putString(message, "codex_message_items", row.codexMessageItems)
	}

	session := map[string]any{}
	putString(session, "id", row.session.id)
	putString(session, "source", row.session.source)
	putString(session, "model", row.session.model)
	putFloat(session, "started_at", row.session.startedAt)
	putFloat(session, "ended_at", row.session.endedAt)
	putString(session, "title", row.session.title)
	putInt(session, "input_tokens", row.session.inputTokens)
	putInt(session, "output_tokens", row.session.outputTokens)
	putInt(session, "reasoning_tokens", row.session.reasoningTokens)
	putFloat(session, "estimated_cost_usd", row.session.estimatedCostUSD)
	putFloat(session, "actual_cost_usd", row.session.actualCostUSD)
	if options.includeSystemPrompt {
		putString(session, "system_prompt", row.session.systemPrompt)
	}

	payload := map[string]any{"message": message}
	if len(session) > 0 {
		payload["session"] = session
	}
	return payload
}

func hermesRawKind(role sql.NullString) string {
	roleValue := strings.TrimSpace(nullStringValue(role))
	if roleValue == "" {
		roleValue = "unknown"
	}
	return "message." + roleValue
}

func putString(payload map[string]any, key string, value sql.NullString) {
	if value.Valid {
		payload[key] = value.String
	}
}

func putInt(payload map[string]any, key string, value sql.NullInt64) {
	if value.Valid {
		payload[key] = value.Int64
	}
}

func putFloat(payload map[string]any, key string, value sql.NullFloat64) {
	if value.Valid {
		payload[key] = value.Float64
	}
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func hermesEpochSecondsToTime(seconds float64) time.Time {
	whole, fractional := math.Modf(seconds)
	nanos := int64(math.Round(fractional * 1_000_000_000))
	return time.Unix(int64(whole), nanos).UTC()
}

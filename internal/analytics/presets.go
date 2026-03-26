package analytics

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	analyticsSensitivePolicyID = "analytics_local_sensitive"
	previewMaxChars            = 96
)

type presetSpec struct {
	Name              string
	Aliases           []string
	SupportsSensitive bool
	DefaultLimit      int
	Run               func(context.Context, *sql.DB, Status, int, time.Time) (presetResult, error)
}

type presetResult struct {
	Columns []string
	Rows    []map[string]any
}

func (r presetResult) queryRows() ([]string, [][]any) {
	rows := make([][]any, 0, len(r.Rows))
	for _, item := range r.Rows {
		values := make([]any, 0, len(r.Columns))
		for _, column := range r.Columns {
			values = append(values, item[column])
		}
		rows = append(rows, values)
	}
	return append([]string(nil), r.Columns...), rows
}

var presetSpecs = []presetSpec{
	{
		Name:         "overview",
		DefaultLimit: 1,
		Run:          buildOverviewPreset,
	},
	{
		Name:              "attention",
		Aliases:           []string{"failures"},
		SupportsSensitive: true,
		DefaultLimit:      20,
		Run:               buildAttentionPreset,
	},
	{
		Name:              "recent_sessions",
		Aliases:           []string{"sessions"},
		SupportsSensitive: true,
		DefaultLimit:      20,
		Run:               buildRecentSessionsPreset,
	},
	{
		Name:         "projects",
		DefaultLimit: 20,
		Run:          buildProjectsPreset,
	},
	{
		Name:         "sources",
		DefaultLimit: 20,
		Run:          buildSourcesPreset,
	},
	{
		Name:         "command_health",
		DefaultLimit: 20,
		Run:          buildCommandHealthPreset,
	},
	{
		Name:         "timeline",
		Aliases:      []string{"activity"},
		DefaultLimit: 30,
		Run:          buildTimelinePreset,
	},
	{
		Name:         "coverage",
		DefaultLimit: 1,
		Run:          buildCoveragePreset,
	},
}

func resolvePreset(name string) (presetSpec, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "overview"
	}
	for _, spec := range presetSpecs {
		if name == spec.Name {
			return spec, nil
		}
		for _, alias := range spec.Aliases {
			if name == alias {
				return spec, nil
			}
		}
	}
	return presetSpec{}, fmt.Errorf(
		"unknown analytics preset %q (supported: %s; aliases: sessions, failures, activity)",
		name,
		strings.Join(canonicalPresetNames(), ", "),
	)
}

func canonicalPresetNames() []string {
	names := make([]string, 0, len(presetSpecs))
	for _, spec := range presetSpecs {
		names = append(names, spec.Name)
	}
	return names
}

func effectivePresetLimit(limit, defaultLimit int) (int, error) {
	if limit < 0 {
		return 0, fmt.Errorf("analytics query -limit must be >= 0")
	}
	if limit == 0 {
		return defaultLimit, nil
	}
	return limit, nil
}

func buildOverviewPreset(ctx context.Context, db *sql.DB, status Status, _ int, _ time.Time) (presetResult, error) {
	sessionsWithFailures, err := countWhere(ctx, db, `SELECT COUNT(*) FROM session_rollups WHERE failure_signal_count > 0`)
	if err != nil {
		return presetResult{}, err
	}
	return presetResult{
		Columns: []string{"envelopes", "events", "sessions", "command_rollups", "sessions_with_failures", "coverage"},
		Rows: []map[string]any{{
			"envelopes":              status.RowCounts["envelope_facts"],
			"events":                 status.RowCounts["event_facts"],
			"sessions":               status.RowCounts["session_rollups"],
			"command_rollups":        status.RowCounts["command_rollups"],
			"sessions_with_failures": sessionsWithFailures,
			"coverage":               displayText(status.CoverageMode),
		}},
	}, nil
}

func buildAttentionPreset(ctx context.Context, db *sql.DB, _ Status, limit int, now time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			global_session_key,
			source_type,
			source_instance_id,
			source_session_key,
			project_locator,
			project_key,
			last_timestamp,
			duration_ms,
			failure_signal_count,
			failed_command_rollup_count,
			incomplete_command_rollup_count
		FROM session_rollups
		WHERE failure_signal_count > 0 OR incomplete_command_rollup_count > 0
		ORDER BY failure_signal_count DESC, incomplete_command_rollup_count DESC, last_timestamp DESC, global_session_key ASC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}

	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		lastSeen := mapTime(item["last_timestamp"])
		duration := mapInt64(item["duration_ms"])
		failures := mapInt64(item["failure_signal_count"])
		incomplete := mapInt64(item["incomplete_command_rollup_count"])
		row := map[string]any{
			"global_session_key": hiddenString(item, "global_session_key"),
			"session":            displaySession(item),
			"source":             displaySource(mapString(item["source_type"]), mapString(item["source_instance_id"])),
			"project":            displayProject(mapString(item["project_locator"]), mapString(item["project_key"])),
			"status":             displaySessionStatus(failures, incomplete),
			"failures":           failures,
			"failed_cmds":        mapInt64(item["failed_command_rollup_count"]),
			"incomplete_cmds":    incomplete,
			"last_seen":          formatTimestamp(lastSeen),
			"age":                formatAge(lastSeen, now),
			"span":               formatDurationMillis(duration),
		}
		rows = append(rows, row)
	}

	return presetResult{
		Columns: []string{"session", "source", "project", "status", "failures", "failed_cmds", "incomplete_cmds", "last_seen", "age", "span"},
		Rows:    rows,
	}, nil
}

func buildRecentSessionsPreset(ctx context.Context, db *sql.DB, _ Status, limit int, now time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			global_session_key,
			source_type,
			source_instance_id,
			source_session_key,
			project_locator,
			project_key,
			event_count,
			user_message_count,
			agent_message_count,
			command_rollup_count,
			last_timestamp,
			duration_ms
		FROM session_rollups
		ORDER BY last_timestamp DESC, global_session_key ASC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}

	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		lastSeen := mapTime(item["last_timestamp"])
		duration := mapInt64(item["duration_ms"])
		row := map[string]any{
			"global_session_key": hiddenString(item, "global_session_key"),
			"session":            displaySession(item),
			"source":             displaySource(mapString(item["source_type"]), mapString(item["source_instance_id"])),
			"project":            displayProject(mapString(item["project_locator"]), mapString(item["project_key"])),
			"events":             mapInt64(item["event_count"]),
			"user_msgs":          mapInt64(item["user_message_count"]),
			"agent_msgs":         mapInt64(item["agent_message_count"]),
			"commands":           mapInt64(item["command_rollup_count"]),
			"last_seen":          formatTimestamp(lastSeen),
			"age":                formatAge(lastSeen, now),
			"span":               formatDurationMillis(duration),
		}
		rows = append(rows, row)
	}

	return presetResult{
		Columns: []string{"session", "source", "project", "events", "user_msgs", "agent_msgs", "commands", "last_seen", "age", "span"},
		Rows:    rows,
	}, nil
}

func buildProjectsPreset(ctx context.Context, db *sql.DB, _ Status, limit int, _ time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			project_locator,
			project_key,
			COUNT(*) AS sessions,
			CAST(SUM(event_count) AS BIGINT) AS events,
			CAST(SUM(CASE WHEN failure_signal_count > 0 THEN 1 ELSE 0 END) AS BIGINT) AS failure_sessions,
			CAST(SUM(command_rollup_count) AS BIGINT) AS commands,
			MAX(last_timestamp) AS last_timestamp
		FROM session_rollups
		GROUP BY project_locator, project_key
		ORDER BY last_timestamp DESC, sessions DESC, project_locator ASC, project_key ASC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}
	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		rows = append(rows, map[string]any{
			"project":   displayProject(mapString(item["project_locator"]), mapString(item["project_key"])),
			"sessions":  mapInt64(item["sessions"]),
			"events":    mapInt64(item["events"]),
			"failures":  mapInt64(item["failure_sessions"]),
			"commands":  mapInt64(item["commands"]),
			"last_seen": formatTimestamp(mapTime(item["last_timestamp"])),
		})
	}
	return presetResult{
		Columns: []string{"project", "sessions", "events", "failures", "commands", "last_seen"},
		Rows:    rows,
	}, nil
}

func buildSourcesPreset(ctx context.Context, db *sql.DB, _ Status, limit int, _ time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			source_type,
			source_instance_id,
			COUNT(*) AS sessions,
			CAST(SUM(event_count) AS BIGINT) AS events,
			CAST(SUM(CASE WHEN failure_signal_count > 0 THEN 1 ELSE 0 END) AS BIGINT) AS failure_sessions,
			CAST(SUM(command_rollup_count) AS BIGINT) AS commands,
			CAST(SUM(tool_failure_count) AS BIGINT) AS tool_failures,
			MAX(last_timestamp) AS last_timestamp
		FROM session_rollups
		GROUP BY source_type, source_instance_id
		ORDER BY last_timestamp DESC, sessions DESC, source_type ASC, source_instance_id ASC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}
	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		rows = append(rows, map[string]any{
			"source":        displaySource(mapString(item["source_type"]), mapString(item["source_instance_id"])),
			"sessions":      mapInt64(item["sessions"]),
			"events":        mapInt64(item["events"]),
			"failures":      mapInt64(item["failure_sessions"]),
			"commands":      mapInt64(item["commands"]),
			"tool_failures": mapInt64(item["tool_failures"]),
			"last_seen":     formatTimestamp(mapTime(item["last_timestamp"])),
		})
	}
	return presetResult{
		Columns: []string{"source", "sessions", "events", "failures", "commands", "tool_failures", "last_seen"},
		Rows:    rows,
	}, nil
}

func buildCommandHealthPreset(ctx context.Context, db *sql.DB, _ Status, limit int, _ time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			c.global_session_key,
			s.source_type,
			s.source_instance_id,
			s.source_session_key,
			s.project_locator,
			s.project_key,
			c.tool_name,
			c.status,
			c.exit_code,
			c.duration_ms,
			COALESCE(c.finish_timestamp, c.start_timestamp) AS finished_at
		FROM command_rollups c
		JOIN session_rollups s ON s.global_session_key = c.global_session_key
		ORDER BY COALESCE(c.finish_timestamp, c.start_timestamp) DESC, c.global_command_rollup_key ASC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}
	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		exitCode := mapInt64Ptr(item["exit_code"])
		rows = append(rows, map[string]any{
			"session":     displaySession(item),
			"project":     displayProject(mapString(item["project_locator"]), mapString(item["project_key"])),
			"tool":        displayText(firstNonEmpty(mapString(item["tool_name"]), "tool")),
			"status":      displayCommandStatus(mapString(item["status"]), exitCode),
			"exit_code":   exitCodeValue(exitCode),
			"duration":    formatDurationMillis(mapInt64(item["duration_ms"])),
			"finished_at": formatTimestamp(mapTime(item["finished_at"])),
		})
	}
	return presetResult{
		Columns: []string{"session", "project", "tool", "status", "exit_code", "duration", "finished_at"},
		Rows:    rows,
	}, nil
}

func buildTimelinePreset(ctx context.Context, db *sql.DB, _ Status, limit int, _ time.Time) (presetResult, error) {
	query := fmt.Sprintf(`
		SELECT
			date_trunc('day', timestamp) AS bucket_start,
			COUNT(*) AS events,
			CAST(SUM(CASE WHEN kind = 'session.started' THEN 1 ELSE 0 END) AS BIGINT) AS sessions_started,
			CAST(SUM(CASE WHEN kind = 'command.finished' THEN 1 ELSE 0 END) AS BIGINT) AS commands_finished,
			CAST(
				SUM(CASE WHEN kind = 'tool.failed' THEN 1 ELSE 0 END)
				+ SUM(CASE WHEN kind = 'command.finished' AND exit_code IS NOT NULL AND exit_code <> 0 THEN 1 ELSE 0 END)
				AS BIGINT
			) AS failures
		FROM event_facts
		GROUP BY bucket_start
		ORDER BY bucket_start DESC
		LIMIT %d
	`, limit)
	sourceRows, err := runQueryMaps(ctx, db, query)
	if err != nil {
		return presetResult{}, err
	}
	rows := make([]map[string]any, 0, len(sourceRows))
	for _, item := range sourceRows {
		rows = append(rows, map[string]any{
			"bucket_start":      formatTimestamp(mapTime(item["bucket_start"])),
			"events":            mapInt64(item["events"]),
			"sessions_started":  mapInt64(item["sessions_started"]),
			"commands_finished": mapInt64(item["commands_finished"]),
			"failures":          mapInt64(item["failures"]),
		})
	}
	return presetResult{
		Columns: []string{"bucket_start", "events", "sessions_started", "commands_finished", "failures"},
		Rows:    rows,
	}, nil
}

func buildCoveragePreset(_ context.Context, _ *sql.DB, status Status, _ int, _ time.Time) (presetResult, error) {
	builtAt := "-"
	if status.BuildCompletedAt != nil {
		builtAt = formatTimestamp(*status.BuildCompletedAt)
	}
	return presetResult{
		Columns: []string{"machine", "ledger_instance", "coverage", "cache_offsets", "retained_offsets", "last_clean_offset", "built_at", "db_size"},
		Rows: []map[string]any{{
			"machine":           displayText(status.MachineID),
			"ledger_instance":   displayText(status.LedgerInstanceID),
			"coverage":          displayText(status.CoverageMode),
			"cache_offsets":     formatOffsetRange(status.CacheCoveredOffsetRange),
			"retained_offsets":  formatOffsetRange(status.RetainedOffsetRange),
			"last_clean_offset": status.LastCleanOffset,
			"built_at":          builtAt,
			"db_size":           formatBytes(status.SizeBytes),
		}},
	}, nil
}

func runQueryMaps(ctx context.Context, db *sql.DB, query string) ([]map[string]any, error) {
	columns, rows, err := runQuery(ctx, db, query)
	if err != nil {
		return nil, err
	}
	return queryRowsToMaps(columns, rows), nil
}

func queryRowsToMaps(columns []string, rows [][]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(columns))
		for idx, column := range columns {
			if idx >= len(row) {
				item[column] = nil
				continue
			}
			item[column] = normalizePresetValue(row[idx])
		}
		out = append(out, item)
	}
	return out
}

func normalizePresetValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return typed
	}
}

func countWhere(ctx context.Context, db *sql.DB, query string) (int64, error) {
	row := db.QueryRowContext(ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func displaySession(row map[string]any) string {
	return displaySessionLabel(
		mapString(row["source_type"]),
		mapString(row["source_instance_id"]),
		mapString(row["source_session_key"]),
		mapString(row["global_session_key"]),
	)
}

func displaySessionLabel(sourceType, sourceInstanceID, sourceSessionKey, globalKey string) string {
	suffix := strings.TrimSpace(sourceSessionKey)
	if suffix == "" {
		suffix = shortID(globalKey)
	}
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}
	return fmt.Sprintf("%s#%s", displaySource(sourceType, sourceInstanceID), suffix)
}

func displaySource(sourceType, sourceInstanceID string) string {
	sourceType = displayText(sourceType)
	sourceInstanceID = displayText(sourceInstanceID)
	if sourceType == "-" {
		return sourceInstanceID
	}
	if sourceInstanceID == "-" {
		return sourceType
	}
	return sourceType + "/" + sourceInstanceID
}

func displayProject(locator, key string) string {
	value := firstNonEmpty(locator, key)
	if value == "" {
		return "-"
	}
	if strings.Contains(value, "://") || len(value) <= 40 {
		return value
	}
	clean := filepath.ToSlash(filepath.Clean(value))
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	if len(parts) <= 2 {
		return clean
	}
	return ".../" + strings.Join(parts[len(parts)-2:], "/")
}

func displaySessionStatus(failureSignals, incomplete int64) string {
	switch {
	case failureSignals > 0:
		return "failed"
	case incomplete > 0:
		return "incomplete"
	default:
		return "clean"
	}
}

func displayCommandStatus(status string, exitCode *int64) string {
	switch {
	case exitCode != nil && *exitCode != 0:
		return "failed"
	case status == "start_only" || status == "finish_only":
		return "incomplete"
	default:
		return "clean"
	}
}

func displayText(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func formatTimestamp(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format(time.RFC3339)
}

func formatAge(ts, now time.Time) string {
	if ts.IsZero() || now.Before(ts) {
		return "-"
	}
	return formatDuration(now.Sub(ts))
}

func formatDurationMillis(value int64) string {
	if value <= 0 {
		return "0s"
	}
	return formatDuration(time.Duration(value) * time.Millisecond)
}

func formatDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int64(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int64(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int64(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int64(d.Hours()/24))
	}
}

func formatOffsetRange(r *OffsetRange) string {
	if r == nil {
		return "-"
	}
	return fmt.Sprintf("%d..%d", r.Min, r.Max)
}

func formatBytes(value int64) string {
	if value <= 0 {
		return "0 B"
	}
	type unit struct {
		name string
		size float64
	}
	units := []unit{
		{name: "GiB", size: 1024 * 1024 * 1024},
		{name: "MiB", size: 1024 * 1024},
		{name: "KiB", size: 1024},
	}
	size := float64(value)
	for _, item := range units {
		if size >= item.size {
			return fmt.Sprintf("%.1f %s", size/item.size, item.name)
		}
	}
	return fmt.Sprintf("%d B", value)
}

func exitCodeValue(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func hiddenString(row map[string]any, key string) string {
	return mapString(row[key])
}

func mapString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func mapInt64(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case uint64:
		return int64(typed)
	default:
		return 0
	}
}

func mapInt64Ptr(value any) *int64 {
	switch typed := value.(type) {
	case nil:
		return nil
	case int:
		value := int64(typed)
		return &value
	case int32:
		value := int64(typed)
		return &value
	case int64:
		value := typed
		return &value
	case float64:
		value := int64(typed)
		return &value
	default:
		return nil
	}
}

func mapTime(value any) time.Time {
	switch typed := value.(type) {
	case nil:
		return time.Time{}
	case time.Time:
		return typed.UTC()
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err == nil {
			return parsed.UTC()
		}
		return time.Time{}
	default:
		return time.Time{}
	}
}

func shortID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	if len(value) <= 8 {
		return value
	}
	return value[len(value)-8:]
}

func sensitiveColumnsForPreset(name string) []string {
	switch name {
	case "attention", "recent_sessions":
		return []string{"content_status", "first_user_preview", "last_agent_preview"}
	default:
		return nil
	}
}

func appendSensitiveColumns(result presetResult, presetName string) presetResult {
	columns := append([]string(nil), result.Columns...)
	columns = append(columns, sensitiveColumnsForPreset(presetName)...)
	result.Columns = columns
	return result
}

func sortedSessionKeys(result presetResult) []string {
	keys := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		key := hiddenString(row, "global_session_key")
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

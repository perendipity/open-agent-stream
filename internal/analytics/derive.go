package analytics

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type sequenceStore struct {
	data map[string]int
}

func newSequenceStore() *sequenceStore {
	return &sequenceStore{data: map[string]int{}}
}

func (s *sequenceStore) GetSessionSequence(sessionKey string) (int, error) {
	return s.data[sessionKey], nil
}

func (s *sequenceStore) SetSessionSequence(sessionKey string, sequence int) error {
	s.data[sessionKey] = sequence
	return nil
}

func loadSequenceStore(tx *sql.Tx) (*sequenceStore, error) {
	store := newSequenceStore()
	rows, err := tx.Query(`SELECT session_key, MAX(sequence) FROM analytics_event_facts GROUP BY session_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			sessionKey string
			sequence   int
		)
		if err := rows.Scan(&sessionKey, &sequence); err != nil {
			return nil, err
		}
		store.data[sessionKey] = sequence
	}
	return store, rows.Err()
}

func seedNormalizerCallNames(tx *sql.Tx, normalizer *normalize.Service) error {
	rows, err := tx.Query(`SELECT DISTINCT call_id, tool_name FROM analytics_event_facts WHERE call_id <> '' AND tool_name <> ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			callID   string
			toolName string
		)
		if err := rows.Scan(&callID, &toolName); err != nil {
			return err
		}
		normalizer.SeedCallName(callID, toolName)
	}
	return rows.Err()
}

type envelopeFactRow struct {
	GlobalEnvelopeKey string
	MachineID         string
	LedgerInstanceID  string
	LedgerOffset      int64
	EnvelopeID        string
	SourceType        string
	SourceInstanceID  string
	ArtifactID        string
	ArtifactLocator   string
	ProjectLocator    string
	ProjectKey        string
	SourceSessionKey  string
	GlobalSessionKey  string
	CursorKind        string
	CursorValue       string
	ObservedAt        time.Time
	SourceTimestamp   *time.Time
	RawKind           string
	ContentHash       string
	PayloadBytes      int64
	CapabilitiesCSV   string
	ParseConfidence   float64
}

func (r envelopeFactRow) args() []any {
	return []any{
		r.GlobalEnvelopeKey,
		r.MachineID,
		r.LedgerInstanceID,
		r.LedgerOffset,
		r.EnvelopeID,
		r.SourceType,
		r.SourceInstanceID,
		r.ArtifactID,
		r.ArtifactLocator,
		r.ProjectLocator,
		r.ProjectKey,
		r.SourceSessionKey,
		r.GlobalSessionKey,
		r.CursorKind,
		r.CursorValue,
		r.ObservedAt.UTC(),
		timeValue(r.SourceTimestamp),
		r.RawKind,
		r.ContentHash,
		r.PayloadBytes,
		r.CapabilitiesCSV,
		r.ParseConfidence,
	}
}

type eventFactRow struct {
	GlobalEventKey    string
	GlobalEnvelopeKey string
	MachineID         string
	LedgerInstanceID  string
	LedgerOffset      int64
	EventID           string
	EventVersion      int
	SourceType        string
	SourceInstanceID  string
	SessionKey        string
	SourceSessionKey  string
	GlobalSessionKey  string
	ProjectKey        string
	ProjectLocator    string
	Sequence          int
	Timestamp         time.Time
	Kind              string
	ActorKind         string
	ActorName         string
	ParseStatus       string
	CapabilitiesCSV   string
	ToolName          string
	CallID            string
	ExitCode          *int64
	DurationMillis    *int64
	EnvelopeID        string
}

func (r eventFactRow) args() []any {
	return []any{
		r.GlobalEventKey,
		r.GlobalEnvelopeKey,
		r.MachineID,
		r.LedgerInstanceID,
		r.LedgerOffset,
		r.EventID,
		r.EventVersion,
		r.SourceType,
		r.SourceInstanceID,
		r.SessionKey,
		r.SourceSessionKey,
		r.GlobalSessionKey,
		r.ProjectKey,
		r.ProjectLocator,
		r.Sequence,
		r.Timestamp.UTC(),
		r.Kind,
		r.ActorKind,
		r.ActorName,
		r.ParseStatus,
		r.CapabilitiesCSV,
		r.ToolName,
		r.CallID,
		int64Value(r.ExitCode),
		int64Value(r.DurationMillis),
		r.EnvelopeID,
	}
}

type commandEventRow struct {
	GlobalEventKey    string
	GlobalEnvelopeKey string
	MachineID         string
	GlobalSessionKey  string
	SourceInstanceID  string
	SourceSessionKey  string
	Sequence          int
	Timestamp         time.Time
	Kind              string
	ToolName          string
	ActorName         string
	CallID            string
	ExitCode          *int64
	DurationMillis    *int64
}

type pendingCommandStart struct {
	row     commandEventRow
	matched bool
}

type commandRollupRow struct {
	GlobalCommandRollupKey  string
	DerivationVersion       string
	MachineID               string
	GlobalSessionKey        string
	SourceInstanceID        string
	SourceSessionKey        string
	Status                  string
	ToolName                string
	ActorName               string
	CallID                  string
	StartGlobalEventKey     string
	FinishGlobalEventKey    string
	StartGlobalEnvelopeKey  string
	FinishGlobalEnvelopeKey string
	StartSequence           *int64
	FinishSequence          *int64
	StartTimestamp          *time.Time
	FinishTimestamp         *time.Time
	ExitCode                *int64
	DurationMillis          *int64
}

func (r commandRollupRow) args() []any {
	return []any{
		r.GlobalCommandRollupKey,
		r.DerivationVersion,
		r.MachineID,
		r.GlobalSessionKey,
		r.SourceInstanceID,
		r.SourceSessionKey,
		r.Status,
		r.ToolName,
		r.ActorName,
		r.CallID,
		r.StartGlobalEventKey,
		r.FinishGlobalEventKey,
		r.StartGlobalEnvelopeKey,
		r.FinishGlobalEnvelopeKey,
		int64Value(r.StartSequence),
		int64Value(r.FinishSequence),
		timeValue(r.StartTimestamp),
		timeValue(r.FinishTimestamp),
		int64Value(r.ExitCode),
		int64Value(r.DurationMillis),
	}
}

type sessionRollupRow struct {
	GlobalSessionKey             string
	DerivationVersion            string
	MachineID                    string
	SourceType                   string
	SourceInstanceID             string
	SessionKey                   string
	SourceSessionKey             string
	ProjectKey                   string
	ProjectLocator               string
	FirstTimestamp               time.Time
	LastTimestamp                time.Time
	DurationMillis               int64
	EventCount                   int64
	UserMessageCount             int64
	AgentMessageCount            int64
	SystemMessageCount           int64
	CommandEventCount            int64
	CommandRollupCount           int64
	FailedCommandRollupCount     int64
	IncompleteCommandRollupCount int64
	ToolFailureCount             int64
	FilePatchCount               int64
	FailureSignalCount           int64
	ParseFailureCount            int64
}

func (r sessionRollupRow) args() []any {
	return []any{
		r.GlobalSessionKey,
		r.DerivationVersion,
		r.MachineID,
		r.SourceType,
		r.SourceInstanceID,
		r.SessionKey,
		r.SourceSessionKey,
		r.ProjectKey,
		r.ProjectLocator,
		r.FirstTimestamp.UTC(),
		r.LastTimestamp.UTC(),
		r.DurationMillis,
		r.EventCount,
		r.UserMessageCount,
		r.AgentMessageCount,
		r.SystemMessageCount,
		r.CommandEventCount,
		r.CommandRollupCount,
		r.FailedCommandRollupCount,
		r.IncompleteCommandRollupCount,
		r.ToolFailureCount,
		r.FilePatchCount,
		r.FailureSignalCount,
		r.ParseFailureCount,
	}
}

func buildEnvelopeFact(record ledger.Record, event schema.CanonicalEvent, machineID, ledgerInstanceID string) envelopeFactRow {
	return envelopeFactRow{
		GlobalEnvelopeKey: schema.StableID("gaenv", machineID, record.Envelope.EnvelopeID),
		MachineID:         machineID,
		LedgerInstanceID:  ledgerInstanceID,
		LedgerOffset:      record.Offset,
		EnvelopeID:        record.Envelope.EnvelopeID,
		SourceType:        record.Envelope.SourceType,
		SourceInstanceID:  record.Envelope.SourceInstanceID,
		ArtifactID:        record.Envelope.ArtifactID,
		ArtifactLocator:   record.Envelope.ArtifactLocator,
		ProjectLocator:    firstNonEmpty(event.Context.ProjectLocator, record.Envelope.ProjectLocator),
		ProjectKey:        event.Context.ProjectKey,
		SourceSessionKey:  event.Context.SourceSessionKey,
		GlobalSessionKey:  schema.StableID("gasess", machineID, record.Envelope.SourceInstanceID, event.Context.SourceSessionKey),
		CursorKind:        record.Envelope.Cursor.Kind,
		CursorValue:       record.Envelope.Cursor.Value,
		ObservedAt:        record.Envelope.ObservedAt.UTC(),
		SourceTimestamp:   record.Envelope.SourceTimestamp,
		RawKind:           record.Envelope.RawKind,
		ContentHash:       record.Envelope.ContentHash,
		PayloadBytes:      int64(len(record.Envelope.RawPayload)),
		CapabilitiesCSV:   strings.Join(record.Envelope.ParseHints.Capabilities, ","),
		ParseConfidence:   record.Envelope.ParseHints.Confidence,
	}
}

func buildEventFact(record ledger.Record, event schema.CanonicalEvent, machineID, ledgerInstanceID string) eventFactRow {
	exitCode, hasExitCode := payloadInt(event.Payload, "exit_code")
	durationMillis, hasDurationMillis := payloadInt(event.Payload, "duration_ms")
	row := eventFactRow{
		GlobalEventKey:    schema.StableID("gaevt", machineID, event.EventID),
		GlobalEnvelopeKey: schema.StableID("gaenv", machineID, record.Envelope.EnvelopeID),
		MachineID:         machineID,
		LedgerInstanceID:  ledgerInstanceID,
		LedgerOffset:      record.Offset,
		EventID:           event.EventID,
		EventVersion:      event.EventVersion,
		SourceType:        event.SourceType,
		SourceInstanceID:  event.SourceInstanceID,
		SessionKey:        event.SessionKey,
		SourceSessionKey:  event.Context.SourceSessionKey,
		GlobalSessionKey:  schema.StableID("gasess", machineID, event.SourceInstanceID, event.Context.SourceSessionKey),
		ProjectKey:        event.Context.ProjectKey,
		ProjectLocator:    event.Context.ProjectLocator,
		Sequence:          event.Sequence,
		Timestamp:         event.Timestamp.UTC(),
		Kind:              event.Kind,
		ActorKind:         event.Actor.Kind,
		ActorName:         event.Actor.Name,
		ParseStatus:       event.ParseStatus,
		CapabilitiesCSV:   strings.Join(event.Context.Capabilities, ","),
		ToolName:          eventToolName(event),
		CallID:            payloadCallID(event.Payload),
		EnvelopeID:        record.Envelope.EnvelopeID,
	}
	if hasExitCode {
		row.ExitCode = int64Ptr(int64(exitCode))
	}
	if hasDurationMillis {
		row.DurationMillis = int64Ptr(int64(durationMillis))
	}
	return row
}

func eventToolName(event schema.CanonicalEvent) string {
	return firstNonEmpty(
		payloadString(event.Payload, "tool_name", "tool", "name"),
		event.Actor.Name,
	)
}

func deriveCommandRollups(tx *sql.Tx) ([]commandRollupRow, error) {
	rows, err := tx.Query(`
		SELECT
			global_event_key,
			global_envelope_key,
			machine_id,
			global_session_key,
			source_instance_id,
			source_session_key,
			sequence,
			timestamp,
			kind,
			tool_name,
			actor_name,
			call_id,
			exit_code,
			duration_ms
		FROM analytics_event_facts
		WHERE kind IN ('command.started', 'command.finished')
		ORDER BY global_session_key, sequence ASC, timestamp ASC, global_event_key ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	grouped := map[string][]commandEventRow{}
	order := make([]string, 0)
	for rows.Next() {
		var (
			row        commandEventRow
			callID     sql.NullString
			toolName   sql.NullString
			actorName  sql.NullString
			sourceKey  sql.NullString
			exitCode   sql.NullInt64
			durationMS sql.NullInt64
		)
		if err := rows.Scan(
			&row.GlobalEventKey,
			&row.GlobalEnvelopeKey,
			&row.MachineID,
			&row.GlobalSessionKey,
			&row.SourceInstanceID,
			&sourceKey,
			&row.Sequence,
			&row.Timestamp,
			&row.Kind,
			&toolName,
			&actorName,
			&callID,
			&exitCode,
			&durationMS,
		); err != nil {
			return nil, err
		}
		row.ToolName = nullString(toolName)
		row.ActorName = nullString(actorName)
		row.CallID = nullString(callID)
		row.SourceSessionKey = nullString(sourceKey)
		if exitCode.Valid {
			row.ExitCode = int64Ptr(exitCode.Int64)
		}
		if durationMS.Valid {
			row.DurationMillis = int64Ptr(durationMS.Int64)
		}
		if _, ok := grouped[row.GlobalSessionKey]; !ok {
			order = append(order, row.GlobalSessionKey)
		}
		grouped[row.GlobalSessionKey] = append(grouped[row.GlobalSessionKey], row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]commandRollupRow, 0)
	for _, sessionKey := range order {
		out = append(out, collapseCommandSession(grouped[sessionKey])...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GlobalSessionKey == out[j].GlobalSessionKey {
			left := startSequenceOrZero(out[i])
			right := startSequenceOrZero(out[j])
			if left == right {
				return out[i].GlobalCommandRollupKey < out[j].GlobalCommandRollupKey
			}
			return left < right
		}
		return out[i].GlobalSessionKey < out[j].GlobalSessionKey
	})
	return out, nil
}

func collapseCommandSession(events []commandEventRow) []commandRollupRow {
	pendingStarts := make([]pendingCommandStart, 0, len(events))
	pendingByCallID := map[string][]int{}
	out := make([]commandRollupRow, 0, len(events))

	for _, event := range events {
		switch event.Kind {
		case "command.started":
			index := len(pendingStarts)
			pendingStarts = append(pendingStarts, pendingCommandStart{row: event})
			if event.CallID != "" {
				pendingByCallID[event.CallID] = append(pendingByCallID[event.CallID], index)
			}
		case "command.finished":
			start, ok := matchPendingStart(event, pendingStarts, pendingByCallID)
			if !ok {
				out = append(out, finishOnlyCommandRollup(event))
				continue
			}
			out = append(out, pairedCommandRollup(start, event))
		}
	}

	for _, start := range pendingStarts {
		if start.matched {
			continue
		}
		out = append(out, startOnlyCommandRollup(start.row))
	}
	return out
}

func matchPendingStart(event commandEventRow, starts []pendingCommandStart, pendingByCallID map[string][]int) (commandEventRow, bool) {
	if event.CallID != "" {
		if index, ok := popPendingIndex(pendingByCallID, event.CallID, starts); ok {
			starts[index].matched = true
			return starts[index].row, true
		}
	}
	for idx := range starts {
		if starts[idx].matched {
			continue
		}
		starts[idx].matched = true
		if starts[idx].row.CallID != "" {
			removePendingIndex(pendingByCallID, starts[idx].row.CallID, idx)
		}
		return starts[idx].row, true
	}
	return commandEventRow{}, false
}

func popPendingIndex(indexes map[string][]int, key string, starts []pendingCommandStart) (int, bool) {
	queue := indexes[key]
	for len(queue) > 0 {
		index := queue[0]
		queue = queue[1:]
		if index < 0 || index >= len(starts) || starts[index].matched {
			continue
		}
		if len(queue) == 0 {
			delete(indexes, key)
		} else {
			indexes[key] = queue
		}
		return index, true
	}
	delete(indexes, key)
	return 0, false
}

func removePendingIndex(indexes map[string][]int, key string, target int) {
	queue := indexes[key]
	if len(queue) == 0 {
		return
	}
	filtered := queue[:0]
	for _, index := range queue {
		if index == target {
			continue
		}
		filtered = append(filtered, index)
	}
	if len(filtered) == 0 {
		delete(indexes, key)
		return
	}
	indexes[key] = filtered
}

func pairedCommandRollup(start, finish commandEventRow) commandRollupRow {
	return commandRollupRow{
		GlobalCommandRollupKey:  stableCommandRollupKey(start.MachineID, start.GlobalEnvelopeKey, finish.GlobalEnvelopeKey),
		DerivationVersion:       DerivationVersion,
		MachineID:               start.MachineID,
		GlobalSessionKey:        start.GlobalSessionKey,
		SourceInstanceID:        start.SourceInstanceID,
		SourceSessionKey:        firstNonEmpty(start.SourceSessionKey, finish.SourceSessionKey),
		Status:                  "completed",
		ToolName:                firstNonEmpty(start.ToolName, finish.ToolName),
		ActorName:               firstNonEmpty(start.ActorName, finish.ActorName),
		CallID:                  firstNonEmpty(start.CallID, finish.CallID),
		StartGlobalEventKey:     start.GlobalEventKey,
		FinishGlobalEventKey:    finish.GlobalEventKey,
		StartGlobalEnvelopeKey:  start.GlobalEnvelopeKey,
		FinishGlobalEnvelopeKey: finish.GlobalEnvelopeKey,
		StartSequence:           int64Ptr(int64(start.Sequence)),
		FinishSequence:          int64Ptr(int64(finish.Sequence)),
		StartTimestamp:          timePtr(start.Timestamp),
		FinishTimestamp:         timePtr(finish.Timestamp),
		ExitCode:                finish.ExitCode,
		DurationMillis:          durationMillis(start, finish),
	}
}

func startOnlyCommandRollup(start commandEventRow) commandRollupRow {
	return commandRollupRow{
		GlobalCommandRollupKey: stableCommandRollupKey(start.MachineID, start.GlobalEnvelopeKey, ""),
		DerivationVersion:      DerivationVersion,
		MachineID:              start.MachineID,
		GlobalSessionKey:       start.GlobalSessionKey,
		SourceInstanceID:       start.SourceInstanceID,
		SourceSessionKey:       start.SourceSessionKey,
		Status:                 "start_only",
		ToolName:               start.ToolName,
		ActorName:              start.ActorName,
		CallID:                 start.CallID,
		StartGlobalEventKey:    start.GlobalEventKey,
		StartGlobalEnvelopeKey: start.GlobalEnvelopeKey,
		StartSequence:          int64Ptr(int64(start.Sequence)),
		StartTimestamp:         timePtr(start.Timestamp),
	}
}

func finishOnlyCommandRollup(finish commandEventRow) commandRollupRow {
	return commandRollupRow{
		GlobalCommandRollupKey:  stableCommandRollupKey(finish.MachineID, "", finish.GlobalEnvelopeKey),
		DerivationVersion:       DerivationVersion,
		MachineID:               finish.MachineID,
		GlobalSessionKey:        finish.GlobalSessionKey,
		SourceInstanceID:        finish.SourceInstanceID,
		SourceSessionKey:        finish.SourceSessionKey,
		Status:                  "finish_only",
		ToolName:                finish.ToolName,
		ActorName:               finish.ActorName,
		CallID:                  finish.CallID,
		FinishGlobalEventKey:    finish.GlobalEventKey,
		FinishGlobalEnvelopeKey: finish.GlobalEnvelopeKey,
		FinishSequence:          int64Ptr(int64(finish.Sequence)),
		FinishTimestamp:         timePtr(finish.Timestamp),
		ExitCode:                finish.ExitCode,
		DurationMillis:          finish.DurationMillis,
	}
}

func durationMillis(start, finish commandEventRow) *int64 {
	if finish.DurationMillis != nil {
		return finish.DurationMillis
	}
	if finish.Timestamp.Before(start.Timestamp) {
		return nil
	}
	return int64Ptr(finish.Timestamp.Sub(start.Timestamp).Milliseconds())
}

func stableCommandRollupKey(machineID, startEnvelopeKey, finishEnvelopeKey string) string {
	return schema.StableID("gacmd", machineID, DerivationVersion, firstNonEmpty(startEnvelopeKey, "none"), firstNonEmpty(finishEnvelopeKey, "none"))
}

func startSequenceOrZero(row commandRollupRow) int64 {
	if row.StartSequence != nil {
		return *row.StartSequence
	}
	if row.FinishSequence != nil {
		return *row.FinishSequence
	}
	return 0
}

func deriveSessionRollups(tx *sql.Tx, commands []commandRollupRow) ([]sessionRollupRow, error) {
	rows, err := tx.Query(`
		SELECT
			global_session_key,
			machine_id,
			source_type,
			source_instance_id,
			session_key,
			source_session_key,
			project_key,
			project_locator,
			timestamp,
			kind,
			parse_status,
			exit_code
		FROM analytics_event_facts
		ORDER BY global_session_key, sequence ASC, timestamp ASC, global_event_key ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := map[string]*sessionRollupRow{}
	order := make([]string, 0)
	for rows.Next() {
		var (
			globalSessionKey string
			machineID        string
			sourceType       string
			sourceInstanceID string
			sessionKey       string
			sourceSessionKey sql.NullString
			projectKey       sql.NullString
			projectLocator   sql.NullString
			timestamp        time.Time
			kind             string
			parseStatus      sql.NullString
			exitCode         sql.NullInt64
		)
		if err := rows.Scan(
			&globalSessionKey,
			&machineID,
			&sourceType,
			&sourceInstanceID,
			&sessionKey,
			&sourceSessionKey,
			&projectKey,
			&projectLocator,
			&timestamp,
			&kind,
			&parseStatus,
			&exitCode,
		); err != nil {
			return nil, err
		}
		item, ok := stats[globalSessionKey]
		if !ok {
			item = &sessionRollupRow{
				GlobalSessionKey:  globalSessionKey,
				DerivationVersion: DerivationVersion,
				MachineID:         machineID,
				SourceType:        sourceType,
				SourceInstanceID:  sourceInstanceID,
				SessionKey:        sessionKey,
				SourceSessionKey:  nullString(sourceSessionKey),
				ProjectKey:        nullString(projectKey),
				ProjectLocator:    nullString(projectLocator),
				FirstTimestamp:    timestamp.UTC(),
				LastTimestamp:     timestamp.UTC(),
			}
			stats[globalSessionKey] = item
			order = append(order, globalSessionKey)
		}
		item.EventCount++
		if timestamp.Before(item.FirstTimestamp) {
			item.FirstTimestamp = timestamp.UTC()
		}
		if timestamp.After(item.LastTimestamp) {
			item.LastTimestamp = timestamp.UTC()
		}
		switch kind {
		case "message.user":
			item.UserMessageCount++
		case "message.agent":
			item.AgentMessageCount++
		case "message.system":
			item.SystemMessageCount++
		case "command.started", "command.finished":
			item.CommandEventCount++
		case "tool.failed":
			item.ToolFailureCount++
			item.FailureSignalCount++
		case "file.patch":
			item.FilePatchCount++
		}
		if kind == "command.finished" && exitCode.Valid && exitCode.Int64 != 0 {
			item.FailureSignalCount++
		}
		if status := nullString(parseStatus); status != "" && status != "ok" {
			item.ParseFailureCount++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, command := range commands {
		item := stats[command.GlobalSessionKey]
		if item == nil {
			continue
		}
		item.CommandRollupCount++
		if command.Status == "start_only" || command.Status == "finish_only" {
			item.IncompleteCommandRollupCount++
		}
		if command.ExitCode != nil && *command.ExitCode != 0 {
			item.FailedCommandRollupCount++
		}
	}

	out := make([]sessionRollupRow, 0, len(order))
	for _, key := range order {
		item := stats[key]
		item.DurationMillis = item.LastTimestamp.Sub(item.FirstTimestamp).Milliseconds()
		out = append(out, *item)
	}
	return out, nil
}

func payloadCallID(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if value := payloadString(payload, "call_id"); value != "" {
		return value
	}
	return payloadString(mapValue(payload, "payload"), "call_id")
}

func payloadString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		text, _ := value.(string)
		if strings.TrimSpace(text) != "" {
			return text
		}
	}
	return ""
}

func payloadInt(payload map[string]any, key string) (int, bool) {
	if payload == nil {
		return 0, false
	}
	value, ok := payload[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func mapValue(payload map[string]any, key string) map[string]any {
	if payload == nil {
		return nil
	}
	value, ok := payload[key]
	if !ok {
		return nil
	}
	out, _ := value.(map[string]any)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func int64Ptr(value int64) *int64 {
	return &value
}

func timePtr(value time.Time) *time.Time {
	ts := value.UTC()
	return &ts
}

func int64Value(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func timeValue(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC()
}

func requireSingleString(value string, field string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	return nil
}

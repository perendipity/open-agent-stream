package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type sessionKindCount struct {
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

type commandEventRecord struct {
	Sequence       int
	Timestamp      time.Time
	Kind           string
	CallID         string
	Command        string
	ExitCode       *int
	DurationMillis *int
}

type pendingCommandStart struct {
	record  commandEventRecord
	matched bool
}

type inspectedCommand struct {
	Sequence        int        `json:"sequence"`
	Status          string     `json:"status"`
	Command         string     `json:"command,omitempty"`
	ExitCode        *int       `json:"exit_code,omitempty"`
	StartSequence   *int       `json:"start_sequence,omitempty"`
	StartTimestamp  *time.Time `json:"start_timestamp,omitempty"`
	FinishSequence  *int       `json:"finish_sequence,omitempty"`
	FinishTimestamp *time.Time `json:"finish_timestamp,omitempty"`
	DurationMillis  *int       `json:"duration_ms,omitempty"`
}

type inspectedToolFailure struct {
	Sequence  int       `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	ToolName  string    `json:"tool_name,omitempty"`
	Command   string    `json:"command,omitempty"`
	ExitCode  *int      `json:"exit_code,omitempty"`
	Summary   string    `json:"summary,omitempty"`
}

type sessionTimelineEvent struct {
	Sequence  int       `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Kind      string    `json:"kind"`
	ActorKind string    `json:"actor_kind,omitempty"`
	ActorName string    `json:"actor_name,omitempty"`
	Summary   string    `json:"summary,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

type sessionInspection struct {
	SessionKey            string                 `json:"session_key"`
	SourceType            string                 `json:"source_type"`
	SourceInstanceID      string                 `json:"source_instance_id"`
	SourceSessionKey      string                 `json:"source_session_key,omitempty"`
	ProjectKey            string                 `json:"project_key,omitempty"`
	ProjectLocator        string                 `json:"project_locator,omitempty"`
	CommandFilter         string                 `json:"command_filter,omitempty"`
	EventCount            int                    `json:"event_count"`
	FirstTimestamp        time.Time              `json:"first_timestamp"`
	LastTimestamp         time.Time              `json:"last_timestamp"`
	DurationSeconds       float64                `json:"duration_seconds"`
	KindCounts            []sessionKindCount     `json:"kind_counts"`
	Commands              []inspectedCommand     `json:"commands,omitempty"`
	OmittedCommands       int                    `json:"omitted_commands,omitempty"`
	ToolFailures          []inspectedToolFailure `json:"tool_failures,omitempty"`
	AttentionEvents       []sessionTimelineEvent `json:"attention_events,omitempty"`
	Timeline              []sessionTimelineEvent `json:"timeline"`
	OmittedTimelineEvents int                    `json:"omitted_timeline_events,omitempty"`
	commandHeadCount      int
	timelineHeadCount     int
}

const (
	commandFilterAll        = "all"
	commandFilterAttention  = "attention"
	commandFilterFailed     = "failed"
	commandFilterIncomplete = "incomplete"
	commandFilterCompleted  = "completed"
)

func inspectCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON to inspect directly from the ledger")
	inputPath := fs.String("input", "-", "path to exported JSONL, or - for stdin")
	sessionKey := fs.String("session", "", "session key from the summary output")
	commandStatus := fs.String("command-status", commandFilterAll, "which collapsed command rows to show: all|attention|failed|incomplete|completed")
	commandLimit := fs.Int("command-limit", 40, "maximum collapsed command rows to display (0 = all)")
	timelineLimit := fs.Int("timeline-limit", 40, "maximum representative timeline rows to display")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a human-readable report")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas inspect -session <session_key> [flags]

Inspect one canonical session in reviewer-friendly detail.

Selection:
  -session <key>   Session key from "oas summary"

Input sources:
  -input <path>    Read exported JSONL from a file
  -input -         Read exported JSONL from stdin
  -config <path>   Inspect directly from the configured ledger

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "session", Placeholder: "<session_key>"},
			usageFlag{Name: "input", Placeholder: "<path|->"},
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "command-status", Placeholder: "<all|attention|failed|incomplete|completed>"},
			usageFlag{Name: "command-limit", Placeholder: "<n>"},
			usageFlag{Name: "timeline-limit", Placeholder: "<n>"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas summary -input ./exports/events.jsonl",
			"oas inspect -input ./exports/events.jsonl -session sess_123",
			"oas inspect -input ./exports/events.jsonl -session sess_123 -command-status attention",
			"oas inspect -input ./exports/events.jsonl -session sess_123 -command-limit 0",
			"oas export -config ./oas.json | oas inspect -session sess_123",
			"oas inspect -config ./oas.json -session sess_123 -json",
		)
	}
	_ = fs.Parse(args)

	if strings.TrimSpace(*sessionKey) == "" {
		fatal(errors.New("inspect requires -session <session_key>; use `oas summary` to find one"))
	}
	normalizedCommandFilter, err := normalizeCommandFilter(*commandStatus)
	if err != nil {
		fatal(err)
	}

	reader, closer, err := openSummaryInput(ctx, *configPath, *inputPath)
	if err != nil {
		fatal(err)
	}
	if closer != nil {
		defer closer.Close()
	}

	inspection, err := inspectSession(reader, *sessionKey, normalizedCommandFilter, *commandLimit, *timelineLimit)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, inspection); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeSessionInspection(os.Stdout, inspection); err != nil {
		fatal(err)
	}
}

func inspectSession(reader io.Reader, sessionKey, commandFilter string, commandLimit, timelineLimit int) (sessionInspection, error) {
	normalizedCommandFilter, err := normalizeCommandFilter(commandFilter)
	if err != nil {
		return sessionInspection{}, err
	}
	decoder := json.NewDecoder(reader)
	inspection := sessionInspection{}
	kindCounts := map[string]int{}
	commandContextByCallID := map[string]string{}
	commandEvents := make([]commandEventRecord, 0)
	timeline := make([]sessionTimelineEvent, 0)
	found := false

	for {
		var event schema.CanonicalEvent
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			return sessionInspection{}, err
		}
		if event.SessionKey != sessionKey {
			continue
		}

		if !found {
			found = true
			inspection = sessionInspection{
				SessionKey:       event.SessionKey,
				SourceType:       event.SourceType,
				SourceInstanceID: event.SourceInstanceID,
				SourceSessionKey: event.Context.SourceSessionKey,
				ProjectKey:       event.Context.ProjectKey,
				ProjectLocator:   event.Context.ProjectLocator,
				CommandFilter:    normalizedCommandFilter,
				FirstTimestamp:   event.Timestamp,
				LastTimestamp:    event.Timestamp,
			}
		}

		inspection.EventCount++
		if event.Timestamp.Before(inspection.FirstTimestamp) {
			inspection.FirstTimestamp = event.Timestamp
		}
		if event.Timestamp.After(inspection.LastTimestamp) {
			inspection.LastTimestamp = event.Timestamp
		}
		if inspection.SourceType == "" {
			inspection.SourceType = event.SourceType
		}
		if inspection.SourceInstanceID == "" {
			inspection.SourceInstanceID = event.SourceInstanceID
		}
		if inspection.SourceSessionKey == "" {
			inspection.SourceSessionKey = event.Context.SourceSessionKey
		}
		if inspection.ProjectKey == "" {
			inspection.ProjectKey = event.Context.ProjectKey
		}
		if inspection.ProjectLocator == "" {
			inspection.ProjectLocator = event.Context.ProjectLocator
		}

		kindCounts[event.Kind]++

		commandContext := inspectionCommandContext(event, commandContextByCallID)
		if event.Kind == "command.started" {
			if callID := payloadCallID(event.Payload); callID != "" && commandContext != "" {
				commandContextByCallID[callID] = commandContext
			}
		}

		if command, ok := inspectionCommandEvent(event, commandContext); ok {
			commandEvents = append(commandEvents, command)
		}
		if failure, ok := inspectionToolFailure(event, commandContext); ok {
			inspection.ToolFailures = append(inspection.ToolFailures, failure)
		}
		if attentionEvent, ok := inspectionAttentionEvent(event, commandContext); ok {
			inspection.AttentionEvents = append(inspection.AttentionEvents, attentionEvent)
		}
		if timelineEvent, ok := inspectionTimeline(event, commandContext); ok {
			timeline = append(timeline, timelineEvent)
		}
	}

	if !found {
		return sessionInspection{}, fmt.Errorf("session %q not found", sessionKey)
	}

	inspection.DurationSeconds = inspection.LastTimestamp.Sub(inspection.FirstTimestamp).Seconds()
	inspection.KindCounts = collapseKindCounts(kindCounts)

	sort.Slice(commandEvents, func(i, j int) bool {
		if commandEvents[i].Sequence == commandEvents[j].Sequence {
			return commandEvents[i].Timestamp.Before(commandEvents[j].Timestamp)
		}
		return commandEvents[i].Sequence < commandEvents[j].Sequence
	})
	filteredCommands := filterCommands(collapseCommands(commandEvents), normalizedCommandFilter)
	inspection.Commands, inspection.OmittedCommands, inspection.commandHeadCount = selectCommands(filteredCommands, commandLimit)
	sort.Slice(inspection.ToolFailures, func(i, j int) bool {
		if inspection.ToolFailures[i].Sequence == inspection.ToolFailures[j].Sequence {
			return inspection.ToolFailures[i].Timestamp.Before(inspection.ToolFailures[j].Timestamp)
		}
		return inspection.ToolFailures[i].Sequence < inspection.ToolFailures[j].Sequence
	})
	sort.Slice(inspection.AttentionEvents, func(i, j int) bool {
		if inspection.AttentionEvents[i].Sequence == inspection.AttentionEvents[j].Sequence {
			return inspection.AttentionEvents[i].Timestamp.Before(inspection.AttentionEvents[j].Timestamp)
		}
		return inspection.AttentionEvents[i].Sequence < inspection.AttentionEvents[j].Sequence
	})
	sort.Slice(timeline, func(i, j int) bool {
		if timeline[i].Sequence == timeline[j].Sequence {
			return timeline[i].Timestamp.Before(timeline[j].Timestamp)
		}
		return timeline[i].Sequence < timeline[j].Sequence
	})

	inspection.Timeline, inspection.OmittedTimelineEvents, inspection.timelineHeadCount = selectTimeline(timeline, timelineLimit)
	return inspection, nil
}

func collapseKindCounts(counts map[string]int) []sessionKindCount {
	out := make([]sessionKindCount, 0, len(counts))
	for kind, count := range counts {
		out = append(out, sessionKindCount{Kind: kind, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func normalizeCommandFilter(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", commandFilterAll:
		return commandFilterAll, nil
	case commandFilterAttention:
		return commandFilterAttention, nil
	case commandFilterFailed:
		return commandFilterFailed, nil
	case commandFilterIncomplete:
		return commandFilterIncomplete, nil
	case commandFilterCompleted:
		return commandFilterCompleted, nil
	default:
		return "", fmt.Errorf(
			"invalid -command-status %q (supported: %s, %s, %s, %s, %s)",
			value,
			commandFilterAll,
			commandFilterAttention,
			commandFilterFailed,
			commandFilterIncomplete,
			commandFilterCompleted,
		)
	}
}

func inspectionCommandEvent(event schema.CanonicalEvent, commandContext string) (commandEventRecord, bool) {
	if event.Kind != "command.started" && event.Kind != "command.finished" {
		return commandEventRecord{}, false
	}
	command := compactText(firstNonEmpty(payloadString(event.Payload, "command"), commandContext))
	exitCode, ok := payloadInt(event.Payload, "exit_code")
	durationMillis, hasDurationMillis := payloadInt(event.Payload, "duration_ms")
	record := commandEventRecord{
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		Kind:      event.Kind,
		CallID:    payloadCallID(event.Payload),
		Command:   command,
	}
	if ok {
		record.ExitCode = intPtr(exitCode)
	}
	if hasDurationMillis {
		record.DurationMillis = intPtr(durationMillis)
	}
	return record, true
}

func collapseCommands(events []commandEventRecord) []inspectedCommand {
	if len(events) == 0 {
		return nil
	}
	pendingStarts := make([]pendingCommandStart, 0, len(events))
	pendingByCallID := map[string][]int{}
	pendingByCommand := map[string][]int{}
	rows := make([]inspectedCommand, 0, len(events))

	for _, event := range events {
		switch event.Kind {
		case "command.started":
			index := len(pendingStarts)
			pendingStarts = append(pendingStarts, pendingCommandStart{record: event})
			if callID := strings.TrimSpace(event.CallID); callID != "" {
				pendingByCallID[callID] = append(pendingByCallID[callID], index)
			}
			if commandKey := commandMatchKey(event.Command); commandKey != "" {
				pendingByCommand[commandKey] = append(pendingByCommand[commandKey], index)
			}
		case "command.finished":
			start, ok := matchPendingCommandStart(event, pendingStarts, pendingByCallID, pendingByCommand)
			if !ok {
				rows = append(rows, incompleteCommandRow(event))
				continue
			}
			rows = append(rows, completedCommandRow(start, event))
		default:
			rows = append(rows, incompleteCommandRow(event))
		}
	}

	for _, pending := range pendingStarts {
		if pending.matched {
			continue
		}
		rows = append(rows, incompleteCommandRow(pending.record))
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Sequence == rows[j].Sequence {
			leftFinish := 0
			if rows[i].FinishSequence != nil {
				leftFinish = *rows[i].FinishSequence
			}
			rightFinish := 0
			if rows[j].FinishSequence != nil {
				rightFinish = *rows[j].FinishSequence
			}
			return leftFinish < rightFinish
		}
		return rows[i].Sequence < rows[j].Sequence
	})

	return rows
}

func filterCommands(commands []inspectedCommand, commandFilter string) []inspectedCommand {
	if commandFilter == commandFilterAll {
		return append([]inspectedCommand(nil), commands...)
	}
	out := make([]inspectedCommand, 0, len(commands))
	for _, command := range commands {
		if commandMatchesFilter(command, commandFilter) {
			out = append(out, command)
		}
	}
	return out
}

func commandMatchesFilter(command inspectedCommand, commandFilter string) bool {
	switch commandFilter {
	case commandFilterAll:
		return true
	case commandFilterCompleted:
		return command.Status == "completed"
	case commandFilterIncomplete:
		return commandIsIncomplete(command)
	case commandFilterFailed:
		return commandHasNonZeroExit(command)
	case commandFilterAttention:
		return commandIsIncomplete(command) || commandHasNonZeroExit(command)
	default:
		return true
	}
}

func commandIsIncomplete(command inspectedCommand) bool {
	return command.Status == "started_only" || command.Status == "finished_only"
}

func commandHasNonZeroExit(command inspectedCommand) bool {
	return command.ExitCode != nil && *command.ExitCode != 0
}

func canCollapseCommandPair(start, finish commandEventRecord) bool {
	if start.Kind != "command.started" || finish.Kind != "command.finished" {
		return false
	}
	if finish.Sequence <= start.Sequence {
		return false
	}
	startCallID := strings.TrimSpace(start.CallID)
	finishCallID := strings.TrimSpace(finish.CallID)
	if startCallID != "" && finishCallID != "" {
		return startCallID == finishCallID
	}
	startCommand := strings.TrimSpace(start.Command)
	finishCommand := strings.TrimSpace(finish.Command)
	return startCommand == "" || finishCommand == "" || startCommand == finishCommand
}

func commandMatchKey(command string) string {
	return strings.TrimSpace(command)
}

func matchPendingCommandStart(
	finish commandEventRecord,
	pendingStarts []pendingCommandStart,
	pendingByCallID map[string][]int,
	pendingByCommand map[string][]int,
) (commandEventRecord, bool) {
	if callID := strings.TrimSpace(finish.CallID); callID != "" {
		if index, ok := popPendingIndex(pendingByCallID, callID, pendingStarts); ok {
			return claimPendingCommandStart(index, pendingStarts, pendingByCallID, pendingByCommand)
		}
	}
	if commandKey := commandMatchKey(finish.Command); commandKey != "" {
		if index, ok := popPendingIndex(pendingByCommand, commandKey, pendingStarts); ok {
			return claimPendingCommandStart(index, pendingStarts, pendingByCallID, pendingByCommand)
		}
	}
	return commandEventRecord{}, false
}

func popPendingIndex(indexes map[string][]int, key string, pendingStarts []pendingCommandStart) (int, bool) {
	queue := indexes[key]
	for len(queue) > 0 {
		index := queue[0]
		queue = queue[1:]
		if index < 0 || index >= len(pendingStarts) || pendingStarts[index].matched {
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

func claimPendingCommandStart(
	index int,
	pendingStarts []pendingCommandStart,
	pendingByCallID map[string][]int,
	pendingByCommand map[string][]int,
) (commandEventRecord, bool) {
	if index < 0 || index >= len(pendingStarts) || pendingStarts[index].matched {
		return commandEventRecord{}, false
	}
	pendingStarts[index].matched = true
	record := pendingStarts[index].record
	removePendingIndex(pendingByCallID, strings.TrimSpace(record.CallID), index)
	removePendingIndex(pendingByCommand, commandMatchKey(record.Command), index)
	return record, true
}

func removePendingIndex(indexes map[string][]int, key string, target int) {
	if key == "" {
		return
	}
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

func completedCommandRow(start, finish commandEventRecord) inspectedCommand {
	return inspectedCommand{
		Sequence:        start.Sequence,
		Status:          "completed",
		Command:         firstNonEmpty(start.Command, finish.Command),
		ExitCode:        finish.ExitCode,
		StartSequence:   intPtr(start.Sequence),
		StartTimestamp:  timePtr(start.Timestamp),
		FinishSequence:  intPtr(finish.Sequence),
		FinishTimestamp: timePtr(finish.Timestamp),
		DurationMillis:  durationMillisForPair(start, finish),
	}
}

func incompleteCommandRow(event commandEventRecord) inspectedCommand {
	row := inspectedCommand{
		Sequence: event.Sequence,
		Command:  event.Command,
		ExitCode: event.ExitCode,
	}
	switch event.Kind {
	case "command.started":
		row.Status = "started_only"
		row.StartSequence = intPtr(event.Sequence)
		row.StartTimestamp = timePtr(event.Timestamp)
	case "command.finished":
		row.Status = "finished_only"
		row.FinishSequence = intPtr(event.Sequence)
		row.FinishTimestamp = timePtr(event.Timestamp)
		row.DurationMillis = event.DurationMillis
	default:
		row.Status = event.Kind
	}
	return row
}

func durationMillisForPair(start, finish commandEventRecord) *int {
	if finish.DurationMillis != nil {
		return intPtr(*finish.DurationMillis)
	}
	if finish.Timestamp.Before(start.Timestamp) {
		return nil
	}
	durationMillis := int(finish.Timestamp.Sub(start.Timestamp) / time.Millisecond)
	return intPtr(durationMillis)
}

func inspectionToolFailure(event schema.CanonicalEvent, commandContext string) (inspectedToolFailure, bool) {
	if event.Kind != "tool.failed" {
		return inspectedToolFailure{}, false
	}
	toolName := firstNonEmpty(
		compactText(payloadString(event.Payload, "tool_name", "tool", "name")),
		compactText(event.Actor.Name),
	)
	command := compactText(firstNonEmpty(payloadString(event.Payload, "command"), commandContext))
	exitCode, ok := payloadInt(event.Payload, "exit_code")
	record := inspectedToolFailure{
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		ToolName:  toolName,
		Command:   command,
		Summary:   compactText(summarizeEvent(event, commandContext)),
	}
	if ok {
		record.ExitCode = intPtr(exitCode)
	}
	return record, true
}

func inspectionAttentionEvent(event schema.CanonicalEvent, commandContext string) (sessionTimelineEvent, bool) {
	detail := ""
	switch event.Kind {
	case "tool.failed":
		detail = compactTextLimit(commandFailureDetail(event.Payload), 180)
	case "command.finished":
		exitCode, ok := payloadInt(event.Payload, "exit_code")
		if !ok || exitCode == 0 {
			return sessionTimelineEvent{}, false
		}
		detail = compactTextLimit(commandFailureDetail(event.Payload), 180)
	default:
		return sessionTimelineEvent{}, false
	}
	return sessionTimelineEvent{
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		Kind:      event.Kind,
		ActorKind: event.Actor.Kind,
		ActorName: event.Actor.Name,
		Summary:   compactText(summarizeEvent(event, commandContext)),
		Detail:    detail,
	}, true
}

func inspectionTimeline(event schema.CanonicalEvent, commandContext string) (sessionTimelineEvent, bool) {
	summary := compactText(summarizeEvent(event, commandContext))
	include := false
	switch event.Kind {
	case "session.started", "session.resumed", "session.ended", "model.changed",
		"command.started", "command.finished", "tool.failed", "file.patch",
		"approval.requested", "approval.resolved", "git.status", "git.diff",
		"git.commit", "git.checkout":
		include = true
	case "message.user":
		include = true
	case "message.agent", "message.system":
		include = summary != ""
	}
	if !include {
		return sessionTimelineEvent{}, false
	}
	return sessionTimelineEvent{
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		Kind:      event.Kind,
		ActorKind: event.Actor.Kind,
		ActorName: event.Actor.Name,
		Summary:   summary,
	}, true
}

func inspectionCommandContext(event schema.CanonicalEvent, commandContextByCallID map[string]string) string {
	if command := compactText(payloadString(event.Payload, "command")); command != "" {
		return command
	}
	if callID := payloadCallID(event.Payload); callID != "" {
		if command := compactText(commandContextByCallID[callID]); command != "" {
			return command
		}
	}
	return compactText(commandFromOutputHeader(event.Payload))
}

func selectTimeline(events []sessionTimelineEvent, limit int) ([]sessionTimelineEvent, int, int) {
	if limit <= 0 || len(events) <= limit {
		return append([]sessionTimelineEvent(nil), events...), 0, len(events)
	}
	headCount := (limit + 1) / 2
	tailCount := limit / 2
	selected := make([]sessionTimelineEvent, 0, headCount+tailCount)
	selected = append(selected, events[:headCount]...)
	if tailCount > 0 {
		selected = append(selected, events[len(events)-tailCount:]...)
	}
	return selected, len(events) - len(selected), headCount
}

func selectCommands(commands []inspectedCommand, limit int) ([]inspectedCommand, int, int) {
	if limit <= 0 || len(commands) <= limit {
		return append([]inspectedCommand(nil), commands...), 0, len(commands)
	}
	headCount := (limit + 1) / 2
	tailCount := limit / 2
	selected := make([]inspectedCommand, 0, headCount+tailCount)
	selected = append(selected, commands[:headCount]...)
	if tailCount > 0 {
		selected = append(selected, commands[len(commands)-tailCount:]...)
	}
	return selected, len(commands) - len(selected), headCount
}

func writeSessionInspection(target io.Writer, inspection sessionInspection) error {
	meta := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(target, "SESSION"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "session_key:\t%s\n", inspection.SessionKey); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "source:\t%s\n", inspection.SourceType); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "instance:\t%s\n", inspection.SourceInstanceID); err != nil {
		return err
	}
	if inspection.SourceSessionKey != "" {
		if _, err := fmt.Fprintf(meta, "source_session_key:\t%s\n", inspection.SourceSessionKey); err != nil {
			return err
		}
	}
	if inspection.ProjectKey != "" {
		if _, err := fmt.Fprintf(meta, "project_key:\t%s\n", inspection.ProjectKey); err != nil {
			return err
		}
	}
	if inspection.ProjectLocator != "" {
		if _, err := fmt.Fprintf(meta, "project_locator:\t%s\n", inspection.ProjectLocator); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(meta, "first:\t%s\n", inspection.FirstTimestamp.UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "last:\t%s\n", inspection.LastTimestamp.UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "duration:\t%s\n", (inspection.LastTimestamp.Sub(inspection.FirstTimestamp)).Round(time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "events:\t%d\n", inspection.EventCount); err != nil {
		return err
	}
	if err := meta.Flush(); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(target, "\nKINDS"); err != nil {
		return err
	}
	kindWriter := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(kindWriter, "KIND\tCOUNT"); err != nil {
		return err
	}
	for _, item := range inspection.KindCounts {
		if _, err := fmt.Fprintf(kindWriter, "%s\t%d\n", item.Kind, item.Count); err != nil {
			return err
		}
	}
	if err := kindWriter.Flush(); err != nil {
		return err
	}

	if inspection.CommandFilter == "" || inspection.CommandFilter == commandFilterAll {
		if _, err := fmt.Fprintln(target, "\nCOMMANDS"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(target, "\nCOMMANDS (filter: %s)\n", inspection.CommandFilter); err != nil {
			return err
		}
	}
	if len(inspection.Commands) == 0 {
		message := "none"
		if inspection.CommandFilter != "" && inspection.CommandFilter != commandFilterAll {
			message = "none matching filter"
		}
		if _, err := fmt.Fprintln(target, message); err != nil {
			return err
		}
	} else {
		commandWriter := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(commandWriter, "SEQ\tSTART\tEND\tSTATUS\tEXIT\tDURATION\tCOMMAND"); err != nil {
			return err
		}
		for idx, command := range inspection.Commands {
			if inspection.OmittedCommands > 0 && idx == inspection.commandHeadCount {
				if _, err := fmt.Fprintf(
					commandWriter,
					"...\t...\t...\t...\t%d command rows omitted; rerun with -command-limit 0 for all commands\n",
					inspection.OmittedCommands,
				); err != nil {
					return err
				}
			}
			exit := ""
			if command.ExitCode != nil {
				exit = strconv.Itoa(*command.ExitCode)
			}
			if _, err := fmt.Fprintf(
				commandWriter,
				"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				commandSequenceLabel(command),
				formatOptionalTimestamp(command.StartTimestamp),
				formatOptionalTimestamp(command.FinishTimestamp),
				command.Status,
				exit,
				formatCommandDuration(command.DurationMillis),
				command.Command,
			); err != nil {
				return err
			}
		}
		if err := commandWriter.Flush(); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(target, "\nTOOL FAILURES"); err != nil {
		return err
	}
	if len(inspection.ToolFailures) == 0 {
		if _, err := fmt.Fprintln(target, "none"); err != nil {
			return err
		}
	} else {
		failureWriter := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(failureWriter, "SEQ\tTIME\tTOOL\tEXIT\tSUMMARY"); err != nil {
			return err
		}
		for _, failure := range inspection.ToolFailures {
			exit := ""
			if failure.ExitCode != nil {
				exit = strconv.Itoa(*failure.ExitCode)
			}
			if _, err := fmt.Fprintf(
				failureWriter,
				"%d\t%s\t%s\t%s\t%s\n",
				failure.Sequence,
				failure.Timestamp.UTC().Format(time.RFC3339),
				failure.ToolName,
				exit,
				failure.Summary,
			); err != nil {
				return err
			}
		}
		if err := failureWriter.Flush(); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(target, "\nATTENTION EVENTS"); err != nil {
		return err
	}
	if len(inspection.AttentionEvents) == 0 {
		if _, err := fmt.Fprintln(target, "none"); err != nil {
			return err
		}
	} else {
		attentionWriter := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(attentionWriter, "SEQ\tTIME\tKIND\tACTOR\tSUMMARY\tDETAIL"); err != nil {
			return err
		}
		for _, event := range inspection.AttentionEvents {
			if _, err := fmt.Fprintf(
				attentionWriter,
				"%d\t%s\t%s\t%s\t%s\t%s\n",
				event.Sequence,
				event.Timestamp.UTC().Format(time.RFC3339),
				event.Kind,
				formatActor(event.ActorKind, event.ActorName),
				event.Summary,
				event.Detail,
			); err != nil {
				return err
			}
		}
		if err := attentionWriter.Flush(); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(target, "\nTIMELINE"); err != nil {
		return err
	}
	if len(inspection.Timeline) == 0 {
		_, err := fmt.Fprintln(target, "none")
		return err
	}
	timelineWriter := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(timelineWriter, "SEQ\tTIME\tKIND\tACTOR\tSUMMARY"); err != nil {
		return err
	}
	for idx, event := range inspection.Timeline {
		if inspection.OmittedTimelineEvents > 0 && idx == inspection.timelineHeadCount {
			if _, err := fmt.Fprintf(
				timelineWriter,
				"...\t...\t...\t...\t%d representative events omitted\n",
				inspection.OmittedTimelineEvents,
			); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(
			timelineWriter,
			"%d\t%s\t%s\t%s\t%s\n",
			event.Sequence,
			event.Timestamp.UTC().Format(time.RFC3339),
			event.Kind,
			formatActor(event.ActorKind, event.ActorName),
			event.Summary,
		); err != nil {
			return err
		}
	}
	return timelineWriter.Flush()
}

func summarizeEvent(event schema.CanonicalEvent, commandContext string) string {
	switch event.Kind {
	case "message.user", "message.agent", "message.system":
		return firstNonEmpty(
			payloadString(event.Payload, "text"),
			payloadString(event.Payload, "message"),
			payloadString(event.Payload, "reason"),
		)
	case "command.started", "command.finished":
		command := firstNonEmpty(payloadString(event.Payload, "command"), commandContext)
		if exitCode, ok := payloadInt(event.Payload, "exit_code"); ok {
			if command == "" {
				return fmt.Sprintf("exit %d", exitCode)
			}
			return fmt.Sprintf("%s (exit %d)", command, exitCode)
		}
		return command
	case "tool.failed":
		return joinSummary(
			firstNonEmpty(payloadString(event.Payload, "tool_name", "tool", "name"), event.Actor.Name),
			firstNonEmpty(payloadString(event.Payload, "command"), commandContext),
			exitSummary(event.Payload),
			firstNonEmpty(
				payloadString(event.Payload, "reason"),
				payloadString(event.Payload, "output_text"),
				payloadString(event.Payload, "text"),
			),
		)
	case "file.patch":
		return joinSummary(
			firstNonEmpty(payloadString(event.Payload, "tool_name", "tool", "name"), event.Actor.Name),
			payloadString(event.Payload, "command"),
		)
	case "model.changed":
		return firstNonEmpty(
			payloadString(event.Payload, "model"),
			payloadString(event.Payload, "text"),
		)
	default:
		return firstNonEmpty(
			payloadString(event.Payload, "text"),
			payloadString(event.Payload, "message"),
			payloadString(event.Payload, "command"),
			payloadString(event.Payload, "reason"),
		)
	}
}

func commandFailureDetail(payload map[string]any) string {
	output := commandFailureOutput(payload)
	if strings.TrimSpace(output) == "" {
		return ""
	}
	return extractOutputExcerpt(output)
}

func commandFailureOutput(payload map[string]any) string {
	if nested := payloadMap(payload, "payload"); nested != nil {
		if output := firstNonEmpty(
			payloadString(nested, "output"),
			payloadString(nested, "output_text"),
			payloadString(nested, "stderr"),
			payloadString(nested, "stdout"),
			payloadString(nested, "text"),
			payloadString(nested, "reason"),
		); strings.TrimSpace(output) != "" {
			return output
		}
	}
	return firstNonEmpty(
		payloadString(payload, "output_text"),
		payloadString(payload, "stderr"),
		payloadString(payload, "stdout"),
		payloadString(payload, "text"),
		payloadString(payload, "reason"),
	)
}

func extractOutputExcerpt(raw string) string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if idx := strings.LastIndex(raw, "\nOutput:\n"); idx != -1 {
		tail := strings.TrimSpace(raw[idx+len("\nOutput:\n"):])
		if tail != "" {
			raw = tail
		}
	}
	lines := strings.Split(raw, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || isOutputMetadataLine(line) {
			continue
		}
		cleaned = append(cleaned, line)
	}
	if len(cleaned) == 0 {
		return ""
	}
	for idx, line := range cleaned {
		if looksLikeErrorDetail(line) {
			if idx+1 < len(cleaned) && shouldAppendContinuation(line, cleaned[idx+1]) {
				return strings.TrimSpace(line + " " + cleaned[idx+1])
			}
			return line
		}
	}
	if len(cleaned) > 1 && strings.HasSuffix(cleaned[0], ":") {
		return strings.TrimSpace(cleaned[0] + " " + cleaned[1])
	}
	return cleaned[0]
}

func isOutputMetadataLine(line string) bool {
	switch {
	case strings.HasPrefix(line, "Command:"),
		strings.HasPrefix(line, "Chunk ID:"),
		strings.HasPrefix(line, "Wall time:"),
		strings.HasPrefix(line, "Process exited with code"),
		strings.HasPrefix(line, "Process exited with exit code"),
		strings.HasPrefix(line, "Original token count:"),
		strings.HasPrefix(line, "Output:"),
		strings.HasPrefix(line, "stdout:"),
		strings.HasPrefix(line, "stderr:"),
		strings.HasPrefix(line, "Session ID:"):
		return true
	default:
		return false
	}
}

func looksLikeErrorDetail(line string) bool {
	lower := strings.ToLower(line)
	for _, needle := range []string{
		"error",
		"failed",
		"failure",
		"fatal",
		"missing",
		"panic",
		"unknown",
		"not found",
		"not installed",
		"cannot",
		"can't",
		"denied",
		"timeout",
		"timed out",
		"undefined",
		"invalid",
		"no required module",
		"no such file",
		"traceback",
		"exception",
		"cannot find",
		"permission denied",
	} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return strings.HasPrefix(lower, "/bin/") ||
		strings.HasPrefix(lower, "bash:") ||
		strings.HasPrefix(lower, "sh:") ||
		strings.HasPrefix(lower, "sudo:")
}

func shouldAppendContinuation(line, next string) bool {
	if strings.HasSuffix(line, ":") {
		return true
	}
	nextLower := strings.ToLower(next)
	for _, prefix := range []string{"run ", "go ", "npm ", "pnpm ", "yarn ", "cargo "} {
		if strings.HasPrefix(nextLower, prefix) {
			return true
		}
	}
	return false
}

func exitSummary(payload map[string]any) string {
	exitCode, ok := payloadInt(payload, "exit_code")
	if !ok {
		return ""
	}
	return fmt.Sprintf("exit %d", exitCode)
}

func joinSummary(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = compactText(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return strings.Join(out, " — ")
}

func payloadCallID(payload map[string]any) string {
	nested := payloadMap(payload, "payload")
	if nested == nil {
		return ""
	}
	return strings.TrimSpace(payloadString(nested, "call_id"))
}

func commandFromOutputHeader(payload map[string]any) string {
	output := commandFailureOutput(payload)
	if strings.TrimSpace(output) == "" {
		return ""
	}
	output = strings.ReplaceAll(output, "\r\n", "\n")
	firstLine := output
	if idx := strings.Index(firstLine, "\n"); idx != -1 {
		firstLine = firstLine[:idx]
	}
	firstLine = strings.TrimSpace(firstLine)
	if !strings.HasPrefix(firstLine, "Command:") {
		return ""
	}
	command := strings.TrimSpace(strings.TrimPrefix(firstLine, "Command:"))
	return normalizeCommandHeader(command)
}

func normalizeCommandHeader(command string) string {
	command = strings.TrimSpace(command)
	for _, prefix := range []string{"/bin/bash -lc ", "bash -lc ", "/bin/sh -lc ", "sh -lc "} {
		if strings.HasPrefix(command, prefix) {
			command = strings.TrimSpace(strings.TrimPrefix(command, prefix))
			break
		}
	}
	if len(command) >= 2 {
		switch {
		case command[0] == '"' && command[len(command)-1] == '"':
			if unquoted, err := strconv.Unquote(command); err == nil {
				command = unquoted
			}
		case command[0] == '\'' && command[len(command)-1] == '\'':
			command = command[1 : len(command)-1]
		}
	}
	return strings.TrimSpace(command)
}

func payloadString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				return typed
			}
		default:
			return fmt.Sprint(typed)
		}
	}
	return ""
}

func payloadMap(payload map[string]any, key string) map[string]any {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil
	}
	typed, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return typed
}

func payloadInt(payload map[string]any, key string) (int, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float32:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		value, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(value), true
	case string:
		value, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
}

func compactText(value string) string {
	return compactTextLimit(value, 140)
}

func compactTextLimit(value string, limit int) string {
	value = strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit-3]) + "..."
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func intPtr(value int) *int {
	v := value
	return &v
}

func timePtr(value time.Time) *time.Time {
	v := value
	return &v
}

func commandSequenceLabel(command inspectedCommand) string {
	switch {
	case command.StartSequence != nil && command.FinishSequence != nil:
		return fmt.Sprintf("%d-%d", *command.StartSequence, *command.FinishSequence)
	case command.StartSequence != nil:
		return fmt.Sprintf("%d-", *command.StartSequence)
	case command.FinishSequence != nil:
		return fmt.Sprintf("-%d", *command.FinishSequence)
	default:
		return strconv.Itoa(command.Sequence)
	}
}

func formatOptionalTimestamp(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func formatCommandDuration(durationMillis *int) string {
	if durationMillis == nil {
		return ""
	}
	return (time.Duration(*durationMillis) * time.Millisecond).String()
}

func formatActor(kind, name string) string {
	actor := strings.TrimSpace(strings.Join([]string{kind, name}, ":"))
	return strings.Trim(actor, ":")
}

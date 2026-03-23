package normalize

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type SequenceStore interface {
	GetSessionSequence(sessionKey string) (int, error)
	SetSessionSequence(sessionKey string, sequence int) error
}

type Service struct {
	sequences SequenceStore
	mu        sync.Mutex
	callNames map[string]string
}

func NewService(sequences SequenceStore) *Service {
	return &Service{
		sequences: sequences,
		callNames: map[string]string{},
	}
}

func (s *Service) SeedCallName(callID, toolName string) {
	callID = strings.TrimSpace(callID)
	toolName = strings.TrimSpace(toolName)
	if callID == "" || toolName == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callNames[callID] = toolName
}

func (s *Service) Normalize(record ledger.Record) (schema.CanonicalEvent, error) {
	envelope := schema.EnsureEnvelopeID(record.Envelope)
	payload, parseErr := decodePayload(envelope.RawPayload)
	sessionSourceKey := firstNonEmpty(
		envelope.ParseHints.SourceSessionKey,
		stringValue(payload, "session_id"),
		stringValue(payload, "conversation_id"),
		strings.TrimSpace(envelope.ArtifactID),
	)
	if sessionSourceKey == "" {
		sessionSourceKey = "unknown"
	}
	sessionKey := schema.StableSessionKey(envelope.SourceInstanceID, sessionSourceKey)
	sequence, err := s.nextSequence(sessionKey)
	if err != nil {
		return schema.CanonicalEvent{}, err
	}
	projectLocator := firstNonEmpty(
		envelope.ProjectLocator,
		envelope.ParseHints.ProjectHint,
		stringValue(payload, "project"),
		stringValue(payload, "workspace"),
		stringValue(payload, "repo"),
	)
	timestamp := envelope.ObservedAt
	if envelope.SourceTimestamp != nil && !envelope.SourceTimestamp.IsZero() {
		timestamp = envelope.SourceTimestamp.UTC()
	}
	kind, actor, parseStatus, payloadMap := s.classify(envelope, payload, parseErr)
	event := schema.CanonicalEvent{
		EventVersion:     schema.CanonicalEventVersion,
		SourceType:       envelope.SourceType,
		SourceInstanceID: envelope.SourceInstanceID,
		SessionKey:       sessionKey,
		Sequence:         sequence,
		Timestamp:        timestamp,
		Kind:             kind,
		Actor:            actor,
		Context: schema.EventContext{
			SourceType:       envelope.SourceType,
			ProjectKey:       schema.StableProjectKey(projectLocator),
			ProjectLocator:   projectLocator,
			SourceSessionKey: sessionSourceKey,
			Capabilities:     append([]string(nil), envelope.ParseHints.Capabilities...),
		},
		Payload: payloadMap,
		RawRef: schema.RawRef{
			LedgerOffset: record.Offset,
			EnvelopeID:   envelope.EnvelopeID,
		},
		ParseStatus: parseStatus,
	}
	event = schema.EnsureEventID(event)
	return event, nil
}

type MemorySequenceStore struct {
	mu   sync.Mutex
	data map[string]int
}

func NewMemorySequenceStore() *MemorySequenceStore {
	return &MemorySequenceStore{data: map[string]int{}}
}

func (m *MemorySequenceStore) GetSessionSequence(sessionKey string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[sessionKey], nil
}

func (m *MemorySequenceStore) SetSessionSequence(sessionKey string, sequence int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[sessionKey] = sequence
	return nil
}

func (s *Service) nextSequence(sessionKey string) (int, error) {
	current, err := s.sequences.GetSessionSequence(sessionKey)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := s.sequences.SetSessionSequence(sessionKey, next); err != nil {
		return 0, err
	}
	return next, nil
}

func decodePayload(raw json.RawMessage) (map[string]any, error) {
	var payload map[string]any
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return map[string]any{"raw_payload": string(raw)}, err
	}
	return payload, nil
}

var (
	commandLinePattern = regexp.MustCompile(`Command:\s+[^\n]*-lc '([^']*)'`)
	exitCodePattern    = regexp.MustCompile(`Process exited with code (\d+)`)
)

func (s *Service) classify(envelope schema.RawEnvelope, payload map[string]any, parseErr error) (string, schema.Actor, string, map[string]any) {
	if parseErr != nil {
		return "error", schema.Actor{Kind: "system", Name: envelope.SourceType}, "failed", map[string]any{
			"reason":      parseErr.Error(),
			"raw_kind":    envelope.RawKind,
			"source_type": envelope.SourceType,
		}
	}

	if envelope.SourceType == "codex_local" {
		return s.classifyCodex(envelope, payload)
	}

	return genericClassify(envelope, payload)
}

func genericClassify(envelope schema.RawEnvelope, payload map[string]any) (string, schema.Actor, string, map[string]any) {
	kind := canonicalKind(envelope.RawKind, payload)
	status := "ok"
	if kind == "error" {
		status = "failed"
	}
	return kind, actorForKind(kind, envelope, payload), status, schema.CloneMap(payload)
}

func (s *Service) classifyCodex(envelope schema.RawEnvelope, payload map[string]any) (string, schema.Actor, string, map[string]any) {
	topType := stringValue(payload, "type")
	nested := mapValue(payload, "payload")
	nestedType := stringValue(nested, "type")
	payloadMap := schema.CloneMap(payload)
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}

	var (
		kind     string
		toolName string
		command  string
		exitCode *int
	)

	switch topType {
	case "session_meta":
		kind = "session.started"
	case "turn_context":
		kind = "model.changed"
	case "compacted":
		kind = "message.system"
	case "event_msg":
		kind = codexEventKind(nestedType)
	case "response_item":
		kind, toolName, command, exitCode = s.codexResponseKind(nestedType, nested)
	default:
		kind = canonicalKind(firstNonEmpty(topType, nestedType), nested)
		if kind == "error" {
			kind = canonicalKind(topType, payload)
		}
	}

	if kind == "" {
		kind = "error"
	}
	if text := codexMessageText(topType, nestedType, nested); text != "" {
		payloadMap["text"] = text
	}
	if toolName != "" {
		payloadMap["tool_name"] = toolName
	}
	if command != "" {
		payloadMap["command"] = command
	}
	if exitCode != nil {
		payloadMap["exit_code"] = *exitCode
	}
	if nestedType == "token_count" {
		payloadMap["usage"] = mapValue(nested, "info")
	}

	status := "ok"
	if kind == "error" {
		status = "failed"
	}
	return kind, codexActor(kind, envelope, nested, toolName), status, payloadMap
}

func codexEventKind(nestedType string) string {
	switch nestedType {
	case "user_message":
		return "message.user"
	case "agent_message":
		return "message.agent"
	case "token_count":
		return "usage.reported"
	case "task_started", "task_complete", "context_compacted", "turn_aborted", "item_completed":
		return "message.system"
	default:
		kind := canonicalKind(nestedType, nil)
		if kind == "error" {
			return "message.system"
		}
		return kind
	}
}

func (s *Service) codexResponseKind(nestedType string, nested map[string]any) (string, string, string, *int) {
	switch nestedType {
	case "message":
		switch stringValue(nested, "role") {
		case "assistant":
			return "message.agent", "", "", nil
		case "user":
			return "message.user", "", "", nil
		default:
			return "message.system", "", "", nil
		}
	case "reasoning":
		return "message.agent", "", "", nil
	case "function_call", "custom_tool_call":
		toolName := codexToolName(nestedType, nested, "")
		if callID := stringValue(nested, "call_id"); callID != "" {
			s.storeCallName(callID, toolName)
		}
		arguments := parseJSONObjectString(firstNonEmpty(stringValue(nested, "arguments"), stringValue(nested, "input")))
		command := firstNonEmpty(stringValue(arguments, "cmd"), stringValue(arguments, "command"))
		switch toolName {
		case "exec_command":
			return "command.started", toolName, command, nil
		case "apply_patch":
			return "file.patch", toolName, command, nil
		default:
			return "tool.started", toolName, command, nil
		}
	case "function_call_output", "custom_tool_call_output":
		callID := stringValue(nested, "call_id")
		toolName := codexToolName(nestedType, nested, s.lookupCallName(callID))
		outputText, command, parsedExit, hasExit := parseToolOutput(stringValue(nested, "output"))
		if command == "" {
			command = firstNonEmpty(stringValue(nested, "command"), command)
		}
		if toolName == "" {
			toolName = firstNonEmpty(stringValue(nested, "name"), "tool")
		}
		if outputText != "" {
			nested["output_text"] = outputText
		}
		if toolName == "exec_command" {
			if hasExit {
				return "command.finished", toolName, command, &parsedExit
			}
			return "command.finished", toolName, command, nil
		}
		if toolName == "apply_patch" {
			if hasExit {
				return "file.patch", toolName, command, &parsedExit
			}
			return "file.patch", toolName, command, nil
		}
		if hasExit && parsedExit > 0 {
			return "tool.failed", toolName, command, &parsedExit
		}
		if hasExit {
			return "tool.finished", toolName, command, &parsedExit
		}
		return "tool.finished", toolName, command, nil
	case "web_search_call":
		if stringValue(nested, "status") == "completed" {
			return "tool.finished", "web_search", "", nil
		}
		return "tool.started", "web_search", "", nil
	default:
		return canonicalKind(nestedType, nested), "", "", nil
	}
}

func codexActor(kind string, envelope schema.RawEnvelope, nested map[string]any, toolName string) schema.Actor {
	switch kind {
	case "message.user":
		return schema.Actor{Kind: "user", Name: "user"}
	case "message.agent":
		return schema.Actor{Kind: "agent", Name: firstNonEmpty(stringValue(nested, "role"), envelope.SourceType)}
	case "message.system", "session.started", "model.changed", "usage.reported":
		return schema.Actor{Kind: "system", Name: envelope.SourceType}
	case "command.started", "command.finished":
		name := firstNonEmpty(toolName, stringValue(nested, "name"), stringValue(nested, "tool"))
		if name == "" || name == "exec_command" {
			name = "shell"
		}
		return schema.Actor{Kind: "tool", Name: name}
	case "tool.started", "tool.finished", "tool.failed", "file.patch":
		return schema.Actor{Kind: "tool", Name: firstNonEmpty(toolName, stringValue(nested, "name"), stringValue(nested, "tool"), "tool")}
	default:
		return schema.Actor{Kind: "system", Name: envelope.SourceType}
	}
}

func codexToolName(nestedType string, nested map[string]any, fallback string) string {
	switch nestedType {
	case "web_search_call":
		return "web_search"
	default:
		return firstNonEmpty(
			stringValue(nested, "name"),
			stringValue(nested, "tool_name"),
			stringValue(nested, "tool"),
			fallback,
		)
	}
}

func codexMessageText(topType, nestedType string, nested map[string]any) string {
	switch topType {
	case "event_msg":
		return stringValue(nested, "message")
	case "response_item":
		if nestedType == "message" {
			return extractContentText(nested["content"])
		}
	}
	return ""
}

func extractContentText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			entry, ok := item.(map[string]any)
			if !ok {
				continue
			}
			text := firstNonEmpty(
				stringValue(entry, "text"),
				stringValue(entry, "input_text"),
				stringValue(entry, "output_text"),
			)
			if text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func parseJSONObjectString(input string) map[string]any {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(input), &payload); err != nil {
		return nil
	}
	return payload
}

func parseToolOutput(raw string) (string, string, int, bool) {
	outputText := raw
	exitCode, hasExit := 0, false
	if wrapped := parseJSONObjectString(raw); wrapped != nil {
		if text := stringValue(wrapped, "output"); text != "" {
			outputText = text
		}
		if metadata := mapValue(wrapped, "metadata"); metadata != nil {
			if value, ok := intValue(metadata, "exit_code"); ok {
				exitCode, hasExit = value, true
			}
		}
	}

	command := ""
	if matches := commandLinePattern.FindStringSubmatch(outputText); len(matches) == 2 {
		command = matches[1]
	}
	if matches := exitCodePattern.FindStringSubmatch(outputText); len(matches) == 2 {
		if value, err := strconv.Atoi(matches[1]); err == nil {
			exitCode, hasExit = value, true
		}
	}
	return outputText, command, exitCode, hasExit
}

func (s *Service) storeCallName(callID, name string) {
	if callID == "" || name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callNames[callID] = name
}

func (s *Service) lookupCallName(callID string) string {
	if callID == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callNames[callID]
}

func canonicalKind(rawKind string, payload map[string]any) string {
	kind := firstNonEmpty(rawKind, stringValue(payload, "type"), stringValue(payload, "kind"))
	switch kind {
	case "session.started", "session_start":
		return "session.started"
	case "session.resumed", "session_resume":
		return "session.resumed"
	case "session.ended", "session_end":
		return "session.ended"
	case "message.user", "user":
		return "message.user"
	case "message.agent", "assistant", "agent":
		return "message.agent"
	case "message.system", "system":
		return "message.system"
	case "tool.started", "tool_use":
		return "tool.started"
	case "tool.finished":
		return "tool.finished"
	case "tool.failed":
		return "tool.failed"
	case "command.started":
		return "command.started"
	case "command.finished":
		return "command.finished"
	case "file.read":
		return "file.read"
	case "file.write", "file_edit":
		return "file.write"
	case "file.patch":
		return "file.patch"
	case "file.delete":
		return "file.delete"
	case "approval.requested", "approval_request":
		return "approval.requested"
	case "approval.resolved", "approval_result":
		return "approval.resolved"
	case "git.status":
		return "git.status"
	case "git.diff":
		return "git.diff"
	case "git.commit":
		return "git.commit"
	case "git.checkout":
		return "git.checkout"
	case "model.changed":
		return "model.changed"
	case "usage.reported":
		return "usage.reported"
	case "tool_result":
		if numberValue(payload, "exit_code") > 0 {
			return "tool.failed"
		}
		return "tool.finished"
	default:
		return "error"
	}
}

func actorForKind(kind string, envelope schema.RawEnvelope, payload map[string]any) schema.Actor {
	switch {
	case strings.HasPrefix(kind, "message.user"):
		return schema.Actor{Kind: "user", Name: "user"}
	case strings.HasPrefix(kind, "message.agent"):
		return schema.Actor{Kind: "agent", Name: envelope.SourceType}
	case strings.HasPrefix(kind, "tool.") || strings.HasPrefix(kind, "command."):
		name := firstNonEmpty(stringValue(payload, "tool"), "shell")
		return schema.Actor{Kind: "tool", Name: name}
	case strings.HasPrefix(kind, "session."):
		return schema.Actor{Kind: "system", Name: envelope.SourceType}
	default:
		return schema.Actor{Kind: "system", Name: envelope.SourceType}
	}
}

func stringValue(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func numberValue(payload map[string]any, key string) int {
	value, _ := intValue(payload, key)
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func ParseTimestamp(fields map[string]any, keys ...string) *time.Time {
	for _, key := range keys {
		raw := stringValue(fields, key)
		if raw == "" {
			continue
		}
		timestamp, err := time.Parse(time.RFC3339Nano, raw)
		if err == nil {
			parsed := timestamp.UTC()
			return &parsed
		}
	}
	return nil
}

func mapValue(payload map[string]any, key string) map[string]any {
	if payload == nil {
		return nil
	}
	value, ok := payload[key]
	if !ok {
		return nil
	}
	mapped, _ := value.(map[string]any)
	return mapped
}

func intValue(payload map[string]any, key string) (int, bool) {
	if payload == nil {
		return 0, false
	}
	value, ok := payload[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case int64:
		return int(typed), true
	default:
		return 0, false
	}
}

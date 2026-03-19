package sourceutil

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type RecordInfo struct {
	RawKind          string
	SourceSessionKey string
	ProjectLocator   string
	SourceTimestamp  *time.Time
}

func DiscoverJSONArtifacts(_ context.Context, root string) ([]sourceapi.Artifact, error) {
	var artifacts []sourceapi.Artifact
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		switch ext {
		case ".json", ".jsonl", ".ndjson":
		default:
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		artifacts = append(artifacts, sourceapi.Artifact{
			ID:             schema.StableID("art", relative),
			Locator:        path,
			ProjectLocator: root,
			Fingerprint:    schema.StableID("fp", relative, strconv.FormatInt(info.Size(), 10), info.ModTime().UTC().Format(time.RFC3339Nano)),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return artifacts, nil
}

func ReadArtifact(
	sourceType string,
	cfg sourceapi.Config,
	artifact sourceapi.Artifact,
	checkpoint sourceapi.Checkpoint,
	resolver func(map[string]any) RecordInfo,
	capabilities []string,
) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	ext := strings.ToLower(filepath.Ext(artifact.Locator))
	switch ext {
	case ".json":
		return readJSONFile(sourceType, cfg, artifact, checkpoint, resolver, capabilities)
	case ".jsonl", ".ndjson":
		return readJSONLines(sourceType, cfg, artifact, checkpoint, resolver, capabilities)
	default:
		return nil, checkpoint, errors.New("unsupported artifact extension: " + ext)
	}
}

func readJSONLines(
	sourceType string,
	cfg sourceapi.Config,
	artifact sourceapi.Artifact,
	checkpoint sourceapi.Checkpoint,
	resolver func(map[string]any) RecordInfo,
	capabilities []string,
) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	file, err := os.Open(artifact.Locator)
	if err != nil {
		return nil, checkpoint, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, checkpoint, err
	}

	startLine, _ := strconv.Atoi(checkpoint.Cursor)
	reader := bufio.NewReader(file)

	var (
		lineNo         int
		envelopes      []schema.RawEnvelope
		currentSession string
		currentProject string
	)
	for {
		rawLine, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(rawLine) == 0 {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, checkpoint, err
		}
		lineNo++
		if lineNo <= startLine {
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		line := strings.TrimSpace(string(rawLine))
		if line == "" {
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		payload, fields, info := buildPayload([]byte(line), resolver)
		if info.SourceSessionKey != "" {
			currentSession = info.SourceSessionKey
		}
		if info.ProjectLocator != "" {
			currentProject = info.ProjectLocator
		}
		envelopes = append(envelopes, schema.EnsureEnvelopeID(schema.RawEnvelope{
			EnvelopeVersion:  schema.RawEnvelopeVersion,
			SourceType:       sourceType,
			SourceInstanceID: cfg.InstanceID,
			ArtifactID:       artifact.ID,
			ArtifactLocator:  artifact.Locator,
			ProjectLocator:   firstNonEmpty(info.ProjectLocator, currentProject, artifact.ProjectLocator),
			Cursor:           schema.Cursor{Kind: "line", Value: strconv.Itoa(lineNo)},
			ObservedAt:       time.Now().UTC(),
			SourceTimestamp:  info.SourceTimestamp,
			RawKind:          firstNonEmpty(info.RawKind, stringValue(fields, "type"), stringValue(fields, "kind"), "unknown"),
			RawPayload:       payload,
			ContentHash:      schema.HashBytes(payload),
			ParseHints: schema.ParseHints{
				SourceSessionKey: firstNonEmpty(info.SourceSessionKey, currentSession, filepath.Base(artifact.Locator)),
				ProjectHint:      firstNonEmpty(info.ProjectLocator, currentProject, artifact.ProjectLocator),
				Confidence:       0.8,
				Capabilities:     append([]string(nil), capabilities...),
			},
		}))
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return envelopes, sourceapi.Checkpoint{
		Cursor:              strconv.Itoa(lineNo),
		ArtifactFingerprint: artifact.Fingerprint,
		LastObservedSize:    info.Size(),
		LastObservedModTime: info.ModTime().UTC(),
	}, nil
}

func readJSONFile(
	sourceType string,
	cfg sourceapi.Config,
	artifact sourceapi.Artifact,
	checkpoint sourceapi.Checkpoint,
	resolver func(map[string]any) RecordInfo,
	capabilities []string,
) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	data, err := os.ReadFile(artifact.Locator)
	if err != nil {
		return nil, checkpoint, err
	}
	var items []json.RawMessage
	if len(data) > 0 && data[0] == '[' {
		if err := json.Unmarshal(data, &items); err != nil {
			return nil, checkpoint, err
		}
	} else {
		items = []json.RawMessage{data}
	}
	startIndex, _ := strconv.Atoi(checkpoint.Cursor)
	envelopes := make([]schema.RawEnvelope, 0, len(items))
	currentSession := ""
	currentProject := ""
	for idx, item := range items {
		if idx < startIndex {
			continue
		}
		payload, fields, info := buildPayload(item, resolver)
		if info.SourceSessionKey != "" {
			currentSession = info.SourceSessionKey
		}
		if info.ProjectLocator != "" {
			currentProject = info.ProjectLocator
		}
		envelopes = append(envelopes, schema.EnsureEnvelopeID(schema.RawEnvelope{
			EnvelopeVersion:  schema.RawEnvelopeVersion,
			SourceType:       sourceType,
			SourceInstanceID: cfg.InstanceID,
			ArtifactID:       artifact.ID,
			ArtifactLocator:  artifact.Locator,
			ProjectLocator:   firstNonEmpty(info.ProjectLocator, currentProject, artifact.ProjectLocator),
			Cursor:           schema.Cursor{Kind: "item", Value: strconv.Itoa(idx + 1)},
			ObservedAt:       time.Now().UTC(),
			SourceTimestamp:  info.SourceTimestamp,
			RawKind:          firstNonEmpty(info.RawKind, stringValue(fields, "type"), stringValue(fields, "kind"), "unknown"),
			RawPayload:       payload,
			ContentHash:      schema.HashBytes(payload),
			ParseHints: schema.ParseHints{
				SourceSessionKey: firstNonEmpty(info.SourceSessionKey, currentSession, filepath.Base(artifact.Locator)),
				ProjectHint:      firstNonEmpty(info.ProjectLocator, currentProject, artifact.ProjectLocator),
				Confidence:       0.8,
				Capabilities:     append([]string(nil), capabilities...),
			},
		}))
	}
	info, err := os.Stat(artifact.Locator)
	if err != nil {
		return nil, checkpoint, err
	}
	return envelopes, sourceapi.Checkpoint{
		Cursor:              strconv.Itoa(len(items)),
		ArtifactFingerprint: artifact.Fingerprint,
		LastObservedSize:    info.Size(),
		LastObservedModTime: info.ModTime().UTC(),
	}, nil
}

func buildPayload(raw []byte, resolver func(map[string]any) RecordInfo) (json.RawMessage, map[string]any, RecordInfo) {
	var fields map[string]any
	if json.Valid(raw) {
		_ = json.Unmarshal(raw, &fields)
		info := resolver(fields)
		if info.SourceTimestamp == nil {
			info.SourceTimestamp = normalize.ParseTimestamp(fields, "timestamp", "created_at", "time")
		}
		return append(json.RawMessage(nil), raw...), fields, info
	}
	fields = map[string]any{"raw_text": string(raw)}
	info := resolver(fields)
	return mustJSON(fields), fields, info
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return data
}

func stringValue(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

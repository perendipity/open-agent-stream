package codex

import (
	"context"

	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/sourceutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() string {
	return "codex_local"
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

func (a *Adapter) Discover(ctx context.Context, cfg sourceapi.Config) ([]sourceapi.Artifact, error) {
	return sourceutil.DiscoverJSONArtifacts(ctx, cfg.Root)
}

func (a *Adapter) Read(ctx context.Context, cfg sourceapi.Config, artifact sourceapi.Artifact, checkpoint sourceapi.Checkpoint) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	_ = ctx
	return sourceutil.ReadArtifact(a.Type(), cfg, artifact, checkpoint, func(fields map[string]any) sourceutil.RecordInfo {
		nested := mapValue(fields, "payload")
		timestamp := normalize.ParseTimestamp(fields, "timestamp", "created_at", "time")
		if timestamp == nil {
			timestamp = normalize.ParseTimestamp(nested, "timestamp", "created_at", "time")
		}
		return sourceutil.RecordInfo{
			RawKind: stringValue(fields, "type"),
			SourceSessionKey: firstNonEmpty(
				stringValue(fields, "session_id"),
				stringValue(fields, "conversation_id"),
				stringValue(nested, "id"),
				stringValue(nested, "session_id"),
				stringValue(nested, "conversation_id"),
			),
			ProjectLocator: firstNonEmpty(
				stringValue(fields, "project"),
				stringValue(fields, "workspace"),
				stringValue(fields, "repo"),
				stringValue(nested, "cwd"),
				stringValue(nested, "project"),
				stringValue(nested, "workspace"),
				stringValue(nested, "repo"),
			),
			SourceTimestamp: timestamp,
		}
	}, capabilitiesToStrings(a.Capabilities()))
}

func capabilitiesToStrings(values []sourceapi.Capability) []string {
	out := make([]string, len(values))
	for idx := range values {
		out[idx] = string(values[idx])
	}
	return out
}

func stringValue(fields map[string]any, key string) string {
	if fields == nil {
		return ""
	}
	value, ok := fields[key]
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func mapValue(fields map[string]any, key string) map[string]any {
	if fields == nil {
		return nil
	}
	value, ok := fields[key]
	if !ok {
		return nil
	}
	mapped, _ := value.(map[string]any)
	return mapped
}

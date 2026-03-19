package claude

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
	return "claude_local"
}

func (a *Adapter) Capabilities() []sourceapi.Capability {
	return []sourceapi.Capability{
		sourceapi.CapabilityMessages,
		sourceapi.CapabilityCommands,
		sourceapi.CapabilityToolCalls,
		sourceapi.CapabilityUsage,
	}
}

func (a *Adapter) Discover(ctx context.Context, cfg sourceapi.Config) ([]sourceapi.Artifact, error) {
	return sourceutil.DiscoverJSONArtifacts(ctx, cfg.Root)
}

func (a *Adapter) Read(ctx context.Context, cfg sourceapi.Config, artifact sourceapi.Artifact, checkpoint sourceapi.Checkpoint) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	_ = ctx
	return sourceutil.ReadArtifact(a.Type(), cfg, artifact, checkpoint, func(fields map[string]any) sourceutil.RecordInfo {
		return sourceutil.RecordInfo{
			RawKind:          stringValue(fields, "kind"),
			SourceSessionKey: firstNonEmpty(stringValue(fields, "conversation_id"), stringValue(fields, "session_id")),
			ProjectLocator:   firstNonEmpty(stringValue(fields, "workspace"), stringValue(fields, "project")),
			SourceTimestamp:  normalize.ParseTimestamp(fields, "created_at", "timestamp", "time"),
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

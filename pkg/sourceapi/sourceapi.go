package sourceapi

import (
	"context"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type Capability string

const (
	CapabilityMessages  Capability = "messages"
	CapabilityCommands  Capability = "commands"
	CapabilityToolCalls Capability = "tool_calls"
	CapabilityFileOps   Capability = "file_ops"
	CapabilityUsage     Capability = "usage"
)

type Config struct {
	InstanceID string            `json:"instance_id"`
	Type       string            `json:"type"`
	Root       string            `json:"root"`
	Include    []string          `json:"include,omitempty"`
	Exclude    []string          `json:"exclude,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
}

type Artifact struct {
	ID             string            `json:"id"`
	Locator        string            `json:"locator"`
	ProjectLocator string            `json:"project_locator,omitempty"`
	Fingerprint    string            `json:"fingerprint,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type Checkpoint struct {
	Cursor              string    `json:"cursor,omitempty"`
	ArtifactFingerprint string    `json:"artifact_fingerprint,omitempty"`
	LastObservedSize    int64     `json:"last_observed_size,omitempty"`
	LastObservedModTime time.Time `json:"last_observed_mtime,omitempty"`
}

type Adapter interface {
	Type() string
	Capabilities() []Capability
	Discover(ctx context.Context, cfg Config) ([]Artifact, error)
	Read(ctx context.Context, cfg Config, artifact Artifact, checkpoint Checkpoint) ([]schema.RawEnvelope, Checkpoint, error)
}

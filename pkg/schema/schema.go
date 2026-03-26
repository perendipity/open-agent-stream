package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	RawEnvelopeVersion      = 1
	CanonicalEventVersionV1 = 1
	CanonicalEventVersionV2 = 2
	CanonicalEventVersion   = CanonicalEventVersionV1
	EventSpecV1             = "v1"
	EventSpecV2             = "v2"
)

type Cursor struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type ParseHints struct {
	SourceSessionKey string    `json:"source_session_key,omitempty"`
	ProjectHint      string    `json:"project_hint,omitempty"`
	TimestampHint    time.Time `json:"timestamp_hint,omitempty"`
	Confidence       float64   `json:"confidence,omitempty"`
	Capabilities     []string  `json:"capabilities,omitempty"`
}

type RawEnvelope struct {
	EnvelopeVersion  int             `json:"envelope_version"`
	EnvelopeID       string          `json:"envelope_id,omitempty"`
	SourceType       string          `json:"source_type"`
	SourceInstanceID string          `json:"source_instance_id"`
	ArtifactID       string          `json:"artifact_id"`
	ArtifactLocator  string          `json:"artifact_locator,omitempty"`
	ProjectLocator   string          `json:"project_locator,omitempty"`
	MachineID        string          `json:"machine_id,omitempty"`
	Cursor           Cursor          `json:"cursor"`
	ObservedAt       time.Time       `json:"observed_at"`
	SourceTimestamp  *time.Time      `json:"source_timestamp,omitempty"`
	RawKind          string          `json:"raw_kind"`
	RawPayload       json.RawMessage `json:"raw_payload"`
	ContentHash      string          `json:"content_hash"`
	ParseHints       ParseHints      `json:"parse_hints,omitempty"`
}

type Actor struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type EventContext struct {
	SourceType       string   `json:"source_type"`
	ProjectKey       string   `json:"project_key,omitempty"`
	ProjectLocator   string   `json:"project_locator,omitempty"`
	SourceSessionKey string   `json:"source_session_key,omitempty"`
	Capabilities     []string `json:"capabilities,omitempty"`
}

type RawRef struct {
	LedgerOffset int64  `json:"ledger_offset"`
	EnvelopeID   string `json:"envelope_id"`
}

type CanonicalEvent struct {
	EventVersion     int            `json:"event_version"`
	EventID          string         `json:"event_id"`
	SourceType       string         `json:"source_type"`
	SourceInstanceID string         `json:"source_instance_id"`
	MachineID        string         `json:"machine_id,omitempty"`
	SessionKey       string         `json:"session_key"`
	Sequence         int            `json:"sequence"`
	Timestamp        time.Time      `json:"timestamp"`
	Kind             string         `json:"kind"`
	Actor            Actor          `json:"actor"`
	Context          EventContext   `json:"context"`
	Payload          map[string]any `json:"payload"`
	RawRef           RawRef         `json:"raw_ref"`
	ParseStatus      string         `json:"parse_status,omitempty"`
}

type SessionIdentity struct {
	SessionKey       string `json:"session_key"`
	SourceType       string `json:"source_type"`
	SourceInstanceID string `json:"source_instance_id"`
	SourceSessionKey string `json:"source_session_key"`
	ProjectKey       string `json:"project_key,omitempty"`
	MachineID        string `json:"machine_id,omitempty"`
}

type RedactionPolicyView struct {
	PolicyID        string   `json:"policy_id"`
	DropRaw         bool     `json:"drop_raw"`
	RedactedKeys    []string `json:"redacted_keys"`
	RegexCount      int      `json:"regex_count,omitempty"`
	AllowedProjects []string `json:"allowed_projects,omitempty"`
	DeniedPaths     []string `json:"denied_paths,omitempty"`
}

type SinkCheckpoint struct {
	SinkID           string    `json:"sink_id"`
	LastLedgerOffset int64     `json:"last_ledger_offset"`
	LastEventID      string    `json:"last_event_id,omitempty"`
	AckedAt          time.Time `json:"acked_at"`
	DeliveryCount    int       `json:"delivery_count,omitempty"`
}

func EnsureEnvelopeID(envelope RawEnvelope) RawEnvelope {
	if envelope.EnvelopeID != "" {
		return envelope
	}
	envelope.EnvelopeID = StableID(
		"env",
		envelope.SourceType,
		envelope.SourceInstanceID,
		envelope.ArtifactID,
		envelope.Cursor.Kind,
		envelope.Cursor.Value,
		envelope.ContentHash,
	)
	return envelope
}

func EnsureEventID(event CanonicalEvent) CanonicalEvent {
	if event.EventID != "" {
		return event
	}
	event.EventID = StableEventIDForVersion(EventSpecVersionForEvent(event), event)
	return event
}

func StableSessionKey(sourceInstanceID, sourceSessionKey string) string {
	return StableSessionKeyForVersion(EventSpecV1, "", sourceInstanceID, sourceSessionKey)
}

func StableSessionKeyForVersion(version, machineID, sourceInstanceID, sourceSessionKey string) string {
	switch NormalizeEventSpecVersion(version) {
	case EventSpecV2:
		return StableID("sess", machineID, sourceInstanceID, sourceSessionKey)
	default:
		return StableID("sess", sourceInstanceID, sourceSessionKey)
	}
}

func StableProjectKey(projectLocator string) string {
	if projectLocator == "" {
		return ""
	}
	return StableID("proj", projectLocator)
}

func StableEventID(event CanonicalEvent) string {
	return StableEventIDForVersion(EventSpecVersionForEvent(event), event)
}

func StableEventIDForVersion(version string, event CanonicalEvent) string {
	payloadHash := HashJSON(event.Payload)
	switch NormalizeEventSpecVersion(version) {
	case EventSpecV2:
		return StableID(
			"evt",
			event.SourceType,
			event.MachineID,
			event.SourceInstanceID,
			event.SessionKey,
			strconvI(event.Sequence),
			event.Kind,
			event.Timestamp.UTC().Format(time.RFC3339Nano),
			payloadHash,
		)
	default:
		return StableID(
			"evt",
			event.SourceType,
			event.SourceInstanceID,
			event.SessionKey,
			strconvI(event.Sequence),
			event.Kind,
			event.Timestamp.UTC().Format(time.RFC3339Nano),
			payloadHash,
		)
	}
}

func NormalizeEventSpecVersion(version string) string {
	switch strings.ToLower(strings.TrimSpace(version)) {
	case "", EventSpecV1, "1":
		return EventSpecV1
	case EventSpecV2, "2":
		return EventSpecV2
	default:
		return ""
	}
}

func EventSpecVersionForEvent(event CanonicalEvent) string {
	if event.EventVersion >= CanonicalEventVersionV2 {
		return EventSpecV2
	}
	return EventSpecV1
}

func CanonicalEventVersionForSpec(version string) int {
	switch NormalizeEventSpecVersion(version) {
	case EventSpecV2:
		return CanonicalEventVersionV2
	default:
		return CanonicalEventVersionV1
	}
}

func StableID(prefix string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return prefix + "_" + hex.EncodeToString(sum[:8])
}

func HashBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func HashJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return HashBytes([]byte(`null`))
	}
	return HashBytes(data)
}

func CloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return CloneMap(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneValue(typed[idx])
		}
		return out
	default:
		return typed
	}
}

func strconvI(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

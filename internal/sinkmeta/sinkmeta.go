package sinkmeta

import "github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"

type ReplayClass string

const (
	ReplayClassIdempotent    ReplayClass = "idempotent"
	ReplayClassAppendOnly    ReplayClass = "append_only"
	ReplayClassSideEffecting ReplayClass = "side_effecting"
)

type Info struct {
	ID          string
	Type        string
	ReplayClass ReplayClass
}

func InfoForConfig(cfg sinkapi.Config) Info {
	return Info{
		ID:          cfg.ID,
		Type:        cfg.Type,
		ReplayClass: ReplayClassForType(cfg.Type),
	}
}

func ReplayClassForType(sinkType string) ReplayClass {
	switch sinkType {
	case "sqlite":
		return ReplayClassIdempotent
	case "jsonl", "stdout", "s3":
		return ReplayClassAppendOnly
	case "webhook", "http", "command", "external":
		return ReplayClassSideEffecting
	default:
		return ReplayClassSideEffecting
	}
}

func DefaultReplayAllowed(class ReplayClass) bool {
	return class == ReplayClassIdempotent
}

func ReplayDecisionReason(class ReplayClass) string {
	switch class {
	case ReplayClassIdempotent:
		return "selected by default during replay"
	case ReplayClassAppendOnly:
		return "skipped by default during replay because this sink appends duplicate deliveries"
	case ReplayClassSideEffecting:
		return "skipped by default during replay because this sink may trigger external side effects"
	default:
		return "skipped by default during replay because sink replay safety is unknown"
	}
}

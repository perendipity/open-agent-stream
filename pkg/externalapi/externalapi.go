package externalapi

import "github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"

const (
	ProtocolVersion = "v1"

	ActionHandshake = "handshake"
	ActionHealth    = "health"
	ActionSend      = "send"

	StatusOK        = "ok"
	StatusRetry     = "retry"
	StatusPermanent = "permanent"
)

type Request struct {
	ProtocolVersion string            `json:"protocol_version"`
	Action          string            `json:"action"`
	PluginType      string            `json:"plugin_type"`
	SinkConfig      sinkapi.Config    `json:"sink_config,omitempty"`
	Prepared        *PreparedDispatch `json:"prepared,omitempty"`
}

type Response struct {
	ProtocolVersion  string   `json:"protocol_version"`
	Status           string   `json:"status"`
	Message          string   `json:"message,omitempty"`
	PluginType       string   `json:"plugin_type,omitempty"`
	SupportedActions []string `json:"supported_actions,omitempty"`
}

type PreparedDispatch struct {
	SinkID          string            `json:"sink_id"`
	BatchID         string            `json:"batch_id"`
	Payload         []byte            `json:"payload"`
	PayloadSHA256   string            `json:"payload_sha256"`
	ContentType     string            `json:"content_type"`
	Headers         map[string]string `json:"headers,omitempty"`
	PayloadFormat   string            `json:"payload_format"`
	LedgerMinOffset int64             `json:"ledger_min_offset"`
	LedgerMaxOffset int64             `json:"ledger_max_offset"`
	EventCount      int               `json:"event_count"`
	CreatedAt       string            `json:"created_at"`
	Destination     map[string]any    `json:"destination,omitempty"`
}

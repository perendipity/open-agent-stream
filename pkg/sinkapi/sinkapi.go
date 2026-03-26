package sinkapi

import (
	"context"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type Config struct {
	ID               string            `json:"id"`
	Type             string            `json:"type"`
	EventSpecVersion string            `json:"event_spec_version,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Settings         map[string]any    `json:"settings,omitempty"`
	Delivery         DeliveryConfig    `json:"delivery,omitempty"`
}

type DeliveryConfig struct {
	MaxBatchEvents      int    `json:"max_batch_events,omitempty"`
	MaxBatchBytes       int64  `json:"max_batch_bytes,omitempty"`
	MaxBatchAge         string `json:"max_batch_age,omitempty"`
	WindowEvery         string `json:"window_every,omitempty"`
	WindowOffset        string `json:"window_offset,omitempty"`
	RetryInitialBackoff string `json:"retry_initial_backoff,omitempty"`
	RetryMaxBackoff     string `json:"retry_max_backoff,omitempty"`
	MaxAttempts         int    `json:"max_attempts,omitempty"`
	PoisonAfterFailures int    `json:"poison_after_failures,omitempty"`
}

type Batch struct {
	Events       []schema.CanonicalEvent `json:"events,omitempty"`
	RawEnvelopes []schema.RawEnvelope    `json:"raw_envelopes,omitempty"`
}

type Result struct {
	Acked      int                   `json:"acked"`
	Failed     int                   `json:"failed"`
	Checkpoint schema.SinkCheckpoint `json:"checkpoint"`
}

type Sink interface {
	ID() string
	Type() string
	Init(ctx context.Context) error
	SendBatch(ctx context.Context, batch Batch) (Result, error)
	Flush(ctx context.Context) error
	Health(ctx context.Context) error
	Close(ctx context.Context) error
}

package sinkapi

import (
	"context"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type Config struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Options map[string]string `json:"options,omitempty"`
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

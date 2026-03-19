package stdout

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg sinkapi.Config
	mu  sync.Mutex
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }
func (s *Sink) Init(context.Context) error {
	return nil
}

func (s *Sink) SendBatch(_ context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	encoder := json.NewEncoder(os.Stdout)
	for _, event := range batch.Events {
		if err := encoder.Encode(event); err != nil {
			return sinkapi.Result{}, err
		}
	}
	return sinkapi.Result{
		Acked: len(batch.Events),
		Checkpoint: schema.SinkCheckpoint{
			SinkID:        s.cfg.ID,
			AckedAt:       time.Now().UTC(),
			DeliveryCount: len(batch.Events),
		},
	}, nil
}

func (s *Sink) Flush(context.Context) error  { return nil }
func (s *Sink) Health(context.Context) error { return nil }
func (s *Sink) Close(context.Context) error  { return nil }

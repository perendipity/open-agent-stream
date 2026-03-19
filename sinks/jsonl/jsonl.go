package jsonl

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg  sinkapi.Config
	file *os.File
	mu   sync.Mutex
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(context.Context) error {
	path := s.cfg.Options["path"]
	if path == "" {
		return errors.New("jsonl sink requires options.path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.file = file
	return nil
}

func (s *Sink) SendBatch(_ context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return sinkapi.Result{}, errors.New("jsonl sink not initialized")
	}
	encoder := json.NewEncoder(s.file)
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

func (s *Sink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	return s.file.Sync()
}

func (s *Sink) Health(context.Context) error {
	if s.file == nil {
		return errors.New("jsonl sink not initialized")
	}
	return nil
}

func (s *Sink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg    sinkapi.Config
	client *http.Client
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(context.Context) error {
	if s.cfg.Options["url"] == "" {
		return errors.New("webhook sink requires options.url")
	}
	s.client = &http.Client{Timeout: 10 * time.Second}
	return nil
}

func (s *Sink) SendBatch(ctx context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	if s.client == nil {
		return sinkapi.Result{}, errors.New("webhook sink not initialized")
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return sinkapi.Result{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Options["url"], bytes.NewReader(body))
	if err != nil {
		return sinkapi.Result{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if len(batch.Events) > 0 {
		request.Header.Set("X-OAS-Last-Event-ID", batch.Events[len(batch.Events)-1].EventID)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return sinkapi.Result{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return sinkapi.Result{}, errors.New("webhook sink returned " + response.Status)
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

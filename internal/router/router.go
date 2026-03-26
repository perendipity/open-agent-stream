package router

import (
	"context"
	"errors"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/eventspec"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Router struct {
	sinks    []sinkEntry
	byID     map[string]sinkEntry
	state    *state.Store
	redactor *redact.Engine
}

type sinkEntry struct {
	cfg  sinkapi.Config
	sink sinkapi.Sink
}

func New(configs []sinkapi.Config, sinks []sinkapi.Sink, stateStore *state.Store, redactor *redact.Engine) *Router {
	entries := make([]sinkEntry, 0, len(sinks))
	byID := make(map[string]sinkEntry, len(sinks))
	for idx, sink := range sinks {
		entry := sinkEntry{cfg: configs[idx], sink: sink}
		entries = append(entries, entry)
		byID[sink.ID()] = entry
	}
	return &Router{
		sinks:    entries,
		byID:     byID,
		state:    stateStore,
		redactor: redactor,
	}
}

func (r *Router) Init(ctx context.Context) error {
	var errs []error
	for _, entry := range r.sinks {
		if err := entry.sink.Init(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Router) Close(ctx context.Context) error {
	var errs []error
	for _, entry := range r.sinks {
		if err := entry.sink.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Router) Flush(ctx context.Context) error {
	var errs []error
	for _, entry := range r.sinks {
		if err := entry.sink.Flush(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Router) Route(ctx context.Context, batch sinkapi.Batch, fromOffset, toOffset int64) error {
	var errs []error
	for _, entry := range r.sinks {
		versioned, err := eventspec.Batch(batch, config.EffectiveSinkEventSpecVersion(entry.cfg.EventSpecVersion))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		filtered, _, err := r.redactor.Apply(entry.sink.ID(), versioned)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(filtered.Events) == 0 && len(filtered.RawEnvelopes) == 0 {
			if err := r.state.PutSinkCheckpoint(schema.SinkCheckpoint{
				SinkID:           entry.sink.ID(),
				LastLedgerOffset: toOffset,
				AckedAt:          time.Now().UTC(),
			}); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		result, err := entry.sink.SendBatch(ctx, filtered)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		checkpoint := result.Checkpoint
		checkpoint.SinkID = entry.sink.ID()
		checkpoint.LastLedgerOffset = toOffset
		checkpoint.AckedAt = time.Now().UTC()
		if checkpoint.LastEventID == "" && len(filtered.Events) > 0 {
			checkpoint.LastEventID = filtered.Events[len(filtered.Events)-1].EventID
		}
		if checkpoint.DeliveryCount == 0 {
			checkpoint.DeliveryCount = len(filtered.Events)
		}
		if err := r.state.PutSinkCheckpoint(checkpoint); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Router) DrainRetries(ctx context.Context) error {
	_ = ctx
	return nil
}

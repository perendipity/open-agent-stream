package router

import (
	"context"
	"errors"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Router struct {
	sinks    []sinkapi.Sink
	byID     map[string]sinkapi.Sink
	state    *state.Store
	redactor *redact.Engine
}

func New(sinks []sinkapi.Sink, stateStore *state.Store, redactor *redact.Engine) *Router {
	byID := make(map[string]sinkapi.Sink, len(sinks))
	for _, sink := range sinks {
		byID[sink.ID()] = sink
	}
	return &Router{
		sinks:    sinks,
		byID:     byID,
		state:    stateStore,
		redactor: redactor,
	}
}

func (r *Router) Init(ctx context.Context) error {
	var errs []error
	for _, sink := range r.sinks {
		if err := sink.Init(ctx); err != nil {
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
	for _, sink := range r.sinks {
		if err := sink.Close(ctx); err != nil {
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
	for _, sink := range r.sinks {
		if err := sink.Flush(ctx); err != nil {
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
	for _, sink := range r.sinks {
		filtered, _, err := r.redactor.Apply(sink.ID(), batch)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(filtered.Events) == 0 && len(filtered.RawEnvelopes) == 0 {
			if err := r.state.PutSinkCheckpoint(schema.SinkCheckpoint{
				SinkID:           sink.ID(),
				LastLedgerOffset: toOffset,
				AckedAt:          time.Now().UTC(),
			}); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		result, err := sink.SendBatch(ctx, filtered)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		checkpoint := result.Checkpoint
		checkpoint.SinkID = sink.ID()
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

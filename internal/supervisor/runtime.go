package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/health"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/router"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkmeta"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	jsonlsink "github.com/open-agent-stream/open-agent-stream/sinks/jsonl"
	sqlitesink "github.com/open-agent-stream/open-agent-stream/sinks/sqlite"
	stdoutsink "github.com/open-agent-stream/open-agent-stream/sinks/stdout"
	webhooksink "github.com/open-agent-stream/open-agent-stream/sinks/webhook"
	claudesource "github.com/open-agent-stream/open-agent-stream/sources/claude"
	codexsource "github.com/open-agent-stream/open-agent-stream/sources/codex"
)

type Runtime struct {
	cfg        config.Config
	sinks      []sinkapi.Sink
	sinkInfo   map[string]sinkmeta.Info
	state      *state.Store
	ledger     *ledger.Store
	normalizer *normalize.Service
	router     *router.Router
	adapters   map[string]sourceapi.Adapter
}

type ReplayOptions struct {
	SinkIDs              []string
	IncludeNonIdempotent bool
}

type ReplayPlan struct {
	RequestedSinks       []string         `json:"requested_sinks,omitempty"`
	IncludeNonIdempotent bool             `json:"include_non_idempotent"`
	Decisions            []ReplayDecision `json:"decisions"`
}

type ReplayDecision struct {
	SinkID      string `json:"sink_id"`
	SinkType    string `json:"sink_type"`
	ReplayClass string `json:"replay_class"`
	Action      string `json:"action"`
	Reason      string `json:"reason"`
}

func New(cfg config.Config) (*Runtime, error) {
	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		return nil, err
	}
	ledgerStore, err := ledger.Open(cfg.LedgerPath)
	if err != nil {
		_ = stateStore.Close()
		return nil, err
	}
	sinks, sinkInfo, err := buildSinks(cfg.Sinks)
	if err != nil {
		_ = ledgerStore.Close()
		_ = stateStore.Close()
		return nil, err
	}
	return &Runtime{
		cfg:        cfg,
		sinks:      sinks,
		sinkInfo:   sinkInfo,
		state:      stateStore,
		ledger:     ledgerStore,
		normalizer: normalize.NewService(stateStore),
		router:     router.New(sinks, stateStore, redact.NewEngine(cfg)),
		adapters: map[string]sourceapi.Adapter{
			"codex_local":  codexsource.New(),
			"claude_local": claudesource.New(),
		},
	}, nil
}

func (r *Runtime) Close(ctx context.Context) error {
	var errs []error
	if r.router != nil {
		if err := r.router.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if r.ledger != nil {
		if err := r.ledger.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if r.state != nil {
		if err := r.state.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Runtime) Run(ctx context.Context) error {
	if err := r.router.Init(ctx); err != nil {
		return err
	}
	_ = r.router.DrainRetries(ctx)
	if err := r.ingestSources(ctx); err != nil {
		return err
	}
	return r.processLedger(ctx, false)
}

func (r *Runtime) Replay(ctx context.Context) error {
	return r.ReplayWithOptions(ctx, ReplayOptions{})
}

func (r *Runtime) ReplayWithOptions(ctx context.Context, options ReplayOptions) error {
	selectedSinks, plan, err := r.selectReplaySinks(options)
	if err != nil {
		return err
	}
	if len(selectedSinks) == 0 {
		blocked := make([]string, 0, len(plan.Decisions))
		for _, decision := range plan.Decisions {
			if decision.Action == "skip" && decision.Reason != "not requested" {
				blocked = append(blocked, decision.SinkID)
			}
		}
		if len(blocked) == 0 {
			return errors.New("replay selected no sinks")
		}
		return fmt.Errorf("replay selected no eligible sinks; blocked by replay safety defaults: %s (rerun with --include-non-idempotent to include append-only and side-effecting sinks)", strings.Join(blocked, ", "))
	}
	replayRouter := router.New(selectedSinks, r.state, redact.NewEngine(r.cfg))
	if err := replayRouter.Init(ctx); err != nil {
		return err
	}
	defer func() {
		_ = replayRouter.Close(ctx)
	}()
	replayNormalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	return r.streamLedger(ctx, replayNormalizer, 0, func(offset int64, batch sinkapi.Batch) error {
		return replayRouter.Route(ctx, batch, offset, offset)
	})
}

func (r *Runtime) Export(ctx context.Context, writer io.Writer) error {
	exportNormalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	encoder := json.NewEncoder(writer)
	return r.streamLedger(ctx, exportNormalizer, 0, func(_ int64, batch sinkapi.Batch) error {
		for _, event := range batch.Events {
			if err := encoder.Encode(event); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Runtime) Doctor(ctx context.Context) ([]health.Check, error) {
	checks := []health.Check{
		health.CheckWritablePath(ctx, "state_path", r.cfg.StatePath),
		health.CheckWritablePath(ctx, "ledger_path", r.cfg.LedgerPath),
	}
	for _, source := range r.cfg.Sources {
		checks = append(checks, health.CheckReadablePath(ctx, "source:"+source.InstanceID, source.Root))
	}
	for _, sinkCfg := range r.cfg.Sinks {
		info := sinkmeta.InfoForConfig(sinkCfg)
		defaultReplay := "skip"
		if sinkmeta.DefaultReplayAllowed(info.ReplayClass) {
			defaultReplay = "include"
		}
		checks = append(checks, health.Check{
			Name:   "sink:" + sinkCfg.ID,
			Status: "ok",
			Detail: fmt.Sprintf("type=%s replay_class=%s default_replay=%s", sinkCfg.Type, info.ReplayClass, defaultReplay),
		})
		if path := sinkCfg.Options["path"]; path != "" {
			checks = append(checks, health.CheckWritablePath(ctx, "sink-path:"+sinkCfg.ID, path))
		}
	}
	return checks, nil
}

func (r *Runtime) ReplayPlan(options ReplayOptions) (ReplayPlan, error) {
	_, plan, err := r.selectReplaySinks(options)
	return plan, err
}

func (r *Runtime) ingestSources(ctx context.Context) error {
	for _, sourceCfg := range r.cfg.Sources {
		adapter, ok := r.adapters[sourceCfg.Type]
		if !ok {
			return errors.New("unknown source type: " + sourceCfg.Type)
		}
		artifacts, err := adapter.Discover(ctx, sourceCfg)
		if err != nil {
			return err
		}
		for _, artifact := range artifacts {
			checkpoint, err := r.state.GetSourceCheckpoint(sourceCfg.InstanceID, artifact.ID)
			if err != nil {
				return err
			}
			envelopes, nextCheckpoint, err := adapter.Read(ctx, sourceCfg, artifact, checkpoint)
			if err != nil {
				_ = r.state.RecordDeadLetter("source_read", artifact.Locator, err.Error(), map[string]any{
					"source_id":   sourceCfg.InstanceID,
					"artifact_id": artifact.ID,
				})
				continue
			}
			for _, envelope := range envelopes {
				if _, err := r.ledger.Append(envelope); err != nil {
					return err
				}
			}
			if err := r.state.PutSourceCheckpoint(sourceCfg.InstanceID, artifact.ID, nextCheckpoint); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runtime) processLedger(ctx context.Context, replay bool) error {
	offsetName := "canonical_v1"
	startOffset, err := r.state.GetNormalizationOffset(offsetName)
	if err != nil {
		return err
	}
	if replay {
		startOffset = 0
	}
	return r.streamLedger(ctx, r.normalizer, startOffset, func(offset int64, batch sinkapi.Batch) error {
		if err := r.router.Route(ctx, batch, offset, offset); err != nil {
			_ = r.state.RecordDeadLetter("sink_delivery", filepath.Base(r.cfg.StatePath), err.Error(), batch)
		}
		return r.state.SetNormalizationOffset(offsetName, offset)
	})
}

func (r *Runtime) streamLedger(ctx context.Context, normalizer *normalize.Service, after int64, consumer func(offset int64, batch sinkapi.Batch) error) error {
	current := after
	for {
		records, err := r.ledger.ListAfter(current, r.cfg.BatchSize)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			event, err := normalizer.Normalize(record)
			if err != nil {
				_ = r.state.RecordDeadLetter("normalize", record.Envelope.EnvelopeID, err.Error(), record.Envelope)
				current = record.Offset
				continue
			}
			batch := sinkapi.Batch{
				Events:       []schema.CanonicalEvent{event},
				RawEnvelopes: []schema.RawEnvelope{record.Envelope},
			}
			if err := consumer(record.Offset, batch); err != nil {
				return err
			}
			current = record.Offset
		}
	}
}

func buildSinks(configs []sinkapi.Config) ([]sinkapi.Sink, map[string]sinkmeta.Info, error) {
	sinks := make([]sinkapi.Sink, 0, len(configs))
	info := make(map[string]sinkmeta.Info, len(configs))
	for _, cfg := range configs {
		var sink sinkapi.Sink
		switch cfg.Type {
		case "stdout":
			sink = stdoutsink.New(cfg)
		case "jsonl":
			sink = jsonlsink.New(cfg)
		case "sqlite":
			sink = sqlitesink.New(cfg)
		case "webhook":
			sink = webhooksink.New(cfg)
		default:
			return nil, nil, errors.New("unknown sink type: " + cfg.Type)
		}
		sinks = append(sinks, sink)
		info[cfg.ID] = sinkmeta.InfoForConfig(cfg)
	}
	return sinks, info, nil
}

func WriteChecks(writer io.Writer, checks []health.Check) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(checks)
}

func EnsureOutputWriter(path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{Writer: os.Stdout}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func (r *Runtime) selectReplaySinks(options ReplayOptions) ([]sinkapi.Sink, ReplayPlan, error) {
	requested := normalizeSinkIDs(options.SinkIDs)
	requestedSet := make(map[string]struct{}, len(requested))
	for _, sinkID := range requested {
		if _, ok := r.sinkInfo[sinkID]; !ok {
			return nil, ReplayPlan{}, fmt.Errorf("unknown replay sink id: %s", sinkID)
		}
		requestedSet[sinkID] = struct{}{}
	}

	selected := make([]sinkapi.Sink, 0, len(r.sinks))
	plan := ReplayPlan{
		RequestedSinks:       requested,
		IncludeNonIdempotent: options.IncludeNonIdempotent,
		Decisions:            make([]ReplayDecision, 0, len(r.sinks)),
	}
	for _, sink := range r.sinks {
		info := r.sinkInfo[sink.ID()]
		decision := ReplayDecision{
			SinkID:      sink.ID(),
			SinkType:    sink.Type(),
			ReplayClass: string(info.ReplayClass),
		}
		if len(requestedSet) > 0 {
			if _, ok := requestedSet[sink.ID()]; !ok {
				decision.Action = "skip"
				decision.Reason = "not requested"
				plan.Decisions = append(plan.Decisions, decision)
				continue
			}
		}
		if sinkmeta.DefaultReplayAllowed(info.ReplayClass) || options.IncludeNonIdempotent {
			decision.Action = "include"
			if options.IncludeNonIdempotent && !sinkmeta.DefaultReplayAllowed(info.ReplayClass) {
				decision.Reason = "explicitly included by --include-non-idempotent"
			} else {
				decision.Reason = sinkmeta.ReplayDecisionReason(info.ReplayClass)
			}
			selected = append(selected, sink)
			plan.Decisions = append(plan.Decisions, decision)
			continue
		}
		decision.Action = "skip"
		decision.Reason = sinkmeta.ReplayDecisionReason(info.ReplayClass)
		plan.Decisions = append(plan.Decisions, decision)
	}
	return selected, plan, nil
}

func normalizeSinkIDs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

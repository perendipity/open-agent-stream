package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/health"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/router"
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
	state      *state.Store
	ledger     *ledger.Store
	normalizer *normalize.Service
	router     *router.Router
	adapters   map[string]sourceapi.Adapter
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
	sinks, err := buildSinks(cfg.Sinks)
	if err != nil {
		_ = ledgerStore.Close()
		_ = stateStore.Close()
		return nil, err
	}
	return &Runtime{
		cfg:        cfg,
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
	if err := r.router.Init(ctx); err != nil {
		return err
	}
	replayNormalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	return r.streamLedger(ctx, replayNormalizer, 0, func(offset int64, batch sinkapi.Batch) error {
		return r.router.Route(ctx, batch, offset, offset)
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
	return checks, nil
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

func buildSinks(configs []sinkapi.Config) ([]sinkapi.Sink, error) {
	sinks := make([]sinkapi.Sink, 0, len(configs))
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
			return nil, errors.New("unknown sink type: " + cfg.Type)
		}
		sinks = append(sinks, sink)
	}
	return sinks, nil
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

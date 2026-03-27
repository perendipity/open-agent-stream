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
	"text/tabwriter"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/analytics"
	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/eventspec"
	"github.com/open-agent-stream/open-agent-stream/internal/health"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/router"
	"github.com/open-agent-stream/open-agent-stream/internal/secretref"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkauth"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkmeta"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/internal/storageguard"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
	commandsink "github.com/open-agent-stream/open-agent-stream/sinks/command"
	externalsink "github.com/open-agent-stream/open-agent-stream/sinks/external"
	httpsink "github.com/open-agent-stream/open-agent-stream/sinks/http"
	jsonlsink "github.com/open-agent-stream/open-agent-stream/sinks/jsonl"
	s3sink "github.com/open-agent-stream/open-agent-stream/sinks/s3"
	sqlitesink "github.com/open-agent-stream/open-agent-stream/sinks/sqlite"
	stdoutsink "github.com/open-agent-stream/open-agent-stream/sinks/stdout"
	claudesource "github.com/open-agent-stream/open-agent-stream/sources/claude"
	codexsource "github.com/open-agent-stream/open-agent-stream/sources/codex"
)

type Runtime struct {
	cfg         config.Config
	sinks       []sinkapi.Sink
	sinkInfo    map[string]sinkmeta.Info
	state       *state.Store
	ledger      *ledger.Store
	normalizer  *normalize.Service
	delivery    *delivery.Manager
	router      *router.Router
	adapters    map[string]sourceapi.Adapter
	storage     *storageguard.Guard
	initialized bool
}

type ReplayOptions struct {
	SinkIDs              []string
	IncludeNonIdempotent bool
}

type ExportOptions struct {
	EventSpecVersion string
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

type CycleResult struct {
	EnvelopesIngested int `json:"envelopes_ingested"`
	EventsDelivered   int `json:"events_delivered"`
}

func (r CycleResult) HasWork() bool {
	return r.EnvelopesIngested > 0 || r.EventsDelivered > 0
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
	redactor := redact.NewEngine(cfg)
	return &Runtime{
		cfg:        cfg,
		sinks:      sinks,
		sinkInfo:   sinkInfo,
		state:      stateStore,
		ledger:     ledgerStore,
		normalizer: normalize.NewService(stateStore),
		delivery:   delivery.New(cfg, sinks, stateStore, redactor),
		router:     router.New(cfg.Sinks, sinks, stateStore, redactor),
		storage:    storageguard.New(cfg, ledgerStore, stateStore),
		adapters: map[string]sourceapi.Adapter{
			"codex_local":  codexsource.New(),
			"claude_local": claudesource.New(),
		},
	}, nil
}

func (r *Runtime) Close(ctx context.Context) error {
	var errs []error
	if r.router != nil && r.initialized {
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
	if err := r.init(ctx); err != nil {
		return err
	}
	_, err := r.RunCycle(ctx)
	return err
}

func (r *Runtime) RunContinuously(ctx context.Context) error {
	if err := r.init(ctx); err != nil {
		return err
	}
	pollInterval, err := r.cfg.PollIntervalValue()
	if err != nil {
		return err
	}
	errorBackoff, err := r.cfg.ErrorBackoffValue()
	if err != nil {
		return err
	}
	maxConsecutiveErrors, err := r.cfg.MaxConsecutiveErrorsValue()
	if err != nil {
		return err
	}

	consecutiveFailures := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, err := r.RunCycle(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			consecutiveFailures++
			fmt.Fprintf(os.Stderr, "[%s] continuous cycle failed (%d/%d): %v\n",
				time.Now().UTC().Format(time.RFC3339), consecutiveFailures, maxConsecutiveErrors, err)
			if consecutiveFailures >= maxConsecutiveErrors {
				return fmt.Errorf("continuous mode aborted after %d consecutive failures: %w", consecutiveFailures, err)
			}
			if waitErr := waitForNextCycle(ctx, errorBackoff); waitErr != nil {
				return waitErr
			}
			continue
		}
		if guardErr := r.enforceStorageGuard(ctx); guardErr != nil {
			consecutiveFailures++
			fmt.Fprintf(os.Stderr, "[%s] storage guard failed (%d/%d): %v\n",
				time.Now().UTC().Format(time.RFC3339), consecutiveFailures, maxConsecutiveErrors, guardErr)
			if consecutiveFailures >= maxConsecutiveErrors {
				return fmt.Errorf("continuous mode aborted after %d consecutive failures: %w", consecutiveFailures, guardErr)
			}
			if waitErr := waitForNextCycle(ctx, errorBackoff); waitErr != nil {
				return waitErr
			}
			continue
		}
		if consecutiveFailures > 0 {
			fmt.Fprintf(os.Stderr, "[%s] continuous mode recovered after %d consecutive failure(s)\n",
				time.Now().UTC().Format(time.RFC3339), consecutiveFailures)
			consecutiveFailures = 0
		}
		if result.HasWork() {
			continue
		}
		if err := waitForNextCycle(ctx, pollInterval); err != nil {
			return err
		}
	}
}

func (r *Runtime) enforceStorageGuard(ctx context.Context) error {
	if r.storage == nil {
		return nil
	}
	offset, err := r.state.GetNormalizationOffset("canonical_v1")
	if err != nil {
		return err
	}
	report, err := r.storage.Enforce(ctx, offset)
	if err != nil {
		return err
	}
	if report.Enforced {
		fmt.Fprintf(os.Stderr, "[%s] storage guard: reason=%s usage_bytes=%d free_bytes=%d pruned_records=%d safe_prune_offset=%d\n",
			time.Now().UTC().Format(time.RFC3339), report.Reason, report.UsageBytes, report.FreeBytes, report.PrunedRecords, report.SafePruneOffset)
	}
	return nil
}

func (r *Runtime) RunCycle(ctx context.Context) (CycleResult, error) {
	if err := r.init(ctx); err != nil {
		return CycleResult{}, err
	}
	ingested, ingestErr := r.ingestSources(ctx)
	delivered, processErr := r.processLedger(ctx, false)
	result := CycleResult{EnvelopesIngested: ingested, EventsDelivered: delivered}
	if ingestErr != nil || processErr != nil {
		return result, errors.Join(ingestErr, processErr)
	}
	return result, nil
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
	selectedConfigs := make([]sinkapi.Config, 0, len(selectedSinks))
	for _, sink := range selectedSinks {
		for _, sinkCfg := range r.cfg.Sinks {
			if sinkCfg.ID == sink.ID() {
				selectedConfigs = append(selectedConfigs, sinkCfg)
				break
			}
		}
	}
	replayRouter := router.New(selectedConfigs, selectedSinks, r.state, redact.NewEngine(r.cfg))
	if err := replayRouter.Init(ctx); err != nil {
		return err
	}
	defer func() {
		_ = replayRouter.Close(ctx)
	}()
	replayNormalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	_, err = r.streamLedger(ctx, replayNormalizer, 0, func(offset int64, batch sinkapi.Batch) error {
		return replayRouter.Route(ctx, batch, offset, offset)
	})
	return err
}

func (r *Runtime) Export(ctx context.Context, writer io.Writer) error {
	return r.ExportWithOptions(ctx, writer, ExportOptions{})
}

func (r *Runtime) ExportWithOptions(ctx context.Context, writer io.Writer, options ExportOptions) error {
	exportNormalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	encoder := json.NewEncoder(writer)
	_, err := r.streamLedger(ctx, exportNormalizer, 0, func(_ int64, batch sinkapi.Batch) error {
		versioned, err := eventspec.Batch(batch, config.EffectiveSinkEventSpecVersion(options.EventSpecVersion))
		if err != nil {
			return err
		}
		for _, event := range versioned.Events {
			if err := encoder.Encode(event); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (r *Runtime) Doctor(ctx context.Context) ([]health.Check, error) {
	checks := []health.Check{
		health.CheckWritablePath(ctx, "state_path", r.cfg.StatePath),
		health.CheckWritablePath(ctx, "ledger_path", r.cfg.LedgerPath),
	}
	deliveryStatus, err := r.delivery.DeliveryStatus(time.Now().UTC())
	if err != nil {
		return nil, err
	}
	for _, source := range r.cfg.Sources {
		checks = append(checks, health.CheckReadablePath(ctx, "source:"+source.InstanceID, source.Root))
	}
	worktreeRoot := ""
	if cwd, err := os.Getwd(); err == nil {
		worktreeRoot = secretref.FindWorktreeRoot(cwd)
	}
	for _, sinkCfg := range r.cfg.Sinks {
		info := sinkmeta.InfoForConfig(sinkCfg)
		defaultReplay := "skip"
		if sinkmeta.DefaultReplayAllowed(info.ReplayClass) {
			defaultReplay = "include"
		}
		detail := fmt.Sprintf("type=%s replay_class=%s default_replay=%s", sinkCfg.Type, info.ReplayClass, defaultReplay)
		if summary, ok := deliveryStatus[sinkCfg.ID]; ok {
			detail += fmt.Sprintf(" queue_depth=%d ready_batches=%d retrying_batches=%d blocked_batches=%d quarantined_batches=%d acked_offset=%d terminal_offset=%d gaps=%d",
				summary.Summary.QueueDepth,
				summary.Summary.ReadyBatchCount,
				summary.Summary.RetryingBatchCount,
				summary.Summary.BlockedBatchCount,
				summary.Summary.QuarantinedBatchCount,
				summary.Summary.AckedContiguousOffset,
				summary.Summary.TerminalContiguousOffset,
				summary.Summary.GapCount,
			)
			if summary.Summary.BlockedKind != "" {
				detail += fmt.Sprintf(" blocked_kind=%q", summary.Summary.BlockedKind)
			}
			if summary.Summary.LastBlockedError != "" {
				detail += fmt.Sprintf(" last_blocked_error=%q", summary.Summary.LastBlockedError)
			}
			if summary.Summary.LastAuthError != "" {
				detail += fmt.Sprintf(" last_auth_error=%q", summary.Summary.LastAuthError)
			}
			if summary.Summary.LastTerminalError != "" {
				detail += fmt.Sprintf(" last_terminal_error=%q", summary.Summary.LastTerminalError)
			}
			if len(summary.Summary.SecretProviderStats) > 0 {
				var providers []string
				for _, stat := range summary.Summary.SecretProviderStats {
					providers = append(providers, fmt.Sprintf("%s(resolutions=%d auth_failures=%d config_failures=%d)",
						stat.Provider,
						stat.SuccessfulResolutions,
						stat.AuthFailures,
						stat.ConfigFailures,
					))
				}
				detail += " secret_providers=" + strings.Join(providers, ",")
			}
		}
		checks = append(checks, health.Check{
			Name:   "sink:" + sinkCfg.ID,
			Status: "ok",
			Detail: detail,
		})
		authChecks, err := sinkauth.StaticChecks(ctx, sinkCfg, worktreeRoot)
		if err != nil {
			return nil, err
		}
		checks = append(checks, authChecks...)
		if path := sinkCfg.Options["path"]; path != "" {
			checks = append(checks, health.CheckWritablePath(ctx, "sink-path:"+sinkCfg.ID, path))
		}
	}
	for idx, sink := range r.sinks {
		sinkCfg := r.cfg.Sinks[idx]
		healthCheck := health.Check{
			Name:   "sink-health:" + sinkCfg.ID,
			Status: "ok",
			Detail: sinkHealthTarget(sinkCfg),
		}
		if err := sink.Health(ctx); err != nil {
			healthCheck.Status = "fail"
			healthCheck.Detail = err.Error()
		}
		checks = append(checks, healthCheck)
	}
	if r.storage != nil {
		report, err := r.storage.Inspect()
		if err != nil {
			checks = append(checks, health.Check{
				Name:   "storage_guard",
				Status: "fail",
				Detail: err.Error(),
			})
		} else {
			status := "ok"
			if report.NeedsEnforcement {
				status = "warn"
			}
			detail := fmt.Sprintf("usage_bytes=%d managed_paths=%d", report.UsageBytes, len(report.ManagedPaths))
			if report.MaxStorageBytes > 0 {
				detail += fmt.Sprintf(" max_storage_bytes=%d", report.MaxStorageBytes)
			}
			if report.DesiredUsageBytes > 0 {
				detail += fmt.Sprintf(" desired_usage_bytes=%d", report.DesiredUsageBytes)
			}
			if report.FreeBytes > 0 {
				detail += fmt.Sprintf(" free_bytes=%d", report.FreeBytes)
			}
			if report.MinFreeBytes > 0 {
				detail += fmt.Sprintf(" min_free_bytes=%d", report.MinFreeBytes)
			}
			if report.Reason != "" {
				detail += fmt.Sprintf(" reason=%s", report.Reason)
			}
			checks = append(checks, health.Check{
				Name:   "storage_guard",
				Status: status,
				Detail: detail,
			})
		}
	}
	return checks, nil
}

func (r *Runtime) ReplayPlan(options ReplayOptions) (ReplayPlan, error) {
	_, plan, err := r.selectReplaySinks(options)
	return plan, err
}

func (r *Runtime) init(ctx context.Context) error {
	if r.initialized {
		return nil
	}
	if err := r.router.Init(ctx); err != nil {
		return err
	}
	if err := r.delivery.Init(); err != nil {
		return err
	}
	r.initialized = true
	return nil
}

func (r *Runtime) ingestSources(ctx context.Context) (int, error) {
	total := 0
	var errs []error
	for _, sourceCfg := range r.cfg.Sources {
		adapter, ok := r.adapters[sourceCfg.Type]
		if !ok {
			return 0, errors.New("unknown source type: " + sourceCfg.Type)
		}
		artifacts, err := adapter.Discover(ctx, sourceCfg)
		if err != nil {
			_ = r.state.RecordDeadLetter("source_discover", sourceCfg.Root, err.Error(), map[string]any{
				"source_id": sourceCfg.InstanceID,
				"root":      sourceCfg.Root,
				"type":      sourceCfg.Type,
			})
			errs = append(errs, fmt.Errorf("discover %s: %w", sourceCfg.InstanceID, err))
			continue
		}
		for _, artifact := range artifacts {
			checkpoint, err := r.state.GetSourceCheckpoint(sourceCfg.InstanceID, artifact.ID)
			if err != nil {
				return total, err
			}
			envelopes, nextCheckpoint, err := adapter.Read(ctx, sourceCfg, artifact, checkpoint)
			if err != nil {
				_ = r.state.RecordDeadLetter("source_read", artifact.Locator, err.Error(), map[string]any{
					"source_id":   sourceCfg.InstanceID,
					"artifact_id": artifact.ID,
				})
				errs = append(errs, fmt.Errorf("read %s/%s: %w", sourceCfg.InstanceID, artifact.ID, err))
				continue
			}
			for _, envelope := range envelopes {
				machineID, err := config.EffectiveMachineID(envelope.MachineID, r.cfg.MachineID)
				if err != nil {
					_ = r.state.RecordDeadLetter("source_machine_id", artifact.Locator, err.Error(), map[string]any{
						"source_id":           sourceCfg.InstanceID,
						"artifact_id":         artifact.ID,
						"captured_machine_id": envelope.MachineID,
						"config_machine_id":   r.cfg.MachineID,
					})
					return total, fmt.Errorf("machine identity mismatch for %s/%s: %w", sourceCfg.InstanceID, artifact.ID, err)
				}
				envelope.MachineID = machineID
				if _, err := r.ledger.Append(envelope); err != nil {
					return total, err
				}
				total++
			}
			if err := r.state.PutSourceCheckpoint(sourceCfg.InstanceID, artifact.ID, nextCheckpoint); err != nil {
				return total, err
			}
		}
	}
	return total, errors.Join(errs...)
}

func (r *Runtime) processLedger(ctx context.Context, replay bool) (int, error) {
	offsetName := "canonical_v1"
	startOffset, err := r.state.GetNormalizationOffset(offsetName)
	if err != nil {
		return 0, err
	}
	if replay {
		startOffset = 0
	}
	_, streamErr := r.streamLedger(ctx, r.normalizer, startOffset, func(offset int64, batch sinkapi.Batch) error {
		if err := r.delivery.PrepareBatch(ctx, batch, offset); err != nil {
			return err
		}
		return r.state.SetNormalizationOffset(offsetName, offset)
	})
	delivered, dispatchErr := r.delivery.DispatchDue(ctx)
	if streamErr != nil || dispatchErr != nil {
		return delivered, errors.Join(streamErr, dispatchErr)
	}
	return delivered, nil
}

func (r *Runtime) streamLedger(ctx context.Context, normalizer *normalize.Service, after int64, consumer func(offset int64, batch sinkapi.Batch) error) (int, error) {
	current := after
	processed := 0
	for {
		records, err := r.ledger.ListAfter(current, r.cfg.BatchSize)
		if err != nil {
			return processed, err
		}
		if len(records) == 0 {
			return processed, nil
		}
		for _, record := range records {
			select {
			case <-ctx.Done():
				return processed, ctx.Err()
			default:
			}
			machineID, err := config.EffectiveMachineID(record.Envelope.MachineID, r.cfg.MachineID)
			if err != nil {
				_ = r.state.RecordDeadLetter("machine_id", record.Envelope.EnvelopeID, err.Error(), record.Envelope)
				return processed, err
			}
			record.Envelope.MachineID = machineID
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
				return processed, err
			}
			current = record.Offset
			processed++
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
		case "http":
			sink = httpsink.New(cfg)
		case "s3":
			sink = s3sink.New(cfg)
		case "command":
			sink = commandsink.New(cfg)
		case "external":
			sink = externalsink.New(cfg)
		case "webhook":
			translated := cfg
			translated.Type = "http"
			if translated.Settings == nil {
				translated.Settings = map[string]any{}
			}
			if _, ok := translated.Settings["method"]; !ok {
				translated.Settings["method"] = "POST"
			}
			if _, ok := translated.Settings["format"]; !ok {
				translated.Settings["format"] = "oas_batch_json"
			}
			sink = httpsink.New(translated)
		default:
			return nil, nil, errors.New("unknown sink type: " + cfg.Type)
		}
		sinks = append(sinks, sink)
		info[cfg.ID] = sinkmeta.InfoForConfig(cfg)
	}
	return sinks, info, nil
}

func WriteChecksJSON(writer io.Writer, checks []health.Check) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(checks)
}

func WriteChecksTable(writer io.Writer, checks []health.Check) error {
	table := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "NAME\tSTATUS\tDETAIL"); err != nil {
		return err
	}
	for _, check := range checks {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", check.Name, check.Status, check.Detail); err != nil {
			return err
		}
	}
	return table.Flush()
}

func waitForNextCycle(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sinkHealthTarget(cfg sinkapi.Config) string {
	switch cfg.Type {
	case "command":
		argv := sinkutil.StringSlice(cfg, "argv")
		if len(argv) == 0 {
			return "settings.argv is required"
		}
		return strings.Join(argv, " ")
	case "http", "webhook":
		return sinkutil.String(cfg, "url")
	case "s3":
		bucket := sinkutil.String(cfg, "bucket")
		if prefix := sinkutil.String(cfg, "prefix"); prefix != "" {
			return bucket + "/" + prefix
		}
		return bucket
	case "external":
		pluginType := sinkutil.String(cfg, "plugin_type")
		argv := sinkutil.StringSlice(cfg, "argv")
		if len(argv) == 0 {
			return pluginType
		}
		if pluginType == "" {
			return strings.Join(argv, " ")
		}
		return pluginType + " via " + argv[0]
	default:
		return cfg.Type
	}
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

func (r *Runtime) Analytics() *analytics.Service {
	return analytics.New(r.cfg, r.ledger)
}

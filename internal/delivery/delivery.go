package delivery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/eventspec"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type PreparedDispatch struct {
	SinkID          string            `json:"sink_id"`
	BatchID         string            `json:"batch_id"`
	Payload         []byte            `json:"payload"`
	PayloadSHA256   string            `json:"payload_sha256"`
	ContentType     string            `json:"content_type"`
	Headers         map[string]string `json:"headers,omitempty"`
	PayloadFormat   string            `json:"payload_format"`
	LedgerMinOffset int64             `json:"ledger_min_offset"`
	LedgerMaxOffset int64             `json:"ledger_max_offset"`
	EventCount      int               `json:"event_count"`
	CreatedAt       time.Time         `json:"created_at"`
	Destination     map[string]any    `json:"destination,omitempty"`
}

type PreparedSink interface {
	sinkapi.Sink
	SealBatch(ctx context.Context, batch sinkapi.Batch, meta PreparedDispatch) (PreparedDispatch, error)
	SendPrepared(ctx context.Context, prepared PreparedDispatch) (sinkapi.Result, error)
}

type PermanentError struct {
	Err error
}

type BlockedKind string

const (
	BlockedKindAuth   BlockedKind = "auth"
	BlockedKindConfig BlockedKind = "config"
)

type BlockedError struct {
	Kind           BlockedKind
	Code           string
	Provider       string
	RefFingerprint string
	Err            error
}

func (e *PermanentError) Error() string {
	if e == nil || e.Err == nil {
		return "permanent delivery failure"
	}
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error { return e.Err }

func (e *BlockedError) Error() string {
	if e == nil || e.Err == nil {
		return "blocked delivery failure"
	}
	return e.Err.Error()
}

func (e *BlockedError) Unwrap() error { return e.Err }

type SinkStatus struct {
	Summary state.DeliverySummary
}

type Manager struct {
	cfg      config.Config
	sinks    []sinkEntry
	byID     map[string]sinkEntry
	state    *state.Store
	redactor *redact.Engine
	rand     *mrand.Rand
}

type sinkEntry struct {
	cfg      sinkapi.Config
	sink     sinkapi.Sink
	prepared PreparedSink
}

func New(cfg config.Config, sinks []sinkapi.Sink, stateStore *state.Store, redactor *redact.Engine) *Manager {
	entries := make([]sinkEntry, 0, len(sinks))
	byID := make(map[string]sinkEntry, len(sinks))
	for idx, sink := range sinks {
		sinkCfg := cfg.Sinks[idx]
		applyDeliveryDefaults(&sinkCfg.Delivery)
		entry := sinkEntry{cfg: sinkCfg, sink: sink}
		if prepared, ok := sink.(PreparedSink); ok {
			entry.prepared = prepared
		}
		entries = append(entries, entry)
		byID[sink.ID()] = entry
	}
	return &Manager{
		cfg:      cfg,
		sinks:    entries,
		byID:     byID,
		state:    stateStore,
		redactor: redactor,
		rand:     mrand.New(mrand.NewSource(time.Now().UnixNano())),
	}
}

func applyDeliveryDefaults(cfg *sinkapi.DeliveryConfig) {
	if cfg.MaxBatchEvents <= 0 {
		cfg.MaxBatchEvents = 1
	}
	if cfg.RetryInitialBackoff == "" {
		cfg.RetryInitialBackoff = (10 * time.Second).String()
	}
	if cfg.RetryMaxBackoff == "" {
		cfg.RetryMaxBackoff = (5 * time.Minute).String()
	}
	if cfg.PoisonAfterFailures <= 0 {
		cfg.PoisonAfterFailures = 20
	}
}

func (m *Manager) Init() error {
	for _, entry := range m.sinks {
		if err := m.state.EnsureSinkProgress(entry.cfg.ID); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) PrepareBatch(ctx context.Context, batch sinkapi.Batch, ledgerOffset int64) error {
	_ = ctx
	now := time.Now().UTC()
	for _, entry := range m.sinks {
		versioned, err := eventspec.Batch(batch, config.EffectiveSinkEventSpecVersion(entry.cfg.EventSpecVersion))
		if err != nil {
			return err
		}
		filtered, _, err := m.redactor.Apply(entry.cfg.ID, versioned)
		if err != nil {
			return err
		}
		status := state.DeliveryItemStatusPending
		if len(filtered.Events) == 0 && len(filtered.RawEnvelopes) == 0 {
			status = state.DeliveryItemStatusSkipped
		}
		if err := m.state.EnqueueDeliveryItem(entry.cfg.ID, ledgerOffset, filtered, status, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) DispatchDue(ctx context.Context) (int, error) {
	if err := m.sealReadyBatches(ctx); err != nil {
		return 0, err
	}
	due, err := m.state.ListDueDeliveryBatches(time.Now().UTC(), 128)
	if err != nil {
		return 0, err
	}
	var errs []error
	delivered := 0
	for _, batch := range due {
		entry, ok := m.byID[batch.SinkID]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown sink for delivery batch %s: %s", batch.BatchID, batch.SinkID))
			continue
		}
		var prepared PreparedDispatch
		if err := json.Unmarshal(batch.PreparedJSON, &prepared); err != nil {
			errs = append(errs, err)
			continue
		}
		result, sendErr := m.sendPrepared(ctx, entry, prepared)
		if sendErr == nil {
			if err := m.state.MarkDeliveryBatchSucceeded(batch.BatchID, time.Now().UTC()); err != nil {
				errs = append(errs, err)
			}
			if len(result.AuthMetrics) > 0 {
				if err := m.state.RecordSecretResolutions(batch.SinkID, result.AuthMetrics, time.Now().UTC()); err != nil {
					errs = append(errs, err)
				}
			}
			delivered += batch.EventCount
			continue
		}
		nextAttempt := m.nextAttemptAt(entry.cfg.Delivery, batch.AttemptCount+1, time.Now().UTC())
		var blockedErr *BlockedError
		if errors.As(sendErr, &blockedErr) {
			if err := m.state.MarkDeliveryBatchBlocked(batch.BatchID, string(blockedErr.Kind), blockedErr.Code, blockedErr.Provider, sendErr.Error(), nextAttempt, time.Now().UTC()); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		var permanentErr *PermanentError
		permanent := errors.As(sendErr, &permanentErr)
		reachedMaxAttempts := entry.cfg.Delivery.MaxAttempts > 0 && batch.AttemptCount+1 >= entry.cfg.Delivery.MaxAttempts
		reachedPoison := entry.cfg.Delivery.PoisonAfterFailures > 0 && batch.AttemptCount+1 >= entry.cfg.Delivery.PoisonAfterFailures
		if permanent || reachedMaxAttempts || reachedPoison {
			if err := m.state.QuarantineDeliveryBatch(batch.BatchID, sendErr.Error(), time.Now().UTC()); err != nil {
				errs = append(errs, err)
				continue
			}
			continue
		}
		if err := m.state.MarkDeliveryBatchRetry(batch.BatchID, sendErr.Error(), nextAttempt, time.Now().UTC()); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return delivered, errors.Join(errs...)
	}
	return delivered, nil
}

func (m *Manager) DeliveryStatus(now time.Time) (map[string]SinkStatus, error) {
	out := make(map[string]SinkStatus, len(m.sinks))
	for _, entry := range m.sinks {
		summary, err := m.state.DeliverySummary(entry.cfg.ID, now)
		if err != nil {
			return nil, err
		}
		out[entry.cfg.ID] = SinkStatus{Summary: summary}
	}
	return out, nil
}

func (m *Manager) sealReadyBatches(ctx context.Context) error {
	for _, entry := range m.sinks {
		for {
			items, err := m.state.ListPendingDeliveryItems(entry.cfg.ID, entry.cfg.Delivery.MaxBatchEvents)
			if err != nil {
				return err
			}
			selected, ready := selectReadyItems(items, entry.cfg.Delivery, time.Now().UTC())
			if !ready {
				break
			}
			combined := combineItems(selected)
			meta := PreparedDispatch{
				SinkID:          entry.cfg.ID,
				BatchID:         newBatchID(),
				LedgerMinOffset: selected[0].LedgerOffset,
				LedgerMaxOffset: selected[len(selected)-1].LedgerOffset,
				EventCount:      len(combined.Events),
				CreatedAt:       time.Now().UTC(),
			}
			prepared, err := sealForSink(ctx, entry, combined, meta)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(prepared)
			if err != nil {
				return err
			}
			itemIDs := make([]int64, 0, len(selected))
			for _, item := range selected {
				itemIDs = append(itemIDs, item.ID)
			}
			if err := m.state.SealDeliveryBatch(state.DeliveryBatch{
				BatchID:         prepared.BatchID,
				SinkID:          prepared.SinkID,
				PreparedJSON:    payload,
				PayloadBytes:    len(prepared.Payload),
				LedgerMinOffset: prepared.LedgerMinOffset,
				LedgerMaxOffset: prepared.LedgerMaxOffset,
				EventCount:      prepared.EventCount,
				Status:          state.DeliveryBatchStatusPending,
				AttemptCount:    0,
				NextAttemptAt:   time.Now().UTC(),
				CreatedAt:       prepared.CreatedAt,
				UpdatedAt:       prepared.CreatedAt,
			}, itemIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

func selectReadyItems(items []state.DeliveryItem, cfg sinkapi.DeliveryConfig, now time.Time) ([]state.DeliveryItem, bool) {
	if len(items) == 0 {
		return nil, false
	}
	selected := make([]state.DeliveryItem, 0, len(items))
	var totalBytes int64
	totalEvents := 0
	expectedOffset := items[0].LedgerOffset
	countReady := false
	bytesReady := false
	for _, item := range items {
		if item.LedgerOffset != expectedOffset {
			break
		}
		if cfg.MaxBatchBytes > 0 && totalBytes > 0 && totalBytes+int64(item.PayloadBytes) > cfg.MaxBatchBytes {
			break
		}
		selected = append(selected, item)
		totalBytes += int64(item.PayloadBytes)
		totalEvents += item.EventCount
		expectedOffset++
		if totalEvents >= cfg.MaxBatchEvents {
			countReady = true
			break
		}
		if cfg.MaxBatchBytes > 0 && totalBytes >= cfg.MaxBatchBytes {
			bytesReady = true
			break
		}
	}
	if len(selected) == 0 {
		return nil, false
	}
	ageReady := false
	if maxBatchAge, _ := time.ParseDuration(cfg.MaxBatchAge); maxBatchAge > 0 {
		ageReady = now.Sub(selected[0].CreatedAt) >= maxBatchAge
	}
	ready := countReady || bytesReady || ageReady || cfg.MaxBatchEvents == 1
	if !ready {
		return nil, false
	}
	if !windowAllows(now, selected[0].CreatedAt, cfg) {
		return nil, false
	}
	return selected, true
}

func windowAllows(now, oldest time.Time, cfg sinkapi.DeliveryConfig) bool {
	if cfg.WindowEvery == "" {
		return true
	}
	every, err := time.ParseDuration(cfg.WindowEvery)
	if err != nil || every <= 0 {
		return true
	}
	offset, _ := time.ParseDuration(cfg.WindowOffset)
	next := nextWindowBoundary(oldest.UTC(), every, offset)
	return !now.UTC().Before(next)
}

func nextWindowBoundary(base time.Time, every, offset time.Duration) time.Time {
	epoch := time.Unix(0, 0).UTC().Add(offset)
	if !base.After(epoch) {
		return epoch
	}
	elapsed := base.Sub(epoch)
	windows := elapsed / every
	boundary := epoch.Add((windows + 1) * every)
	return boundary
}

func combineItems(items []state.DeliveryItem) sinkapi.Batch {
	combined := sinkapi.Batch{}
	for _, item := range items {
		combined.Events = append(combined.Events, item.Batch.Events...)
		combined.RawEnvelopes = append(combined.RawEnvelopes, item.Batch.RawEnvelopes...)
	}
	return combined
}

func sealForSink(ctx context.Context, entry sinkEntry, batch sinkapi.Batch, meta PreparedDispatch) (PreparedDispatch, error) {
	if entry.prepared != nil {
		return entry.prepared.SealBatch(ctx, batch, meta)
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		return PreparedDispatch{}, err
	}
	meta.Payload = payload
	meta.PayloadSHA256 = hashPayload(payload)
	meta.ContentType = "application/json"
	meta.PayloadFormat = "sink_batch_json"
	meta.Headers = map[string]string{"Content-Type": meta.ContentType}
	meta.Destination = map[string]any{"sink_type": entry.cfg.Type}
	return meta, nil
}

func (m *Manager) sendPrepared(ctx context.Context, entry sinkEntry, prepared PreparedDispatch) (sinkapi.Result, error) {
	if entry.prepared != nil {
		return entry.prepared.SendPrepared(ctx, prepared)
	}
	var batch sinkapi.Batch
	if err := json.Unmarshal(prepared.Payload, &batch); err != nil {
		return sinkapi.Result{}, err
	}
	return entry.sink.SendBatch(ctx, batch)
}

func (m *Manager) nextAttemptAt(cfg sinkapi.DeliveryConfig, attempts int, now time.Time) time.Time {
	initial, _ := time.ParseDuration(cfg.RetryInitialBackoff)
	maxDelay, _ := time.ParseDuration(cfg.RetryMaxBackoff)
	delay := initial
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= maxDelay {
			delay = maxDelay
			break
		}
	}
	jitterWindow := int64(delay / 5)
	if jitterWindow > 0 {
		delta := m.rand.Int63n(jitterWindow*2+1) - jitterWindow
		delay += time.Duration(delta)
	}
	if delay < 0 {
		delay = 0
	}
	return now.Add(delay)
}

func hashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func newBatchID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("batch_%d", time.Now().UTC().UnixNano())
	}
	return "batch_" + hex.EncodeToString(buf)
}

func NewPermanentError(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

func NewBlockedError(kind BlockedKind, code, provider, refFingerprint string, err error) error {
	if err == nil {
		return nil
	}
	return &BlockedError{
		Kind:           kind,
		Code:           code,
		Provider:       provider,
		RefFingerprint: refFingerprint,
		Err:            err,
	}
}

func BuildReplayCheckpoint(batch sinkapi.Batch, sinkID string) schema.SinkCheckpoint {
	checkpoint := schema.SinkCheckpoint{
		SinkID:        sinkID,
		AckedAt:       time.Now().UTC(),
		DeliveryCount: len(batch.Events),
	}
	if len(batch.Events) > 0 {
		checkpoint.LastEventID = batch.Events[len(batch.Events)-1].EventID
	}
	return checkpoint
}

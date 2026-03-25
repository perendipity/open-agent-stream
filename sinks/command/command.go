package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkpayload"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg            sinkapi.Config
	argv           []string
	format         string
	stagingDir     string
	timeout        time.Duration
	maxOutputBytes int
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(context.Context) error {
	s.argv = sinkutil.StringSlice(s.cfg, "argv")
	if len(s.argv) == 0 {
		return errors.New("command sink requires settings.argv")
	}
	s.format = firstNonEmpty(sinkutil.String(s.cfg, "format"), sinkpayload.FormatCanonicalJSONLGZ)
	timeout, err := sinkutil.Duration(s.cfg, "timeout", 30*time.Second)
	if err != nil {
		return err
	}
	s.timeout = timeout
	s.stagingDir = firstNonEmpty(sinkutil.String(s.cfg, "staging_dir"), os.TempDir())
	s.maxOutputBytes = sinkutil.Int(s.cfg, "max_output_bytes", 4096)
	return nil
}

func (s *Sink) SealBatch(_ context.Context, batch sinkapi.Batch, meta delivery.PreparedDispatch) (delivery.PreparedDispatch, error) {
	payload, contentType, headers, err := sinkpayload.EncodeBatch(batch, s.format)
	if err != nil {
		return delivery.PreparedDispatch{}, err
	}
	meta.Payload = payload
	meta.PayloadSHA256 = schema.HashBytes(payload)
	meta.ContentType = contentType
	meta.Headers = headers
	meta.PayloadFormat = s.format
	meta.Destination = map[string]any{
		"argv":             append([]string(nil), s.argv...),
		"staging_dir":      s.stagingDir,
		"timeout_ms":       s.timeout.Milliseconds(),
		"max_output_bytes": s.maxOutputBytes,
	}
	return meta, nil
}

func (s *Sink) SendPrepared(ctx context.Context, prepared delivery.PreparedDispatch) (sinkapi.Result, error) {
	stagingDir := stringFromMap(prepared.Destination, "staging_dir", os.TempDir())
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return sinkapi.Result{}, err
	}
	dir, err := os.MkdirTemp(stagingDir, "oas-command-"+prepared.BatchID+"-")
	if err != nil {
		return sinkapi.Result{}, err
	}
	defer os.RemoveAll(dir)

	payloadPath := filepath.Join(dir, prepared.BatchID+".payload")
	if err := os.WriteFile(payloadPath, prepared.Payload, 0o644); err != nil {
		return sinkapi.Result{}, err
	}
	manifestPath := filepath.Join(dir, prepared.BatchID+".manifest.json")
	manifest, err := json.MarshalIndent(prepared, "", "  ")
	if err != nil {
		return sinkapi.Result{}, err
	}
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		return sinkapi.Result{}, err
	}

	timeout := durationFromMap(prepared.Destination, "timeout_ms", 30*time.Second)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	argv := stringSliceFromMap(prepared.Destination, "argv")
	if len(argv) == 0 {
		return sinkapi.Result{}, delivery.NewPermanentError(errors.New("command sink sealed batch missing argv"))
	}
	argv = substituteArgs(argv, prepared, payloadPath, manifestPath)
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Stdout = ioDiscard{}
	stderr := &limitedBuffer{max: int64(intFromMap(prepared.Destination, "max_output_bytes", 4096))}
	cmd.Stderr = stderr
	err = cmd.Run()
	if runCtx.Err() == context.DeadlineExceeded {
		return sinkapi.Result{}, fmt.Errorf("command sink timed out after %s: %s", timeout, stderr.String())
	}
	if err != nil {
		return sinkapi.Result{}, fmt.Errorf("command sink failed: %v: %s", err, stderr.String())
	}
	return sinkapi.Result{
		Acked: prepared.EventCount,
		Checkpoint: schema.SinkCheckpoint{
			SinkID:        prepared.SinkID,
			AckedAt:       time.Now().UTC(),
			DeliveryCount: prepared.EventCount,
		},
	}, nil
}

func (s *Sink) SendBatch(ctx context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	prepared, err := s.SealBatch(ctx, batch, delivery.PreparedDispatch{
		SinkID:        s.cfg.ID,
		BatchID:       "replay-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10),
		EventCount:    len(batch.Events),
		CreatedAt:     time.Now().UTC(),
		PayloadFormat: s.format,
	})
	if err != nil {
		return sinkapi.Result{}, err
	}
	return s.SendPrepared(ctx, prepared)
}

func (s *Sink) Flush(context.Context) error  { return nil }
func (s *Sink) Health(context.Context) error { return nil }
func (s *Sink) Close(context.Context) error  { return nil }

type limitedBuffer struct {
	max int64
	buf bytes.Buffer
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 {
		return len(p), nil
	}
	if int64(b.buf.Len()) >= b.max {
		return len(p), nil
	}
	remaining := int(b.max - int64(b.buf.Len()))
	if remaining < len(p) {
		p = p[:remaining]
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) String() string {
	return strings.TrimSpace(b.buf.String())
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func substituteArgs(argv []string, prepared delivery.PreparedDispatch, payloadPath, manifestPath string) []string {
	replacements := map[string]string{
		"{payload_path}":      payloadPath,
		"{manifest_path}":     manifestPath,
		"{sink_id}":           prepared.SinkID,
		"{batch_id}":          prepared.BatchID,
		"{ledger_min_offset}": strconv.FormatInt(prepared.LedgerMinOffset, 10),
		"{ledger_max_offset}": strconv.FormatInt(prepared.LedgerMaxOffset, 10),
	}
	out := make([]string, len(argv))
	for idx, value := range argv {
		replaced := value
		for placeholder, actual := range replacements {
			replaced = strings.ReplaceAll(replaced, placeholder, actual)
		}
		out[idx] = replaced
	}
	return out
}

func stringSliceFromMap(values map[string]any, key string) []string {
	raw, ok := values[key]
	if !ok {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func stringFromMap(values map[string]any, key, fallback string) string {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fallback
}

func intFromMap(values map[string]any, key string, fallback int) int {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	switch typed := raw.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return fallback
	}
}

func durationFromMap(values map[string]any, key string, fallback time.Duration) time.Duration {
	return time.Duration(intFromMap(values, key, int(fallback/time.Millisecond))) * time.Millisecond
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

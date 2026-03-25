package s3sink

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkpayload"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg          sinkapi.Config
	client       objectClient
	bucket       string
	prefix       string
	keyTemplate  string
	format       string
	storageClass string
	sse          string
}

type objectClient interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(ctx context.Context) error {
	s.bucket = sinkutil.String(s.cfg, "bucket")
	if s.bucket == "" {
		return errors.New("s3 sink requires bucket")
	}
	s.prefix = sinkutil.String(s.cfg, "prefix")
	s.keyTemplate = sinkutil.String(s.cfg, "key_template")
	if s.keyTemplate == "" {
		s.keyTemplate = "{prefix}{sink_id}/{batch_id}.jsonl.gz"
	}
	if !hasUniquenessToken(s.keyTemplate) {
		return errors.New("s3 key_template must include {batch_id}, {payload_sha256}, or ledger range placeholders")
	}
	s.format = firstNonEmpty(sinkutil.String(s.cfg, "format"), sinkpayload.FormatCanonicalJSONLGZ)
	s.storageClass = sinkutil.String(s.cfg, "storage_class")
	s.sse = sinkutil.String(s.cfg, "server_side_encryption")
	region := sinkutil.String(s.cfg, "region")
	if s.client == nil {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		if err != nil {
			return err
		}
		s.client = s3.NewFromConfig(awsCfg)
	}
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
	key := s.resolveKey(meta)
	meta.Destination = map[string]any{
		"bucket":                 s.bucket,
		"key":                    key,
		"storage_class":          s.storageClass,
		"server_side_encryption": s.sse,
	}
	return meta, nil
}

func (s *Sink) SendPrepared(ctx context.Context, prepared delivery.PreparedDispatch) (sinkapi.Result, error) {
	key := stringFromMap(prepared.Destination, "key", "")
	if key == "" {
		return sinkapi.Result{}, delivery.NewPermanentError(errors.New("s3 sink sealed batch missing key"))
	}
	input := &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(prepared.Payload),
		ContentType: &prepared.ContentType,
	}
	if encoding, ok := prepared.Headers["Content-Encoding"]; ok {
		input.ContentEncoding = &encoding
	}
	if storageClass := stringFromMap(prepared.Destination, "storage_class", ""); storageClass != "" {
		class := s3types.StorageClass(storageClass)
		input.StorageClass = class
	}
	if sse := stringFromMap(prepared.Destination, "server_side_encryption", ""); sse != "" {
		mode := s3types.ServerSideEncryption(sse)
		input.ServerSideEncryption = mode
	}
	if _, err := s.client.PutObject(ctx, input); err != nil {
		return sinkapi.Result{}, err
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

func (s *Sink) Flush(context.Context) error { return nil }
func (s *Sink) Health(ctx context.Context) error {
	bucket := sinkutil.String(s.cfg, "bucket")
	if bucket == "" {
		return errors.New("s3 sink requires bucket")
	}
	region := sinkutil.String(s.cfg, "region")
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return err
	}
	if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
		return err
	}
	client := s.client
	if client == nil {
		client = s3.NewFromConfig(awsCfg)
	}
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	return err
}
func (s *Sink) Close(context.Context) error { return nil }

func (s *Sink) resolveKey(meta delivery.PreparedDispatch) string {
	replacements := map[string]string{
		"{prefix}":            s.prefix,
		"{sink_id}":           s.cfg.ID,
		"{batch_id}":          meta.BatchID,
		"{payload_sha256}":    strings.TrimPrefix(meta.PayloadSHA256, "sha256:"),
		"{ledger_min_offset}": strconv.FormatInt(meta.LedgerMinOffset, 10),
		"{ledger_max_offset}": strconv.FormatInt(meta.LedgerMaxOffset, 10),
	}
	key := s.keyTemplate
	for placeholder, value := range replacements {
		key = strings.ReplaceAll(key, placeholder, value)
	}
	return key
}

func hasUniquenessToken(template string) bool {
	for _, token := range []string{"{batch_id}", "{payload_sha256}", "{ledger_min_offset}", "{ledger_max_offset}"} {
		if strings.Contains(template, token) {
			return true
		}
	}
	return false
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

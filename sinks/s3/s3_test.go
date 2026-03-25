package s3sink

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type fakePutObjectClient struct {
	input     *s3.PutObjectInput
	headInput *s3.HeadBucketInput
	err       error
}

func (f *fakePutObjectClient) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.input = input
	if f.err != nil {
		return nil, f.err
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakePutObjectClient) HeadBucket(_ context.Context, input *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.headInput = input
	if f.err != nil {
		return nil, f.err
	}
	return &s3.HeadBucketOutput{}, nil
}

func TestInitRejectsNonDeterministicKeyTemplate(t *testing.T) {
	t.Parallel()

	sink := New(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
		Settings: map[string]any{
			"bucket":       "example-bucket",
			"region":       "us-west-2",
			"key_template": "{prefix}{sink_id}/latest.jsonl.gz",
		},
	})
	if err := sink.Init(context.Background()); err == nil {
		t.Fatal("expected key template validation error")
	}
}

func TestSealBatchFreezesDeterministicKey(t *testing.T) {
	t.Parallel()

	sink := New(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
		Settings: map[string]any{
			"bucket":       "example-bucket",
			"region":       "us-west-2",
			"prefix":       "oas/",
			"key_template": "{prefix}{sink_id}/{batch_id}-{payload_sha256}.jsonl.gz",
		},
	})
	sink.client = &fakePutObjectClient{}
	if err := sink.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	batch := sinkapi.Batch{
		Events: []schema.CanonicalEvent{{
			EventID:          "evt-1",
			EventVersion:     1,
			SourceType:       "codex_local",
			SourceInstanceID: "codex-a",
			SessionKey:       "sess-1",
			Sequence:         1,
			Timestamp:        time.Date(2026, 3, 25, 21, 0, 0, 0, time.UTC),
			Kind:             "session.started",
		}},
	}
	prepared, err := sink.SealBatch(context.Background(), batch, delivery.PreparedDispatch{
		SinkID:          "archive",
		BatchID:         "batch-123",
		LedgerMinOffset: 11,
		LedgerMaxOffset: 11,
		EventCount:      1,
		CreatedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	key, _ := prepared.Destination["key"].(string)
	if !strings.HasPrefix(key, "oas/archive/batch-123-") || !strings.HasSuffix(key, ".jsonl.gz") {
		t.Fatalf("resolved key = %q, want deterministic prefix/suffix", key)
	}
	if prepared.PayloadSHA256 == "" {
		t.Fatal("expected payload sha256")
	}
}

func TestSendPreparedUsesSealedObjectSettings(t *testing.T) {
	t.Parallel()

	client := &fakePutObjectClient{}
	sink := New(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
		Settings: map[string]any{
			"bucket":                 "example-bucket",
			"region":                 "us-west-2",
			"storage_class":          "STANDARD_IA",
			"server_side_encryption": "AES256",
		},
	})
	sink.client = client
	if err := sink.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
		SinkID:      "archive",
		BatchID:     "batch-1",
		Payload:     []byte("payload"),
		ContentType: "application/x-ndjson",
		Headers:     map[string]string{"Content-Encoding": "gzip"},
		EventCount:  3,
		CreatedAt:   time.Now().UTC(),
		Destination: map[string]any{
			"key":                    "oas/archive/batch-1.jsonl.gz",
			"storage_class":          "STANDARD_IA",
			"server_side_encryption": "AES256",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Acked != 3 {
		t.Fatalf("acked=%d, want 3", result.Acked)
	}
	if client.input == nil {
		t.Fatal("expected PutObject to be called")
	}
	if got := *client.input.Bucket; got != "example-bucket" {
		t.Fatalf("bucket=%q, want example-bucket", got)
	}
	if got := *client.input.Key; got != "oas/archive/batch-1.jsonl.gz" {
		t.Fatalf("key=%q, want sealed key", got)
	}
	if got := *client.input.ContentType; got != "application/x-ndjson" {
		t.Fatalf("content type=%q", got)
	}
	if client.input.ContentEncoding == nil || *client.input.ContentEncoding != "gzip" {
		t.Fatalf("content encoding=%v, want gzip", client.input.ContentEncoding)
	}
	if client.input.StorageClass != s3types.StorageClassStandardIa {
		t.Fatalf("storage class=%q, want %q", client.input.StorageClass, s3types.StorageClassStandardIa)
	}
	if client.input.ServerSideEncryption != s3types.ServerSideEncryptionAes256 {
		t.Fatalf("server side encryption=%q, want %q", client.input.ServerSideEncryption, s3types.ServerSideEncryptionAes256)
	}
	body, err := io.ReadAll(client.input.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "payload" {
		t.Fatalf("payload=%q, want payload", string(body))
	}
}

func TestSendPreparedReturnsUnderlyingClientError(t *testing.T) {
	t.Parallel()

	sink := New(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
		Settings: map[string]any{
			"bucket": "example-bucket",
			"region": "us-west-2",
		},
	})
	sink.client = &fakePutObjectClient{err: errors.New("boom")}
	if err := sink.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
		SinkID:      "archive",
		BatchID:     "batch-1",
		Payload:     []byte("payload"),
		ContentType: "application/x-ndjson",
		Destination: map[string]any{
			"key": "oas/archive/batch-1.jsonl.gz",
		},
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err=%v, want boom", err)
	}
}

func TestHealthChecksCredentialsAndBucketAccess(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	client := &fakePutObjectClient{}
	sink := New(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
		Settings: map[string]any{
			"bucket": "example-bucket",
			"region": "us-west-2",
		},
	})
	sink.client = client
	if err := sink.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if client.headInput == nil || client.headInput.Bucket == nil || *client.headInput.Bucket != "example-bucket" {
		t.Fatalf("head bucket input=%v, want example-bucket", client.headInput)
	}
}

package s3sink

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/secretref"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkauth"
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
	region       string
	auth         sinkauth.S3Settings
	resolver     *secretref.Resolver
}

type objectClient interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{
		cfg:      cfg,
		resolver: secretref.Default(),
	}
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
	s.region = sinkutil.String(s.cfg, "region")
	auth, err := sinkauth.S3(s.cfg)
	if err != nil {
		return err
	}
	s.auth = auth
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
	client, authMetrics, provider, err := s.clientFor(ctx)
	if err != nil {
		return sinkapi.Result{}, err
	}
	if _, err := client.PutObject(ctx, input); err != nil {
		return sinkapi.Result{}, classifyS3SendError(provider, err)
	}
	return sinkapi.Result{
		Acked: prepared.EventCount,
		Checkpoint: schema.SinkCheckpoint{
			SinkID:        prepared.SinkID,
			AckedAt:       time.Now().UTC(),
			DeliveryCount: prepared.EventCount,
		},
		AuthMetrics: authMetrics,
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
	client, _, provider, err := s.clientFor(ctx)
	if err != nil {
		return err
	}
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	if err != nil {
		return classifyS3SendError(provider, err)
	}
	return nil
}
func (s *Sink) Close(context.Context) error { return nil }

func (s *Sink) clientFor(ctx context.Context) (objectClient, []sinkapi.AuthMetric, string, error) {
	awsCfg, authMetrics, provider, err := s.awsConfig(ctx)
	if err != nil {
		return nil, nil, provider, err
	}
	if s.client != nil {
		return s.client, authMetrics, provider, nil
	}
	return s3.NewFromConfig(awsCfg), authMetrics, provider, nil
}

func (s *Sink) awsConfig(ctx context.Context) (aws.Config, []sinkapi.AuthMetric, string, error) {
	provider := "aws_default_chain"
	mode := s.auth.Mode
	if !s.auth.Present {
		mode = sinkauth.S3AuthModeDefaultChain
	}
	switch mode {
	case sinkauth.S3AuthModeDefaultChain:
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s.region))
		if err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		return awsCfg, nil, provider, nil
	case sinkauth.S3AuthModeProfile:
		provider = "aws_profile"
		loaders := []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(s.region),
			awsconfig.WithSharedConfigProfile(s.auth.Profile),
		}
		var authMetrics []sinkapi.AuthMetric
		if s.auth.CredentialsFileRef != "" {
			path, metric, err := s.resolvePathRef(ctx, s.auth.CredentialsFileRef)
			if err != nil {
				return aws.Config{}, nil, provider, err
			}
			loaders = append(loaders, awsconfig.WithSharedCredentialsFiles([]string{path}))
			authMetrics = append(authMetrics, metric)
		}
		if s.auth.ConfigFileRef != "" {
			path, metric, err := s.resolvePathRef(ctx, s.auth.ConfigFileRef)
			if err != nil {
				return aws.Config{}, nil, provider, err
			}
			loaders = append(loaders, awsconfig.WithSharedConfigFiles([]string{path}))
			authMetrics = append(authMetrics, metric)
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loaders...)
		if err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		return awsCfg, authMetrics, provider, nil
	case sinkauth.S3AuthModeSecretRefs:
		provider = "aws_secret_refs"
		accessKeyID, accessMetric, err := s.resolveSecretString(ctx, s.auth.AccessKeyIDRef)
		if err != nil {
			return aws.Config{}, nil, provider, err
		}
		secretAccessKey, secretMetric, err := s.resolveSecretString(ctx, s.auth.SecretAccessKeyRef)
		if err != nil {
			return aws.Config{}, nil, provider, err
		}
		sessionToken := ""
		authMetrics := []sinkapi.AuthMetric{accessMetric, secretMetric}
		if s.auth.SessionTokenRef != "" {
			token, metric, err := s.resolveSecretString(ctx, s.auth.SessionTokenRef)
			if err != nil {
				return aws.Config{}, nil, provider, err
			}
			sessionToken = token
			authMetrics = append(authMetrics, metric)
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(
			ctx,
			awsconfig.WithRegion(s.region),
			awsconfig.WithCredentialsProvider(awscredentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)),
		)
		if err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
			return aws.Config{}, nil, provider, classifyS3CredentialError(provider, err)
		}
		return awsCfg, authMetrics, provider, nil
	default:
		return aws.Config{}, nil, provider, errors.New("unsupported s3 auth mode")
	}
}

func (s *Sink) resolveSecretString(ctx context.Context, ref string) (string, sinkapi.AuthMetric, error) {
	resolved, err := s.resolver.Resolve(ctx, ref)
	if err != nil {
		return "", sinkapi.AuthMetric{}, blockedSecretErr(err)
	}
	value := strings.TrimSpace(string(resolved.Value))
	if value == "" {
		return "", sinkapi.AuthMetric{}, delivery.NewBlockedError(delivery.BlockedKindConfig, secretref.KindMissingSecret, resolved.Provider, resolved.RefFingerprint, errors.New("secret resolved empty"))
	}
	return value, sinkapi.AuthMetric{
		Provider:            resolved.Provider,
		ResolvedSecretCount: 1,
	}, nil
}

func (s *Sink) resolvePathRef(ctx context.Context, ref string) (string, sinkapi.AuthMetric, error) {
	parsed, err := secretref.Parse(ref)
	if err != nil {
		return "", sinkapi.AuthMetric{}, blockedSecretErr(err)
	}
	if parsed.Provider == secretref.ProviderFile {
		inspection, err := secretref.InspectStatic(ref, secretref.StaticOptions{})
		if err != nil {
			return "", sinkapi.AuthMetric{}, blockedSecretErr(err)
		}
		return inspection.Ref.Path, sinkapi.AuthMetric{
			Provider:            inspection.Ref.Provider,
			ResolvedSecretCount: 1,
		}, nil
	}
	path, metric, err := s.resolveSecretString(ctx, ref)
	if err != nil {
		return "", sinkapi.AuthMetric{}, err
	}
	if !strings.HasPrefix(path, "/") {
		return "", sinkapi.AuthMetric{}, delivery.NewBlockedError(delivery.BlockedKindConfig, secretref.KindInvalidRef, metric.Provider, "", errors.New("path reference must resolve to an absolute path"))
	}
	return path, metric, nil
}

func classifyS3CredentialError(provider string, err error) error {
	message := strings.ToLower(err.Error())
	switch {
	case containsAny(message, "session has expired", "expired", "reauthenticate", "failed to refresh cached credentials", "sso"):
		return delivery.NewBlockedError(delivery.BlockedKindAuth, secretref.KindAuthExpired, provider, "", err)
	case containsAny(message, "shared credentials", "credential", "profile", "not found", "no such file"):
		return delivery.NewBlockedError(delivery.BlockedKindConfig, secretref.KindMissingSecret, provider, "", err)
	case containsAny(message, "access denied", "forbidden"):
		return delivery.NewBlockedError(delivery.BlockedKindConfig, "sink_access_denied", provider, "", err)
	default:
		return err
	}
}

func classifyS3SendError(provider string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ExpiredToken", "ExpiredTokenException", "InvalidToken", "RequestExpired":
			return delivery.NewBlockedError(delivery.BlockedKindAuth, secretref.KindAuthExpired, provider, "", err)
		case "AccessDenied", "AccessDeniedException", "InvalidAccessKeyId", "SignatureDoesNotMatch", "AuthorizationHeaderMalformed", "AllAccessDisabled":
			return delivery.NewBlockedError(delivery.BlockedKindConfig, "sink_access_denied", provider, "", err)
		}
	}
	message := strings.ToLower(err.Error())
	switch {
	case containsAny(message, "session has expired", "expired", "reauthenticate", "failed to refresh cached credentials"):
		return delivery.NewBlockedError(delivery.BlockedKindAuth, secretref.KindAuthExpired, provider, "", err)
	case containsAny(message, "access denied", "invalidaccesskeyid", "signaturedoesnotmatch"):
		return delivery.NewBlockedError(delivery.BlockedKindConfig, "sink_access_denied", provider, "", err)
	default:
		return err
	}
}

func blockedSecretErr(err error) error {
	var refErr *secretref.Error
	if !errors.As(err, &refErr) {
		return err
	}
	switch refErr.BlockedKind() {
	case "auth":
		return delivery.NewBlockedError(delivery.BlockedKindAuth, refErr.Kind, refErr.Provider, refErr.RefFingerprint, refErr)
	case "config":
		return delivery.NewBlockedError(delivery.BlockedKindConfig, refErr.Kind, refErr.Provider, refErr.RefFingerprint, refErr)
	default:
		return err
	}
}

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

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

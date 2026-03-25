package sinkpayload

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

const (
	FormatOASBatchJSON     = "oas_batch_json"
	FormatCanonicalJSONL   = "canonical_jsonl"
	FormatCanonicalJSONLGZ = "canonical_jsonl.gz"
)

func EncodeBatch(batch sinkapi.Batch, format string) ([]byte, string, map[string]string, error) {
	switch format {
	case "", FormatOASBatchJSON:
		payload, err := json.Marshal(batch)
		if err != nil {
			return nil, "", nil, err
		}
		return payload, "application/json", map[string]string{"Content-Type": "application/json"}, nil
	case FormatCanonicalJSONL:
		payload, err := canonicalJSONL(batch.Events)
		if err != nil {
			return nil, "", nil, err
		}
		return payload, "application/x-ndjson", map[string]string{"Content-Type": "application/x-ndjson"}, nil
	case FormatCanonicalJSONLGZ:
		payload, err := canonicalJSONL(batch.Events)
		if err != nil {
			return nil, "", nil, err
		}
		compressed, err := gzipBytes(payload)
		if err != nil {
			return nil, "", nil, err
		}
		return compressed, "application/x-ndjson", map[string]string{
			"Content-Type":     "application/x-ndjson",
			"Content-Encoding": "gzip",
		}, nil
	default:
		return nil, "", nil, errors.New("unsupported payload format: " + format)
	}
}

func canonicalJSONL(events []schema.CanonicalEvent) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func gzipBytes(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(payload); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

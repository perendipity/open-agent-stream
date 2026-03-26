package analytics

import (
	"context"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
	"github.com/open-agent-stream/open-agent-stream/internal/redact"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type sensitivePreview struct {
	FirstUser string
	LastAgent string
	Filtered  bool
	SawText   bool
}

func (s *Service) augmentSensitivePreset(ctx context.Context, presetName string, result presetResult) (presetResult, error) {
	keys := map[string]*sensitivePreview{}
	for _, row := range result.Rows {
		key := hiddenString(row, "global_session_key")
		if key == "" {
			continue
		}
		keys[key] = &sensitivePreview{}
	}
	result = appendSensitiveColumns(result, presetName)
	if len(keys) == 0 {
		for idx := range result.Rows {
			result.Rows[idx]["content_status"] = "empty"
			result.Rows[idx]["first_user_preview"] = "-"
			result.Rows[idx]["last_agent_preview"] = "-"
		}
		return result, nil
	}

	redactor := redact.NewEngine(s.cfg)
	normalizer := normalize.NewService(normalize.NewMemorySequenceStore())
	current := int64(0)

	for {
		records, err := s.ledger.ListAfter(current, s.cfg.BatchSize)
		if err != nil {
			return presetResult{}, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			select {
			case <-ctx.Done():
				return presetResult{}, ctx.Err()
			default:
			}

			current = record.Offset
			event, err := normalizer.Normalize(record)
			if err != nil {
				return presetResult{}, err
			}
			if event.Kind != "message.user" && event.Kind != "message.agent" {
				continue
			}
			sessionKey := globalSessionKey(s.cfg.MachineID, event.SourceInstanceID, event.Context.SourceSessionKey)
			preview := keys[sessionKey]
			if preview == nil {
				continue
			}

			filtered, _, err := redactor.Apply(analyticsSensitivePolicyID, sinkapi.Batch{
				Events: []schema.CanonicalEvent{event},
			})
			if err != nil {
				return presetResult{}, err
			}
			if len(filtered.Events) == 0 {
				preview.Filtered = true
				continue
			}

			text := sanitizePreviewText(payloadString(filtered.Events[0].Payload, "text"))
			if text == "" {
				continue
			}
			preview.SawText = true
			switch filtered.Events[0].Kind {
			case "message.user":
				if preview.FirstUser == "" {
					preview.FirstUser = text
				}
			case "message.agent":
				preview.LastAgent = text
			}
		}
	}

	for idx, row := range result.Rows {
		key := hiddenString(row, "global_session_key")
		preview := keys[key]
		if preview == nil {
			result.Rows[idx]["content_status"] = "empty"
			result.Rows[idx]["first_user_preview"] = "-"
			result.Rows[idx]["last_agent_preview"] = "-"
			continue
		}
		result.Rows[idx]["content_status"] = preview.contentStatus()
		result.Rows[idx]["first_user_preview"] = displayPreview(preview.FirstUser)
		result.Rows[idx]["last_agent_preview"] = displayPreview(preview.LastAgent)
	}

	return result, nil
}

func (p sensitivePreview) contentStatus() string {
	if hasVisiblePreview(p.FirstUser, p.LastAgent) {
		return "available"
	}
	if hasRedactedPreview(p.FirstUser, p.LastAgent) {
		return "redacted"
	}
	if p.Filtered {
		return "filtered"
	}
	if p.SawText {
		return "empty"
	}
	return "empty"
}

func hasVisiblePreview(values ...string) bool {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && value != "[REDACTED]" {
			return true
		}
	}
	return false
}

func hasRedactedPreview(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "[REDACTED]" {
			return true
		}
	}
	return false
}

func displayPreview(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func sanitizePreviewText(value string) string {
	value = strings.ReplaceAll(value, "```", " ")
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= previewMaxChars {
		return value
	}
	return strings.TrimSpace(string(runes[:previewMaxChars-1])) + "…"
}

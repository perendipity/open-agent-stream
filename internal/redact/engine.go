package redact

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Engine struct {
	cfg config.Config
}

func NewEngine(cfg config.Config) *Engine {
	return &Engine{cfg: cfg}
}

func (e *Engine) Apply(sinkID string, batch sinkapi.Batch) (sinkapi.Batch, schema.RedactionPolicyView, error) {
	policy, view := config.EffectivePolicy(e.cfg, sinkID)
	filtered := sinkapi.Batch{
		Events:       make([]schema.CanonicalEvent, 0, len(batch.Events)),
		RawEnvelopes: make([]schema.RawEnvelope, 0, len(batch.RawEnvelopes)),
	}

	for _, event := range batch.Events {
		if shouldDrop(policy, event.Context.ProjectLocator) {
			continue
		}
		filtered.Events = append(filtered.Events, redactEvent(policy, event))
	}
	if !policy.DropRaw {
		for _, envelope := range batch.RawEnvelopes {
			if shouldDrop(policy, envelope.ProjectLocator) {
				continue
			}
			filtered.RawEnvelopes = append(filtered.RawEnvelopes, redactEnvelope(policy, envelope))
		}
	}

	return filtered, view, nil
}

func shouldDrop(policy config.Policy, projectLocator string) bool {
	if len(policy.AllowedProjects) > 0 {
		allowed := false
		for _, project := range policy.AllowedProjects {
			if project == projectLocator {
				allowed = true
				break
			}
		}
		if !allowed {
			return true
		}
	}
	for _, denied := range policy.DeniedPaths {
		if denied == "" {
			continue
		}
		clean := filepath.Clean(denied)
		if strings.HasPrefix(filepath.Clean(projectLocator), clean) {
			return true
		}
	}
	return false
}

func redactEvent(policy config.Policy, event schema.CanonicalEvent) schema.CanonicalEvent {
	cloned := event
	cloned.Payload = redactMap(policy, schema.CloneMap(event.Payload))
	return cloned
}

func redactEnvelope(policy config.Policy, envelope schema.RawEnvelope) schema.RawEnvelope {
	cloned := envelope
	var payload map[string]any
	if err := json.Unmarshal(envelope.RawPayload, &payload); err == nil {
		payload = redactMap(policy, payload)
		if data, err := json.Marshal(payload); err == nil {
			cloned.RawPayload = data
		}
	}
	return cloned
}

func redactMap(policy config.Policy, payload map[string]any) map[string]any {
	for key, value := range payload {
		if contains(policy.RedactKeys, key) {
			payload[key] = "[REDACTED]"
			continue
		}
		payload[key] = redactValue(policy, value)
	}
	return payload
}

func redactValue(policy config.Policy, value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return redactMap(policy, typed)
	case []any:
		for idx := range typed {
			typed[idx] = redactValue(policy, typed[idx])
		}
		return typed
	case string:
		return applyRegexes(policy.Regexes, typed)
	default:
		return typed
	}
}

func applyRegexes(rules []config.RegexRule, input string) string {
	output := input
	for _, rule := range rules {
		re, err := regexp.Compile(rule.Pattern)
		if err != nil {
			continue
		}
		output = re.ReplaceAllString(output, rule.Replacement)
	}
	return output
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

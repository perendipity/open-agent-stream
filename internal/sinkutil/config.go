package sinkutil

import (
	"fmt"
	"strconv"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func String(cfg sinkapi.Config, key string) string {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case string:
				return typed
			}
		}
	}
	if cfg.Options != nil {
		return cfg.Options[key]
	}
	return ""
}

func Bool(cfg sinkapi.Config, key string) bool {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case bool:
				return typed
			case string:
				parsed, _ := strconv.ParseBool(typed)
				return parsed
			}
		}
	}
	if cfg.Options != nil {
		parsed, _ := strconv.ParseBool(cfg.Options[key])
		return parsed
	}
	return false
}

func Int(cfg sinkapi.Config, key string, fallback int) int {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case float64:
				return int(typed)
			case int:
				return typed
			case string:
				parsed, err := strconv.Atoi(typed)
				if err == nil {
					return parsed
				}
			}
		}
	}
	if cfg.Options != nil {
		if parsed, err := strconv.Atoi(cfg.Options[key]); err == nil {
			return parsed
		}
	}
	return fallback
}

func Int64(cfg sinkapi.Config, key string, fallback int64) int64 {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case float64:
				return int64(typed)
			case int64:
				return typed
			case int:
				return int64(typed)
			case string:
				parsed, err := strconv.ParseInt(typed, 10, 64)
				if err == nil {
					return parsed
				}
			}
		}
	}
	if cfg.Options != nil {
		if parsed, err := strconv.ParseInt(cfg.Options[key], 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func Duration(cfg sinkapi.Config, key string, fallback time.Duration) (time.Duration, error) {
	raw := String(cfg, key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func StringSlice(cfg sinkapi.Config, key string) []string {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
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
			}
		}
	}
	return nil
}

func StringMap(cfg sinkapi.Config, key string) map[string]string {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case map[string]string:
				out := make(map[string]string, len(typed))
				for k, v := range typed {
					out[k] = v
				}
				return out
			case map[string]any:
				out := make(map[string]string, len(typed))
				for k, v := range typed {
					if s, ok := v.(string); ok {
						out[k] = s
					}
				}
				return out
			}
		}
	}
	return nil
}

func Map(cfg sinkapi.Config, key string) map[string]any {
	if cfg.Settings != nil {
		if value, ok := cfg.Settings[key]; ok {
			switch typed := value.(type) {
			case map[string]any:
				out := make(map[string]any, len(typed))
				for k, v := range typed {
					out[k] = v
				}
				return out
			}
		}
	}
	return nil
}

func StringFromMap(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	if value, ok := values[key]; ok {
		if typed, ok := value.(string); ok {
			return typed
		}
	}
	return ""
}

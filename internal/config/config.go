package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type RegexRule struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
}

type Policy struct {
	DropRaw         bool        `json:"drop_raw,omitempty"`
	RedactKeys      []string    `json:"redact_keys,omitempty"`
	Regexes         []RegexRule `json:"regexes,omitempty"`
	AllowedProjects []string    `json:"allowed_projects,omitempty"`
	DeniedPaths     []string    `json:"denied_paths,omitempty"`
}

type PrivacyConfig struct {
	Default Policy            `json:"default"`
	PerSink map[string]Policy `json:"per_sink,omitempty"`
}

type Config struct {
	Version              string             `json:"version"`
	MachineID            string             `json:"machine_id,omitempty"`
	StatePath            string             `json:"state_path,omitempty"`
	LedgerPath           string             `json:"ledger_path,omitempty"`
	BatchSize            int                `json:"batch_size,omitempty"`
	PollInterval         string             `json:"poll_interval,omitempty"`
	ErrorBackoff         string             `json:"error_backoff,omitempty"`
	MaxConsecutiveErrors int                `json:"max_consecutive_errors,omitempty"`
	Sources              []sourceapi.Config `json:"sources"`
	Sinks                []sinkapi.Config   `json:"sinks"`
	Privacy              PrivacyConfig      `json:"privacy,omitempty"`
}

func Load(path string) (Config, error) {
	if path == "" {
		return Config{}, errors.New("config path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	applyDefaults(&cfg)
	return cfg, Validate(cfg)
}

func Validate(cfg Config) error {
	var errs []error
	if _, err := cfg.PollIntervalValue(); err != nil {
		errs = append(errs, fmt.Errorf("poll_interval: %w", err))
	}
	if _, err := cfg.ErrorBackoffValue(); err != nil {
		errs = append(errs, fmt.Errorf("error_backoff: %w", err))
	}
	if _, err := cfg.MaxConsecutiveErrorsValue(); err != nil {
		errs = append(errs, fmt.Errorf("max_consecutive_errors: %w", err))
	}
	if len(cfg.Sources) == 0 {
		errs = append(errs, errors.New("at least one source is required"))
	}
	if len(cfg.Sinks) == 0 {
		errs = append(errs, errors.New("at least one sink is required"))
	}
	for _, source := range cfg.Sources {
		if source.InstanceID == "" {
			errs = append(errs, errors.New("source.instance_id is required"))
		}
		if source.Type == "" {
			errs = append(errs, errors.New("source.type is required"))
		}
		if source.Root == "" {
			errs = append(errs, errors.New("source.root is required"))
		}
	}
	for _, sink := range cfg.Sinks {
		if sink.ID == "" {
			errs = append(errs, errors.New("sink.id is required"))
		}
		if sink.Type == "" {
			errs = append(errs, errors.New("sink.type is required"))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".open-agent-stream"
	}
	return filepath.Join(home, ".open-agent-stream")
}

func EffectivePolicy(cfg Config, sinkID string) (Policy, schema.RedactionPolicyView) {
	policy := cfg.Privacy.Default
	if override, ok := cfg.Privacy.PerSink[sinkID]; ok {
		if len(override.RedactKeys) > 0 {
			policy.RedactKeys = override.RedactKeys
		}
		if len(override.Regexes) > 0 {
			policy.Regexes = override.Regexes
		}
		if len(override.AllowedProjects) > 0 {
			policy.AllowedProjects = override.AllowedProjects
		}
		if len(override.DeniedPaths) > 0 {
			policy.DeniedPaths = override.DeniedPaths
		}
		policy.DropRaw = override.DropRaw
	}
	view := schema.RedactionPolicyView{
		PolicyID:        sinkID,
		DropRaw:         policy.DropRaw,
		RedactedKeys:    append([]string(nil), policy.RedactKeys...),
		RegexCount:      len(policy.Regexes),
		AllowedProjects: append([]string(nil), policy.AllowedProjects...),
		DeniedPaths:     append([]string(nil), policy.DeniedPaths...),
	}
	return policy, view
}

func applyDefaults(cfg *Config) {
	if cfg.Version == "" {
		cfg.Version = "0.1"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.PollInterval == "" {
		cfg.PollInterval = (3 * time.Second).String()
	}
	if cfg.ErrorBackoff == "" {
		cfg.ErrorBackoff = (10 * time.Second).String()
	}
	if cfg.MaxConsecutiveErrors <= 0 {
		cfg.MaxConsecutiveErrors = 10
	}
	if cfg.StatePath == "" {
		cfg.StatePath = filepath.Join(DefaultDataDir(), "state.db")
	}
	if cfg.LedgerPath == "" {
		cfg.LedgerPath = filepath.Join(DefaultDataDir(), "ledger.db")
	}
	if cfg.Privacy.PerSink == nil {
		cfg.Privacy.PerSink = map[string]Policy{}
	}
}

func (cfg Config) PollIntervalValue() (time.Duration, error) {
	return parsePositiveDuration(cfg.PollInterval, 3*time.Second)
}

func (cfg Config) ErrorBackoffValue() (time.Duration, error) {
	return parsePositiveDuration(cfg.ErrorBackoff, 10*time.Second)
}

func (cfg Config) MaxConsecutiveErrorsValue() (int, error) {
	if cfg.MaxConsecutiveErrors <= 0 {
		return 0, errors.New("must be greater than zero")
	}
	return cfg.MaxConsecutiveErrors, nil
}

func parsePositiveDuration(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, errors.New("must be greater than zero")
	}
	return value, nil
}

package conformance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

func LoadExpectedEvents(path string) ([]schema.CanonicalEvent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var events []schema.CanonicalEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func ValidateFixtures(root string) error {
	fixturesRoot := filepath.Join(root, "fixtures")
	expectedRoot := filepath.Join(fixturesRoot, "expected")
	sourceRoot := filepath.Join(fixturesRoot, "sources")

	entries := []string{
		filepath.Join(expectedRoot, "codex_events.json"),
		filepath.Join(expectedRoot, "claude_events.json"),
		filepath.Join(sourceRoot, "codex", "session.jsonl"),
		filepath.Join(sourceRoot, "claude", "session.jsonl"),
	}

	var errs []error
	for _, entry := range entries {
		info, err := os.Stat(entry)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if info.IsDir() {
			errs = append(errs, errors.New(entry+": expected file, found directory"))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	if _, err := LoadExpectedEvents(filepath.Join(expectedRoot, "codex_events.json")); err != nil {
		errs = append(errs, err)
	}
	if _, err := LoadExpectedEvents(filepath.Join(expectedRoot, "claude_events.json")); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

package hermes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Type() string {
	return "hermes_local"
}

func (a *Adapter) Capabilities() []sourceapi.Capability {
	return []sourceapi.Capability{
		sourceapi.CapabilityMessages,
		sourceapi.CapabilityCommands,
		sourceapi.CapabilityToolCalls,
		sourceapi.CapabilityFileOps,
		sourceapi.CapabilityUsage,
	}
}

func (a *Adapter) Discover(_ context.Context, cfg sourceapi.Config) ([]sourceapi.Artifact, error) {
	return discoverDBArtifacts(cfg.Root, cfg.Options)
}

func discoverDBArtifacts(root string, options map[string]string) ([]sourceapi.Artifact, error) {
	resolvedRoot := expandPath(root)
	if options == nil {
		options = map[string]string{}
	}

	if dbPath := strings.TrimSpace(options["db_path"]); dbPath != "" {
		artifact, ok, err := artifactForDB(resolvedRoot, "default", expandPath(dbPath), false)
		if err != nil || !ok {
			return nil, err
		}
		return []sourceapi.Artifact{artifact}, nil
	}

	var artifacts []sourceapi.Artifact
	if artifact, ok, err := artifactForDB(resolvedRoot, "default", filepath.Join(resolvedRoot, "state.db"), true); err != nil {
		return nil, err
	} else if ok {
		artifacts = append(artifacts, artifact)
	}

	profilesOption := strings.TrimSpace(options["profiles"])
	switch {
	case strings.EqualFold(profilesOption, "all"):
		entries, err := os.ReadDir(filepath.Join(resolvedRoot, "profiles"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, entry := range entries {
			profile := entry.Name()
			if !entry.IsDir() || !isProfileName(profile) {
				continue
			}
			path := filepath.Join(resolvedRoot, "profiles", profile, "state.db")
			artifact, ok, err := artifactForDB(resolvedRoot, profile, path, true)
			if err != nil {
				return nil, err
			}
			if ok {
				artifacts = append(artifacts, artifact)
			}
		}
	case profilesOption != "":
		seen := make(map[string]struct{})
		for _, profile := range splitProfiles(profilesOption) {
			if !isProfileName(profile) {
				continue
			}
			if _, ok := seen[profile]; ok {
				continue
			}
			seen[profile] = struct{}{}
			path := filepath.Join(resolvedRoot, "profiles", profile, "state.db")
			artifact, ok, err := artifactForDB(resolvedRoot, profile, path, true)
			if err != nil {
				return nil, err
			}
			if ok {
				artifacts = append(artifacts, artifact)
			}
		}
	}

	return artifacts, nil
}

func artifactForDB(root, profile, path string, enforceUnderRoot bool) (sourceapi.Artifact, bool, error) {
	resolvedPath := expandPath(path)
	info, err := os.Lstat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sourceapi.Artifact{}, false, nil
		}
		return sourceapi.Artifact{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		if enforceUnderRoot {
			return sourceapi.Artifact{}, false, nil
		}
		info, err = os.Stat(resolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return sourceapi.Artifact{}, false, nil
			}
			return sourceapi.Artifact{}, false, err
		}
	}
	if info.IsDir() {
		return sourceapi.Artifact{}, false, nil
	}
	if enforceUnderRoot && !isPathWithinRoot(root, resolvedPath) {
		return sourceapi.Artifact{}, false, nil
	}
	if profile == "" {
		profile = "default"
	}
	return sourceapi.Artifact{
		ID:             schema.StableID("art", "hermes", profile, resolvedPath),
		Locator:        resolvedPath,
		ProjectLocator: root,
		Fingerprint:    schema.StableID("fp", resolvedPath, strconv.FormatInt(info.Size(), 10), info.ModTime().UTC().Format(time.RFC3339Nano)),
		Metadata: map[string]string{
			"profile": profile,
		},
	}, true, nil
}

func expandPath(path string) string {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			switch {
			case path == "~":
				path = home
			case strings.HasPrefix(path, "~/"):
				path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
			}
		}
	}
	if resolved, err := filepath.Abs(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}

func splitProfiles(value string) []string {
	parts := strings.Split(value, ",")
	profiles := make([]string, 0, len(parts))
	for _, part := range parts {
		profile := strings.TrimSpace(part)
		if profile != "" {
			profiles = append(profiles, profile)
		}
	}
	return profiles
}

func isProfileName(profile string) bool {
	return profile != "" && profile != "." && profile != ".." && filepath.Base(profile) == profile && !strings.ContainsAny(profile, `/\\`)
}

func isPathWithinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (a *Adapter) Read(_ context.Context, _ sourceapi.Config, _ sourceapi.Artifact, checkpoint sourceapi.Checkpoint) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
	return nil, checkpoint, fmt.Errorf("hermes_local read is not implemented yet; this adapter currently supports discovery only")
}

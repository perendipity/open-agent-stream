package main

import (
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
)

type configInspectView struct {
	ConfigPath string              `json:"config_path"`
	ConfigDir  string              `json:"config_dir"`
	Config     config.Config       `json:"config"`
	Resolved   configResolvedPaths `json:"resolved"`
	Warnings   []string            `json:"warnings,omitempty"`
}

type configResolvedPaths struct {
	DataDir        string                     `json:"data_dir"`
	StatePath      string                     `json:"state_path"`
	StateDir       string                     `json:"state_dir"`
	LedgerPath     string                     `json:"ledger_path"`
	LedgerDir      string                     `json:"ledger_dir"`
	DaemonPIDPath  string                     `json:"daemon_pid_path"`
	DaemonLogPath  string                     `json:"daemon_log_path"`
	DaemonMetaPath string                     `json:"daemon_meta_path"`
	Sources        []configResolvedSourceRoot `json:"sources,omitempty"`
}

type configResolvedSourceRoot struct {
	InstanceID string `json:"instance_id"`
	Type       string `json:"type"`
	Root       string `json:"root"`
}

func buildConfigInspectView(configPath string, cfg config.Config) configInspectView {
	absConfigPath := absolutePathOrValue(configPath)
	paths := daemonPathsFor(cfg)
	view := configInspectView{
		ConfigPath: absConfigPath,
		ConfigDir:  filepath.Dir(absConfigPath),
		Config:     cfg,
		Warnings:   config.Warnings(cfg),
		Resolved: configResolvedPaths{
			DataDir:        absolutePathOrValue(cfg.DataDir),
			StatePath:      absolutePathOrValue(cfg.StatePath),
			StateDir:       absolutePathOrValue(filepath.Dir(cfg.StatePath)),
			LedgerPath:     absolutePathOrValue(cfg.LedgerPath),
			LedgerDir:      absolutePathOrValue(filepath.Dir(cfg.LedgerPath)),
			DaemonPIDPath:  absolutePathOrValue(paths.pidPath),
			DaemonLogPath:  absolutePathOrValue(paths.logPath),
			DaemonMetaPath: absolutePathOrValue(paths.metaPath),
		},
	}
	for _, source := range cfg.Sources {
		view.Resolved.Sources = append(view.Resolved.Sources, configResolvedSourceRoot{
			InstanceID: source.InstanceID,
			Type:       source.Type,
			Root:       absolutePathOrValue(source.Root),
		})
	}
	return view
}

func absolutePathOrValue(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func writeConfigInspectReport(writer io.Writer, view configInspectView) error {
	var b strings.Builder

	appendKeyValueSection(&b, "Config",
		[2]string{"File", view.ConfigPath},
		[2]string{"Directory", view.ConfigDir},
		[2]string{"Version", view.Config.Version},
		[2]string{"Machine ID", view.Config.MachineID},
	)
	appendBulletSection(&b, "Warnings", view.Warnings)
	appendKeyValueSection(&b, "Runtime",
		[2]string{"Batch size", strconv.Itoa(view.Config.BatchSize)},
		[2]string{"Poll interval", view.Config.PollInterval},
		[2]string{"Error backoff", view.Config.ErrorBackoff},
		[2]string{"Max consecutive errors", strconv.Itoa(view.Config.MaxConsecutiveErrors)},
	)
	appendKeyValueSection(&b, "Storage guard",
		[2]string{"Max storage bytes", strconv.FormatInt(view.Config.MaxStorageBytes, 10)},
		[2]string{"Prune target bytes", strconv.FormatInt(view.Config.PruneTargetBytes, 10)},
		[2]string{"Min free bytes", strconv.FormatInt(view.Config.MinFreeBytes, 10)},
	)
	appendKeyValueSection(&b, "Resolved paths",
		[2]string{"Data dir", view.Resolved.DataDir},
		[2]string{"State DB", view.Resolved.StatePath},
		[2]string{"Ledger DB", view.Resolved.LedgerPath},
		[2]string{"Daemon PID", view.Resolved.DaemonPIDPath},
		[2]string{"Daemon log", view.Resolved.DaemonLogPath},
		[2]string{"Daemon metadata", view.Resolved.DaemonMetaPath},
	)

	if len(view.Resolved.Sources) > 0 {
		b.WriteString("Sources:\n")
		for _, source := range view.Resolved.Sources {
			fmt.Fprintf(&b, "  - %s (%s): %s\n", source.InstanceID, source.Type, source.Root)
		}
		b.WriteString("\n")
	}

	if len(view.Config.Sinks) > 0 {
		b.WriteString("Sinks:\n")
		for _, sink := range view.Config.Sinks {
			fmt.Fprintf(&b, "  - %s (%s): event_spec=%s max_batch_events=%d max_batch_age=%s window_every=%s retry_initial=%s retry_max=%s poison_after=%d\n",
				sink.ID,
				sink.Type,
				config.EffectiveSinkEventSpecVersion(sink.EventSpecVersion),
				sink.Delivery.MaxBatchEvents,
				emptyConfigValue(sink.Delivery.MaxBatchAge),
				emptyConfigValue(sink.Delivery.WindowEvery),
				emptyConfigValue(sink.Delivery.RetryInitialBackoff),
				emptyConfigValue(sink.Delivery.RetryMaxBackoff),
				sink.Delivery.PoisonAfterFailures,
			)
		}
		b.WriteString("\n")
	}

	appendKeyValueSection(&b, "Privacy",
		[2]string{"Default redact keys", strconv.Itoa(len(view.Config.Privacy.Default.RedactKeys))},
		[2]string{"Default regexes", strconv.Itoa(len(view.Config.Privacy.Default.Regexes))},
		[2]string{"Per-sink overrides", strconv.Itoa(len(view.Config.Privacy.PerSink))},
	)

	_, err := io.WriteString(writer, strings.TrimRight(b.String(), "\n")+"\n")
	return err
}

func appendKeyValueSection(builder *strings.Builder, title string, rows ...[2]string) {
	filtered := make([][2]string, 0, len(rows))
	width := 0
	for _, row := range rows {
		if strings.TrimSpace(row[1]) == "" {
			continue
		}
		filtered = append(filtered, row)
		if len(row[0]) > width {
			width = len(row[0])
		}
	}
	if len(filtered) == 0 {
		return
	}
	builder.WriteString(title)
	builder.WriteString(":\n")
	for _, row := range filtered {
		fmt.Fprintf(builder, "  %-*s  %s\n", width, row[0], row[1])
	}
	builder.WriteString("\n")
}

func emptyConfigValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

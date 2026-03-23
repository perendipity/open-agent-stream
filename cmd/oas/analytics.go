package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/analytics"
)

func analyticsCommand(ctx context.Context, args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		analyticsUsage()
		if len(args) == 0 {
			os.Exit(2)
		}
		return
	}
	switch args[0] {
	case "build":
		analyticsBuildCommand(ctx, args[1:])
	case "query":
		analyticsQueryCommand(ctx, args[1:])
	case "export":
		analyticsExportCommand(ctx, args[1:])
	case "status":
		analyticsStatusCommand(ctx, args[1:])
	default:
		analyticsUsage()
		os.Exit(2)
	}
}

func analyticsBuildCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("analytics build", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	dbPath := fs.String("db", "", "analytics cache path (default near the resolved state path)")
	rebuild := fs.Bool("rebuild", false, "discard the existing analytics cache and rebuild from the retained ledger")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a report")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas analytics build -config <path> [flags]

Build or refresh the local DuckDB analytics cache from retained ledger history.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "db", Placeholder: "<path>"},
			usageFlag{Name: "rebuild"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas analytics build -config ./oas.json",
			"oas analytics build -config ./oas.json -rebuild",
		)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	status, err := runtime.Analytics().Build(ctx, analytics.BuildOptions{
		Path:    *dbPath,
		Rebuild: *rebuild,
	})
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, status); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeAnalyticsStatus(os.Stdout, status); err != nil {
		fatal(err)
	}
}

func analyticsQueryCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("analytics query", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	dbPath := fs.String("db", "", "analytics cache path (default near the resolved state path)")
	preset := fs.String("preset", "", "built-in query preset: overview, attention, recent_sessions, projects, sources, command_health, timeline, coverage")
	sqlText := fs.String("sql", "", "run custom SQL against the stable public analytics views")
	limit := fs.Int("limit", 20, "maximum rows for row-oriented presets (0 uses the preset default)")
	sensitive := fs.Bool("sensitive", false, "augment supported presets with transient redacted message previews")
	rebuild := fs.Bool("rebuild", false, "rebuild the cache first if it is not append-compatible")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas analytics query -config <path> [flags]

Query the local DuckDB analytics cache. Custom SQL should target the stable
public views: envelope_facts, event_facts, session_rollups, and command_rollups.
Use -sensitive only with built-in presets that support transient local previews.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
			usageFlag{Name: "preset", Placeholder: "<overview|attention|recent_sessions|projects|sources|command_health|timeline|coverage>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "sql", Placeholder: "<query>"},
			usageFlag{Name: "limit", Placeholder: "<n>"},
			usageFlag{Name: "sensitive"},
			usageFlag{Name: "db", Placeholder: "<path>"},
			usageFlag{Name: "rebuild"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas analytics query -config ./oas.json",
			"oas analytics query -config ./oas.json -preset attention",
			"oas analytics query -config ./oas.json -preset recent_sessions -sensitive",
			"oas analytics query -config ./oas.json -sql 'select kind, count(*) as n from event_facts group by 1 order by 2 desc'",
		)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	columns, rows, status, err := runtime.Analytics().Query(ctx, analytics.QueryOptions{
		Path:      *dbPath,
		Preset:    *preset,
		SQL:       *sqlText,
		Limit:     *limit,
		Sensitive: *sensitive,
		Rebuild:   *rebuild,
	})
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		payload := map[string]any{
			"status":  status,
			"columns": columns,
			"rows":    rowsToMaps(columns, rows),
		}
		if err := writeJSON(os.Stdout, payload); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeQueryTable(os.Stdout, columns, rows); err != nil {
		fatal(err)
	}
}

func analyticsExportCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("analytics export", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	dbPath := fs.String("db", "", "analytics cache path (default near the resolved state path)")
	outputDir := fs.String("output", "", "output directory for the Parquet snapshot dataset")
	rebuild := fs.Bool("rebuild", false, "rebuild the cache first if it is not append-compatible")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a summary")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas analytics export -config <path> -output <dir> [flags]

Export a replaceable Parquet analytics snapshot plus manifest.json.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
			usageFlag{Name: "output", Placeholder: "<dir>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "db", Placeholder: "<path>"},
			usageFlag{Name: "rebuild"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas analytics export -config ./oas.json -output ./exports/analytics",
			"oas analytics export -config ./oas.json -output ./exports/analytics -rebuild",
		)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	manifest, status, err := runtime.Analytics().Export(ctx, analytics.ExportOptions{
		Path:      *dbPath,
		OutputDir: *outputDir,
		Rebuild:   *rebuild,
	})
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		payload := map[string]any{
			"status":   status,
			"manifest": manifest,
		}
		if err := writeJSON(os.Stdout, payload); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeAnalyticsExport(os.Stdout, manifest, status); err != nil {
		fatal(err)
	}
}

func analyticsStatusCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("analytics status", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	dbPath := fs.String("db", "", "analytics cache path (default near the resolved state path)")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a report")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas analytics status -config <path> [flags]

Show analytics cache status, lineage, and completeness details.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "db", Placeholder: "<path>"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas analytics status -config ./oas.json",
			"oas analytics status -config ./oas.json -json",
		)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	status, err := runtime.Analytics().Status(ctx, *dbPath)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, status); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeAnalyticsStatus(os.Stdout, status); err != nil {
		fatal(err)
	}
}

func analyticsUsage() {
	writeAnalyticsUsage(os.Stderr)
}

func writeAnalyticsUsage(writer io.Writer) {
	fmt.Fprintf(writer, `usage: oas analytics <subcommand> -config <path>

Subcommands:
  build     Build or refresh the local DuckDB analytics cache
  query     Run presets or custom SQL against analytics views
  export    Export a Parquet analytics snapshot plus manifest.json
  status    Show analytics cache lineage and completeness

Examples:
  oas analytics build -config ./oas.json
  oas analytics query -config ./oas.json -preset attention
  oas analytics export -config ./oas.json -output ./exports/analytics
  oas analytics status -config ./oas.json

Use:
  oas analytics <subcommand> --help
`)
}

func writeAnalyticsStatus(target io.Writer, status analytics.Status) error {
	meta := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(target, "ANALYTICS"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "path:\t%s\n", status.Path); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(meta, "exists:\t%s\n", strconv.FormatBool(status.Exists)); err != nil {
		return err
	}
	if !status.Exists {
		if err := meta.Flush(); err != nil {
			return err
		}
		_, err := fmt.Fprintln(target, "\ncache not built")
		return err
	}
	if _, err := fmt.Fprintf(meta, "size_bytes:\t%d\n", status.SizeBytes); err != nil {
		return err
	}
	if status.DatasetVersion != "" {
		if _, err := fmt.Fprintf(meta, "dataset_version:\t%s\n", status.DatasetVersion); err != nil {
			return err
		}
	}
	if status.DerivationVersion != "" {
		if _, err := fmt.Fprintf(meta, "derivation_version:\t%s\n", status.DerivationVersion); err != nil {
			return err
		}
	}
	if status.EngineVersion != "" {
		if _, err := fmt.Fprintf(meta, "engine_version:\t%s\n", status.EngineVersion); err != nil {
			return err
		}
	}
	if status.MachineID != "" {
		if _, err := fmt.Fprintf(meta, "machine_id:\t%s\n", status.MachineID); err != nil {
			return err
		}
	}
	if status.MachineIDOrigin != "" {
		if _, err := fmt.Fprintf(meta, "machine_id_origin:\t%s\n", status.MachineIDOrigin); err != nil {
			return err
		}
	}
	if status.LedgerInstanceID != "" {
		if _, err := fmt.Fprintf(meta, "ledger_instance_id:\t%s\n", status.LedgerInstanceID); err != nil {
			return err
		}
	}
	if status.BuildState != "" {
		if _, err := fmt.Fprintf(meta, "build_state:\t%s\n", status.BuildState); err != nil {
			return err
		}
	}
	if status.LastCleanOffset > 0 {
		if _, err := fmt.Fprintf(meta, "last_clean_offset:\t%d\n", status.LastCleanOffset); err != nil {
			return err
		}
	}
	if status.CoverageMode != "" {
		if _, err := fmt.Fprintf(meta, "coverage_mode:\t%s\n", status.CoverageMode); err != nil {
			return err
		}
	}
	if status.CacheCoveredOffsetRange != nil {
		if _, err := fmt.Fprintf(meta, "cache_covered_offsets:\t%d..%d\n", status.CacheCoveredOffsetRange.Min, status.CacheCoveredOffsetRange.Max); err != nil {
			return err
		}
	}
	if status.RetainedOffsetRange != nil {
		if _, err := fmt.Fprintf(meta, "retained_offsets:\t%d..%d\n", status.RetainedOffsetRange.Min, status.RetainedOffsetRange.Max); err != nil {
			return err
		}
	}
	if status.BuildStartedAt != nil {
		if _, err := fmt.Fprintf(meta, "build_started_at:\t%s\n", status.BuildStartedAt.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if status.BuildCompletedAt != nil {
		if _, err := fmt.Fprintf(meta, "build_completed_at:\t%s\n", status.BuildCompletedAt.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if err := meta.Flush(); err != nil {
		return err
	}

	if len(status.RowCounts) == 0 {
		return nil
	}
	rows := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(target, "\nTABLES"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(rows, "NAME\tROWS"); err != nil {
		return err
	}
	for _, name := range []string{"envelope_facts", "event_facts", "session_rollups", "command_rollups"} {
		if _, err := fmt.Fprintf(rows, "%s\t%d\n", name, status.RowCounts[name]); err != nil {
			return err
		}
	}
	return rows.Flush()
}

func writeAnalyticsExport(target io.Writer, manifest analytics.Manifest, status analytics.Status) error {
	if err := writeAnalyticsStatus(target, status); err != nil {
		return err
	}
	table := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(target, "\nSNAPSHOT"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(table, "TABLE\tROWS\tFILE\tSHA256"); err != nil {
		return err
	}
	for _, item := range manifest.Tables {
		if _, err := fmt.Fprintf(table, "%s\t%d\t%s\t%s\n", item.Name, item.Rows, item.File, item.SHA256); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeQueryTable(target io.Writer, columns []string, rows [][]any) error {
	if len(columns) == 0 {
		_, err := fmt.Fprintln(target, "no rows")
		return err
	}
	table := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	for idx, column := range columns {
		if idx > 0 {
			if _, err := fmt.Fprint(table, "\t"); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(table, column); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(table); err != nil {
		return err
	}
	for _, row := range rows {
		for idx, value := range row {
			if idx > 0 {
				if _, err := fmt.Fprint(table, "\t"); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(table, formatQueryValue(value)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(table); err != nil {
			return err
		}
	}
	return table.Flush()
}

func rowsToMaps(columns []string, rows [][]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(columns))
		for idx, column := range columns {
			if idx >= len(row) {
				item[column] = nil
				continue
			}
			item[column] = normalizeQueryValue(row[idx])
		}
		out = append(out, item)
	}
	return out
}

func normalizeQueryValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func formatQueryValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case bool:
		return strconv.FormatBool(typed)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

var _ = errors.New

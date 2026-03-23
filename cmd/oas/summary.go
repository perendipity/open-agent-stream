package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type sessionSummary struct {
	SessionKey       string    `json:"session_key"`
	SourceType       string    `json:"source_type"`
	SourceInstanceID string    `json:"source_instance_id"`
	ProjectKey       string    `json:"project_key,omitempty"`
	ProjectLocator   string    `json:"project_locator,omitempty"`
	Events           int       `json:"events"`
	FailureCount     int       `json:"failure_count"`
	FirstTimestamp   time.Time `json:"first_timestamp"`
	LastTimestamp    time.Time `json:"last_timestamp"`
	DurationSeconds  float64   `json:"duration_seconds"`
	Kinds            []string  `json:"kinds"`
}

type summaryOptions struct {
	SortBy     string
	FailedOnly bool
	Limit      int
}

const (
	summarySortInput   = "input"
	summarySortRecent  = "recent"
	summarySortBiggest = "biggest"
	summarySortFailed  = "failed"
)

func summaryCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON to summarize directly from the ledger")
	inputPath := fs.String("input", "-", "path to exported JSONL, or - for stdin")
	sortBy := fs.String("sort", summarySortRecent, "session order: input, recent, biggest, failed")
	failedOnly := fs.Bool("failed", false, "show only sessions with failure signals (tool.failed or non-zero command exit)")
	limit := fs.Int("limit", 0, "maximum sessions to print after sorting/filtering (0 = all)")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas summary [flags]

Summarize canonical events by session for reviewer-friendly inspection.

Input sources:
  -input <path>    Read exported JSONL from a file
  -input -         Read exported JSONL from stdin (default)
  -config <path>   Summarize directly from the configured ledger

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "input", Placeholder: "<path|->"},
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "sort", Placeholder: "<input|recent|biggest|failed>"},
			usageFlag{Name: "failed"},
			usageFlag{Name: "limit", Placeholder: "<n>"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas export -config ./oas.json | oas summary",
			"oas summary -input ./exports/events.jsonl",
			"oas summary -input ./exports/events.jsonl -sort biggest -limit 10",
			"oas summary -input ./exports/events.jsonl -failed",
			"oas summary -config ./oas.json -json",
		)
	}
	_ = fs.Parse(args)

	reader, closer, err := openSummaryInput(ctx, *configPath, *inputPath)
	if err != nil {
		fatal(err)
	}
	if closer != nil {
		defer closer.Close()
	}

	summaries, err := summarizeSessions(reader)
	if err != nil {
		fatal(err)
	}
	summaries, err = applySummaryOptions(summaries, summaryOptions{
		SortBy:     *sortBy,
		FailedOnly: *failedOnly,
		Limit:      *limit,
	})
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, summaries); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeSessionSummaries(os.Stdout, summaries); err != nil {
		fatal(err)
	}
}

func openSummaryInput(ctx context.Context, configPath, inputPath string) (io.Reader, io.Closer, error) {
	if configPath != "" && inputPath != "-" {
		return nil, nil, fmt.Errorf("summary accepts either -config or -input, not both")
	}
	if configPath != "" {
		runtime := mustRuntime(configPath)
		reader, writer := io.Pipe()
		go func() {
			err := runtime.Export(ctx, writer)
			if closeErr := runtime.Close(ctx); err == nil && closeErr != nil {
				err = closeErr
			}
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			_ = writer.Close()
		}()
		return reader, reader, nil
	}
	if inputPath == "-" {
		return os.Stdin, nil, nil
	}
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, nil, err
	}
	return file, file, nil
}

func summarizeSessions(reader io.Reader) ([]sessionSummary, error) {
	decoder := json.NewDecoder(reader)
	summaries := map[string]*sessionSummary{}
	kindSets := map[string]map[string]struct{}{}
	order := make([]string, 0)

	for {
		var event schema.CanonicalEvent
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		summary, ok := summaries[event.SessionKey]
		if !ok {
			summary = &sessionSummary{
				SessionKey:       event.SessionKey,
				SourceType:       event.SourceType,
				SourceInstanceID: event.SourceInstanceID,
				ProjectKey:       event.Context.ProjectKey,
				ProjectLocator:   event.Context.ProjectLocator,
				FirstTimestamp:   event.Timestamp,
				LastTimestamp:    event.Timestamp,
			}
			summaries[event.SessionKey] = summary
			kindSets[event.SessionKey] = map[string]struct{}{}
			order = append(order, event.SessionKey)
		}

		summary.Events++
		if event.Timestamp.Before(summary.FirstTimestamp) {
			summary.FirstTimestamp = event.Timestamp
		}
		if event.Timestamp.After(summary.LastTimestamp) {
			summary.LastTimestamp = event.Timestamp
		}
		if summary.SourceType == "" {
			summary.SourceType = event.SourceType
		}
		if summary.SourceInstanceID == "" {
			summary.SourceInstanceID = event.SourceInstanceID
		}
		if summary.ProjectKey == "" {
			summary.ProjectKey = event.Context.ProjectKey
		}
		if summary.ProjectLocator == "" {
			summary.ProjectLocator = event.Context.ProjectLocator
		}
		if summarySessionFailed(event) {
			summary.FailureCount++
		}
		kindSets[event.SessionKey][event.Kind] = struct{}{}
	}

	out := make([]sessionSummary, 0, len(order))
	for _, key := range order {
		summary := summaries[key]
		for kind := range kindSets[key] {
			summary.Kinds = append(summary.Kinds, kind)
		}
		sort.Strings(summary.Kinds)
		summary.DurationSeconds = summary.LastTimestamp.Sub(summary.FirstTimestamp).Seconds()
		out = append(out, *summary)
	}
	return out, nil
}

func applySummaryOptions(summaries []sessionSummary, opts summaryOptions) ([]sessionSummary, error) {
	if opts.Limit < 0 {
		return nil, fmt.Errorf("-limit must be >= 0")
	}

	filtered := make([]sessionSummary, 0, len(summaries))
	for _, summary := range summaries {
		if opts.FailedOnly && summary.FailureCount == 0 {
			continue
		}
		filtered = append(filtered, summary)
	}

	sortBy := strings.ToLower(strings.TrimSpace(opts.SortBy))
	if sortBy == "" {
		sortBy = summarySortRecent
	}
	switch sortBy {
	case summarySortInput:
		// Keep the input order stable.
	case summarySortRecent:
		sort.SliceStable(filtered, func(i, j int) bool {
			if !filtered[i].LastTimestamp.Equal(filtered[j].LastTimestamp) {
				return filtered[i].LastTimestamp.After(filtered[j].LastTimestamp)
			}
			if filtered[i].Events != filtered[j].Events {
				return filtered[i].Events > filtered[j].Events
			}
			return filtered[i].FailureCount > filtered[j].FailureCount
		})
	case summarySortBiggest:
		sort.SliceStable(filtered, func(i, j int) bool {
			if filtered[i].Events != filtered[j].Events {
				return filtered[i].Events > filtered[j].Events
			}
			if filtered[i].DurationSeconds != filtered[j].DurationSeconds {
				return filtered[i].DurationSeconds > filtered[j].DurationSeconds
			}
			return filtered[i].LastTimestamp.After(filtered[j].LastTimestamp)
		})
	case summarySortFailed:
		sort.SliceStable(filtered, func(i, j int) bool {
			if filtered[i].FailureCount != filtered[j].FailureCount {
				return filtered[i].FailureCount > filtered[j].FailureCount
			}
			if !filtered[i].LastTimestamp.Equal(filtered[j].LastTimestamp) {
				return filtered[i].LastTimestamp.After(filtered[j].LastTimestamp)
			}
			return filtered[i].Events > filtered[j].Events
		})
	default:
		return nil, fmt.Errorf("invalid -sort %q (want input, recent, biggest, or failed)", opts.SortBy)
	}

	if opts.Limit > 0 && opts.Limit < len(filtered) {
		filtered = filtered[:opts.Limit]
	}
	return filtered, nil
}

func summarySessionFailed(event schema.CanonicalEvent) bool {
	if event.Kind == "tool.failed" {
		return true
	}
	if event.Kind != "command.finished" {
		return false
	}
	exitCode, ok := payloadInt(event.Payload, "exit_code")
	return ok && exitCode != 0
}

func writeSessionSummaries(target io.Writer, summaries []sessionSummary) error {
	if len(summaries) == 0 {
		_, err := fmt.Fprintln(target, "no events")
		return err
	}

	writer := tabwriter.NewWriter(target, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "SESSION_KEY\tSOURCE\tINSTANCE\tEVENTS\tFAILS\tLAST\tDURATION\tPROJECT\tKINDS"); err != nil {
		return err
	}
	for _, summary := range summaries {
		if _, err := fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			summary.SessionKey,
			summary.SourceType,
			summary.SourceInstanceID,
			summary.Events,
			summary.FailureCount,
			summary.LastTimestamp.UTC().Format(time.RFC3339),
			summary.LastTimestamp.Sub(summary.FirstTimestamp).Round(time.Second),
			firstNonEmpty(summary.ProjectLocator, summary.ProjectKey),
			strings.Join(summary.Kinds, ","),
		); err != nil {
			return err
		}
	}
	return writer.Flush()
}

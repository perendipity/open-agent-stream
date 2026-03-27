package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/state"
)

type deliveryBatchView struct {
	BatchID         string         `json:"batch_id"`
	SinkID          string         `json:"sink_id"`
	Status          string         `json:"status"`
	AttemptCount    int            `json:"attempt_count"`
	NextAttemptAt   string         `json:"next_attempt_at,omitempty"`
	LastError       string         `json:"last_error,omitempty"`
	LedgerMinOffset int64          `json:"ledger_min_offset"`
	LedgerMaxOffset int64          `json:"ledger_max_offset"`
	EventCount      int            `json:"event_count"`
	PayloadBytes    int            `json:"payload_bytes"`
	CreatedAt       string         `json:"created_at,omitempty"`
	UpdatedAt       string         `json:"updated_at,omitempty"`
	Prepared        preparedView   `json:"prepared"`
	Destination     map[string]any `json:"destination,omitempty"`
}

type preparedView struct {
	PayloadSHA256 string            `json:"payload_sha256,omitempty"`
	ContentType   string            `json:"content_type,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	PayloadFormat string            `json:"payload_format,omitempty"`
}

func deliveryCommand(args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		deliveryUsage()
		if len(args) == 0 {
			os.Exit(2)
		}
		return
	}
	switch args[0] {
	case "status":
		deliveryStatusCommand(args[1:])
	case "list":
		deliveryListCommand(args[1:])
	case "inspect":
		deliveryInspectCommand(args[1:])
	case "retry":
		deliveryRetryCommand(args[1:])
	default:
		deliveryUsage()
		os.Exit(2)
	}
}

func deliveryStatusCommand(args []string) {
	fs := flag.NewFlagSet("delivery status", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas delivery status -config <path> [flags]

	Show per-sink delivery queue, retry, blocked, and quarantine state without requiring daemon mode.

`)
		printFlagSection(os.Stderr, fs, "Common flags", usageFlag{Name: "config", Placeholder: "<path>"})
		printFlagSection(os.Stderr, fs, "Advanced flags", usageFlag{Name: "json"})
		printExamples(os.Stderr,
			"oas delivery status -config ./examples/config.example.json",
			"oas delivery status -config ./examples/config.example.json -json",
		)
	}
	_ = fs.Parse(args)

	cfg := mustConfig(*configPath)
	statuses, err := daemonDeliveryStatusFor(cfg)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, statuses); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeDeliveryStatusTable(os.Stdout, statuses); err != nil {
		fatal(err)
	}
}

func deliveryListCommand(args []string) {
	fs := flag.NewFlagSet("delivery list", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	sinkID := fs.String("sink", "", "restrict results to one sink id")
	statusesFlag := fs.String("status", "", "comma-separated statuses: pending,retrying,blocked,quarantined")
	limit := fs.Int("limit", 50, "maximum number of batches to return")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas delivery list -config <path> [flags]

List sealed delivery batches that are pending, retrying, blocked, or quarantined.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "sink", Placeholder: "<id>"},
			usageFlag{Name: "status", Placeholder: "<pending,retrying,blocked,quarantined>"},
			usageFlag{Name: "limit", Placeholder: "<n>"},
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas delivery list -config ./examples/config.example.json",
			"oas delivery list -config ./examples/config.example.json -status quarantined",
			"oas delivery list -config ./examples/config.example.json -sink remote-http -json",
		)
	}
	_ = fs.Parse(args)

	cfg := mustConfig(*configPath)
	store := mustStateStore(cfg.StatePath)
	defer closeStateStore(store)

	statuses, err := parseDeliveryStatuses(*statusesFlag)
	if err != nil {
		fatal(err)
	}
	batches, err := store.ListDeliveryBatches(strings.TrimSpace(*sinkID), statuses, *limit)
	if err != nil {
		fatal(err)
	}
	views, err := buildDeliveryBatchViews(batches)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, views); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeDeliveryBatchTable(os.Stdout, views); err != nil {
		fatal(err)
	}
}

func deliveryInspectCommand(args []string) {
	fs := flag.NewFlagSet("delivery inspect", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	batchID := fs.String("batch", "", "delivery batch id")
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a readable report")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas delivery inspect -config <path> -batch <id> [flags]

Inspect one sealed delivery batch, including its immutable metadata and current error state.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
			usageFlag{Name: "batch", Placeholder: "<id>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags", usageFlag{Name: "json"})
		printExamples(os.Stderr,
			"oas delivery inspect -config ./examples/config.example.json -batch batch_123",
			"oas delivery inspect -config ./examples/config.example.json -batch batch_123 -json",
		)
	}
	_ = fs.Parse(args)
	if strings.TrimSpace(*batchID) == "" {
		fatal(errors.New("delivery inspect requires -batch <id>"))
	}

	cfg := mustConfig(*configPath)
	store := mustStateStore(cfg.StatePath)
	defer closeStateStore(store)

	batch, err := store.GetDeliveryBatch(*batchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fatal(fmt.Errorf("delivery batch %s not found", *batchID))
		}
		fatal(err)
	}
	view, err := buildDeliveryBatchView(batch)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := writeJSON(os.Stdout, view); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeDeliveryInspectReport(os.Stdout, view); err != nil {
		fatal(err)
	}
}

func deliveryRetryCommand(args []string) {
	fs := flag.NewFlagSet("delivery retry", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	batchID := fs.String("batch", "", "quarantined delivery batch id")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas delivery retry -config <path> -batch <id>

Requeue one quarantined delivery batch without changing its sealed payload or metadata.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
			usageFlag{Name: "batch", Placeholder: "<id>"},
		)
		printExamples(os.Stderr,
			"oas delivery retry -config ./examples/config.example.json -batch batch_123",
		)
	}
	_ = fs.Parse(args)
	if strings.TrimSpace(*batchID) == "" {
		fatal(errors.New("delivery retry requires -batch <id>"))
	}

	cfg := mustConfig(*configPath)
	store := mustStateStore(cfg.StatePath)
	defer closeStateStore(store)

	if err := store.RetryDeliveryBatch(*batchID, time.Now().UTC()); err != nil {
		fatal(err)
	}
	fmt.Printf("Requeued delivery batch %s\n", *batchID)
}

func deliveryUsage() {
	writeDeliveryUsage(os.Stderr)
}

func writeDeliveryUsage(writer io.Writer) {
	fmt.Fprintf(writer, `usage: oas delivery <subcommand> -config <path>

Subcommands:
  status    Show per-sink delivery queue, blocked state, and watermarks
  list      List pending, retrying, blocked, or quarantined delivery batches
  inspect   Inspect one sealed delivery batch
  retry     Requeue one quarantined delivery batch

Examples:
  oas delivery status -config ./examples/config.example.json
  oas delivery list -config ./examples/config.example.json -status quarantined
  oas delivery inspect -config ./examples/config.example.json -batch batch_123
  oas delivery retry -config ./examples/config.example.json -batch batch_123

Use:
  oas delivery <subcommand> --help
`)
}

func mustConfig(configPath string) config.Config {
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(err)
	}
	return cfg
}

func mustStateStore(path string) *state.Store {
	store, err := state.Open(path)
	if err != nil {
		fatal(err)
	}
	return store
}

func closeStateStore(store *state.Store) {
	if err := store.Close(); err != nil {
		fatal(err)
	}
}

func parseDeliveryStatuses(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	values := strings.Split(raw, ",")
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		switch value {
		case state.DeliveryBatchStatusPending, state.DeliveryBatchStatusRetrying, state.DeliveryBatchStatusBlocked, state.DeliveryBatchStatusQuarantined:
		default:
			return nil, fmt.Errorf("unsupported delivery status %q", value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func buildDeliveryBatchViews(batches []state.DeliveryBatch) ([]deliveryBatchView, error) {
	views := make([]deliveryBatchView, 0, len(batches))
	for _, batch := range batches {
		view, err := buildDeliveryBatchView(batch)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

func buildDeliveryBatchView(batch state.DeliveryBatch) (deliveryBatchView, error) {
	var prepared delivery.PreparedDispatch
	if err := json.Unmarshal(batch.PreparedJSON, &prepared); err != nil {
		return deliveryBatchView{}, err
	}
	view := deliveryBatchView{
		BatchID:         batch.BatchID,
		SinkID:          batch.SinkID,
		Status:          batch.Status,
		AttemptCount:    batch.AttemptCount,
		LastError:       batch.LastError,
		LedgerMinOffset: batch.LedgerMinOffset,
		LedgerMaxOffset: batch.LedgerMaxOffset,
		EventCount:      batch.EventCount,
		PayloadBytes:    batch.PayloadBytes,
		Prepared: preparedView{
			PayloadSHA256: prepared.PayloadSHA256,
			ContentType:   prepared.ContentType,
			Headers:       prepared.Headers,
			PayloadFormat: prepared.PayloadFormat,
		},
		Destination: prepared.Destination,
	}
	if !batch.NextAttemptAt.IsZero() {
		view.NextAttemptAt = batch.NextAttemptAt.Format(time.RFC3339Nano)
	}
	if !batch.CreatedAt.IsZero() {
		view.CreatedAt = batch.CreatedAt.Format(time.RFC3339Nano)
	}
	if !batch.UpdatedAt.IsZero() {
		view.UpdatedAt = batch.UpdatedAt.Format(time.RFC3339Nano)
	}
	return view, nil
}

func writeDeliveryStatusTable(writer io.Writer, statuses []daemonDeliveryStatus) error {
	table := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "SINK\tQUEUE\tREADY\tRETRYING\tQUARANTINED\tACKED\tTERMINAL\tGAPS\tLAST ERROR"); err != nil {
		return err
	}
	for _, status := range statuses {
		if _, err := fmt.Fprintf(table, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			status.SinkID,
			status.QueueDepth,
			status.ReadyBatchCount,
			status.RetryingBatchCount,
			status.QuarantinedBatchCount,
			status.AckedContiguousOffset,
			status.TerminalContiguousOffset,
			status.GapCount,
			emptyConfigValue(status.LastTerminalError),
		); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeDeliveryBatchTable(writer io.Writer, views []deliveryBatchView) error {
	table := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "BATCH\tSINK\tSTATUS\tATTEMPTS\tLEDGER\tEVENTS\tBYTES\tNEXT ATTEMPT\tLAST ERROR"); err != nil {
		return err
	}
	for _, view := range views {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%d-%d\t%d\t%d\t%s\t%s\n",
			view.BatchID,
			view.SinkID,
			view.Status,
			view.AttemptCount,
			view.LedgerMinOffset,
			view.LedgerMaxOffset,
			view.EventCount,
			view.PayloadBytes,
			emptyConfigValue(view.NextAttemptAt),
			emptyConfigValue(view.LastError),
		); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeDeliveryInspectReport(writer io.Writer, view deliveryBatchView) error {
	var b strings.Builder
	appendKeyValueSection(&b, "Batch",
		[2]string{"Batch ID", view.BatchID},
		[2]string{"Sink ID", view.SinkID},
		[2]string{"Status", view.Status},
		[2]string{"Attempts", fmt.Sprintf("%d", view.AttemptCount)},
		[2]string{"Ledger range", fmt.Sprintf("%d-%d", view.LedgerMinOffset, view.LedgerMaxOffset)},
		[2]string{"Event count", fmt.Sprintf("%d", view.EventCount)},
		[2]string{"Payload bytes", fmt.Sprintf("%d", view.PayloadBytes)},
		[2]string{"Next attempt", view.NextAttemptAt},
		[2]string{"Created", view.CreatedAt},
		[2]string{"Updated", view.UpdatedAt},
		[2]string{"Last error", view.LastError},
	)
	appendKeyValueSection(&b, "Prepared payload",
		[2]string{"Format", view.Prepared.PayloadFormat},
		[2]string{"Content type", view.Prepared.ContentType},
		[2]string{"Payload sha256", view.Prepared.PayloadSHA256},
	)
	if len(view.Prepared.Headers) > 0 {
		b.WriteString("Headers:\n")
		keys := make([]string, 0, len(view.Prepared.Headers))
		for key := range view.Prepared.Headers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(&b, "  %s: %s\n", key, view.Prepared.Headers[key])
		}
		b.WriteString("\n")
	}
	if len(view.Destination) > 0 {
		b.WriteString("Destination:\n")
		keys := make([]string, 0, len(view.Destination))
		for key := range view.Destination {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(&b, "  %s: %v\n", key, view.Destination[key])
		}
		b.WriteString("\n")
	}
	_, err := io.WriteString(writer, strings.TrimRight(b.String(), "\n")+"\n")
	return err
}

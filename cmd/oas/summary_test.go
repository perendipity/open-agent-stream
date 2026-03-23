package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestSummarizeSessionsGroupsEventsBySession(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{"project_key":"proj_1","project_locator":"/tmp/project-a"},"payload":{},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:05:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{"project_key":"proj_1","project_locator":"/tmp/project-a"},"payload":{"command":"go test ./...","exit_code":1},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"claude_local","source_instance_id":"claude-b","session_key":"sess_b","sequence":1,"timestamp":"2026-03-22T11:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{},"payload":{},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
`))

	summaries, err := summarizeSessions(input)
	if err != nil {
		t.Fatalf("summarizeSessions error: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("len(summaries) = %d, want 2", len(summaries))
	}

	first := summaries[0]
	if first.SessionKey != "sess_a" {
		t.Fatalf("first.SessionKey = %q, want sess_a", first.SessionKey)
	}
	if first.Events != 2 {
		t.Fatalf("first.Events = %d, want 2", first.Events)
	}
	if got, want := first.FirstTimestamp.UTC().Format(time.RFC3339), "2026-03-22T10:00:00Z"; got != want {
		t.Fatalf("first.FirstTimestamp = %q, want %q", got, want)
	}
	if got, want := first.LastTimestamp.UTC().Format(time.RFC3339), "2026-03-22T10:05:00Z"; got != want {
		t.Fatalf("first.LastTimestamp = %q, want %q", got, want)
	}
	if got, want := strings.Join(first.Kinds, ","), "command.finished,session.started"; got != want {
		t.Fatalf("first.Kinds = %q, want %q", got, want)
	}
	if got, want := first.ProjectKey, "proj_1"; got != want {
		t.Fatalf("first.ProjectKey = %q, want %q", got, want)
	}
	if got, want := first.ProjectLocator, "/tmp/project-a"; got != want {
		t.Fatalf("first.ProjectLocator = %q, want %q", got, want)
	}
	if got, want := first.FailureCount, 1; got != want {
		t.Fatalf("first.FailureCount = %d, want %d", got, want)
	}
	if got, want := first.DurationSeconds, 300.0; got != want {
		t.Fatalf("first.DurationSeconds = %v, want %v", got, want)
	}
}

func TestApplySummaryOptionsSortsAndFilters(t *testing.T) {
	summaries := []sessionSummary{
		{
			SessionKey:      "oldest",
			Events:          20,
			FailureCount:    0,
			FirstTimestamp:  time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			LastTimestamp:   time.Date(2026, 3, 22, 8, 30, 0, 0, time.UTC),
			DurationSeconds: 1800,
		},
		{
			SessionKey:      "biggest",
			Events:          50,
			FailureCount:    0,
			FirstTimestamp:  time.Date(2026, 3, 22, 9, 0, 0, 0, time.UTC),
			LastTimestamp:   time.Date(2026, 3, 22, 9, 10, 0, 0, time.UTC),
			DurationSeconds: 600,
		},
		{
			SessionKey:      "failed",
			Events:          5,
			FailureCount:    2,
			FirstTimestamp:  time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC),
			LastTimestamp:   time.Date(2026, 3, 22, 10, 5, 0, 0, time.UTC),
			DurationSeconds: 300,
		},
	}

	recent, err := applySummaryOptions(summaries, summaryOptions{SortBy: summarySortRecent})
	if err != nil {
		t.Fatalf("recent applySummaryOptions error: %v", err)
	}
	if got, want := recent[0].SessionKey, "failed"; got != want {
		t.Fatalf("recent[0].SessionKey = %q, want %q", got, want)
	}

	biggest, err := applySummaryOptions(summaries, summaryOptions{SortBy: summarySortBiggest})
	if err != nil {
		t.Fatalf("biggest applySummaryOptions error: %v", err)
	}
	if got, want := biggest[0].SessionKey, "biggest"; got != want {
		t.Fatalf("biggest[0].SessionKey = %q, want %q", got, want)
	}

	failedOnly, err := applySummaryOptions(summaries, summaryOptions{
		SortBy:     summarySortRecent,
		FailedOnly: true,
	})
	if err != nil {
		t.Fatalf("failed-only applySummaryOptions error: %v", err)
	}
	if len(failedOnly) != 1 {
		t.Fatalf("len(failedOnly) = %d, want 1", len(failedOnly))
	}
	if got, want := failedOnly[0].SessionKey, "failed"; got != want {
		t.Fatalf("failedOnly[0].SessionKey = %q, want %q", got, want)
	}

	limited, err := applySummaryOptions(summaries, summaryOptions{
		SortBy: summarySortRecent,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("limited applySummaryOptions error: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("len(limited) = %d, want 2", len(limited))
	}
}

func TestApplySummaryOptionsRejectsInvalidSort(t *testing.T) {
	if _, err := applySummaryOptions(nil, summaryOptions{SortBy: "weird"}); err == nil {
		t.Fatal("applySummaryOptions expected error for invalid sort")
	}
}

func TestWriteSessionSummariesProducesTable(t *testing.T) {
	summaries := []sessionSummary{
		{
			SessionKey:       "sess_a",
			SourceType:       "codex_local",
			SourceInstanceID: "codex-a",
			ProjectLocator:   "/tmp/project-a",
			Events:           2,
			FailureCount:     1,
			FirstTimestamp:   time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC),
			LastTimestamp:    time.Date(2026, 3, 22, 10, 5, 0, 0, time.UTC),
			Kinds:            []string{"command.finished", "session.started"},
		},
	}

	var out bytes.Buffer
	if err := writeSessionSummaries(&out, summaries); err != nil {
		t.Fatalf("writeSessionSummaries error: %v", err)
	}

	text := out.String()
	for _, want := range []string{
		"SESSION_KEY",
		"sess_a",
		"codex_local",
		"FAILS",
		"2026-03-22T10:05:00Z",
		"5m0s",
		"/tmp/project-a",
		"command.finished,session.started",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("table output missing %q:\n%s", want, text)
		}
	}
}

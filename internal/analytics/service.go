package analytics

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/ledger"
	"github.com/open-agent-stream/open-agent-stream/internal/normalize"
)

type Service struct {
	cfg    config.Config
	ledger *ledger.Store
}

type rebuildRequiredError struct {
	reason string
}

func (e rebuildRequiredError) Error() string {
	if strings.TrimSpace(e.reason) == "" {
		return "analytics cache requires a rebuild; rerun with -rebuild"
	}
	return fmt.Sprintf("analytics cache requires a rebuild: %s (rerun with -rebuild)", e.reason)
}

type analyticsMeta struct {
	DatasetVersion    string
	DerivationVersion string
	EngineVersion     string
	MachineID         string
	MachineIDOrigin   string
	LedgerInstanceID  string
	BuildState        string
	LastCleanOffset   int64
	BuildStartedAt    *time.Time
	BuildCompletedAt  *time.Time
	CacheCoveredMin   *int64
	CacheCoveredMax   *int64
	RetainedMinSeen   *int64
	RetainedMaxSeen   *int64
	CoverageMode      string
}

func New(cfg config.Config, ledgerStore *ledger.Store) *Service {
	return &Service{cfg: cfg, ledger: ledgerStore}
}

func DefaultPath(cfg config.Config) string {
	if strings.TrimSpace(cfg.StatePath) != "" {
		return filepath.Join(filepath.Dir(cfg.StatePath), "analytics", "analytics.duckdb")
	}
	return filepath.Join(cfg.DataDir, "analytics", "analytics.duckdb")
}

func (s *Service) Build(ctx context.Context, opts BuildOptions) (Status, error) {
	if s == nil || s.ledger == nil {
		return Status{}, errors.New("analytics service requires a ledger")
	}
	if err := requireSingleString(s.cfg.MachineID, "config.machine_id"); err != nil {
		return Status{}, fmt.Errorf("analytics requires a non-empty machine_id: %w", err)
	}

	path := s.dbPath(opts.Path)
	existed := pathExists(path)
	db, err := openDuckDB(path)
	if err != nil {
		return Status{}, err
	}
	defer db.Close()

	if err := ensureSchema(ctx, db); err != nil {
		return Status{}, err
	}
	meta, hasMeta, err := loadMeta(ctx, db)
	if err != nil {
		return Status{}, err
	}
	if existed && !hasMeta && !opts.Rebuild {
		return Status{}, rebuildRequiredError{reason: "analytics cache metadata is missing"}
	}

	ledgerMeta := s.ledger.Metadata()
	retainedBounds, err := s.ledger.Bounds()
	if err != nil {
		return Status{}, err
	}

	reset := opts.Rebuild
	if hasMeta && !reset {
		if err := s.validateAppendCompatibility(meta, ledgerMeta); err != nil {
			return Status{}, err
		}
	}

	machineID, machineOrigin, err := s.resolveMachineBinding(ctx, hasMeta && !reset, meta)
	if err != nil {
		return Status{}, err
	}
	if hasMeta && !reset && meta.MachineIDOrigin == string(MachineIDOriginConfigBackfill) && machineOrigin != MachineIDOriginConfigBackfill {
		return Status{}, rebuildRequiredError{reason: "cache was backfilled from config but retained ledger now contains captured machine ids"}
	}

	startedAt := time.Now().UTC()
	buildingMeta := meta
	if !hasMeta || reset {
		buildingMeta = analyticsMeta{
			DatasetVersion:    DatasetVersion,
			DerivationVersion: DerivationVersion,
			EngineVersion:     engineVersion(),
			MachineID:         machineID,
			MachineIDOrigin:   string(machineOrigin),
			LedgerInstanceID:  ledgerMeta.InstanceID,
			BuildState:        string(BuildStateBuilding),
		}
	} else {
		buildingMeta.DatasetVersion = DatasetVersion
		buildingMeta.DerivationVersion = DerivationVersion
		buildingMeta.EngineVersion = engineVersion()
		buildingMeta.MachineID = machineID
		buildingMeta.MachineIDOrigin = string(machineOrigin)
		buildingMeta.LedgerInstanceID = ledgerMeta.InstanceID
		buildingMeta.BuildState = string(BuildStateBuilding)
	}
	buildingMeta.BuildStartedAt = &startedAt
	if err := replaceMeta(ctx, db, buildingMeta); err != nil {
		return Status{}, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Status{}, err
	}
	defer tx.Rollback()

	if reset {
		if err := clearAnalyticsData(ctx, tx); err != nil {
			return Status{}, err
		}
		hasMeta = false
		meta = analyticsMeta{}
	}

	sequences, err := loadSequenceStore(tx)
	if err != nil {
		return Status{}, err
	}
	normalizer := normalize.NewService(sequences)
	if !reset {
		if err := seedNormalizerCallNames(tx, normalizer); err != nil {
			return Status{}, err
		}
	}

	envelopeStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO analytics_envelope_facts (
			global_envelope_key, machine_id, ledger_instance_id, ledger_offset, envelope_id,
			source_type, source_instance_id, artifact_id, artifact_locator, project_locator,
			project_key, source_session_key, global_session_key, cursor_kind, cursor_value,
			observed_at, source_timestamp, raw_kind, content_hash, payload_bytes,
			capabilities_csv, parse_confidence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return Status{}, err
	}
	defer envelopeStmt.Close()

	eventStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO analytics_event_facts (
			global_event_key, global_envelope_key, machine_id, ledger_instance_id, ledger_offset,
			event_id, event_version, source_type, source_instance_id, session_key,
			source_session_key, global_session_key, project_key, project_locator, sequence,
			timestamp, kind, actor_kind, actor_name, parse_status,
			capabilities_csv, tool_name, call_id, exit_code, duration_ms, envelope_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return Status{}, err
	}
	defer eventStmt.Close()

	lastCleanOffset := int64(0)
	if hasMeta {
		lastCleanOffset = meta.LastCleanOffset
	}
	cacheCoveredMin := meta.CacheCoveredMin
	cacheCoveredMax := meta.CacheCoveredMax
	current := lastCleanOffset

	for {
		records, err := s.ledger.ListAfter(current, s.cfg.BatchSize)
		if err != nil {
			return Status{}, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			select {
			case <-ctx.Done():
				return Status{}, ctx.Err()
			default:
			}

			if _, err := effectiveEnvelopeMachineID(record.Envelope.MachineID, machineID, machineOrigin); err != nil {
				return Status{}, err
			}
			event, err := normalizer.Normalize(record)
			if err != nil {
				return Status{}, fmt.Errorf("normalize ledger offset %d: %w", record.Offset, err)
			}
			envelopeRow := buildEnvelopeFact(record, event, machineID, ledgerMeta.InstanceID)
			if _, err := envelopeStmt.ExecContext(ctx, envelopeRow.args()...); err != nil {
				return Status{}, err
			}
			eventRow := buildEventFact(record, event, machineID, ledgerMeta.InstanceID)
			if _, err := eventStmt.ExecContext(ctx, eventRow.args()...); err != nil {
				return Status{}, err
			}
			lastCleanOffset = record.Offset
			cacheCoveredMin, cacheCoveredMax = extendRange(cacheCoveredMin, cacheCoveredMax, record.Offset)
			current = record.Offset
		}
	}

	commands, err := deriveCommandRollups(tx)
	if err != nil {
		return Status{}, err
	}
	if err := replaceCommandRollups(ctx, tx, commands); err != nil {
		return Status{}, err
	}

	sessions, err := deriveSessionRollups(tx, commands)
	if err != nil {
		return Status{}, err
	}
	if err := replaceSessionRollups(ctx, tx, sessions); err != nil {
		return Status{}, err
	}

	retainedMin, retainedMax := boundsPointers(retainedBounds)
	completedAt := time.Now().UTC()
	finalMeta := analyticsMeta{
		DatasetVersion:    DatasetVersion,
		DerivationVersion: DerivationVersion,
		EngineVersion:     engineVersion(),
		MachineID:         machineID,
		MachineIDOrigin:   string(machineOrigin),
		LedgerInstanceID:  ledgerMeta.InstanceID,
		BuildState:        string(BuildStateReady),
		LastCleanOffset:   lastCleanOffset,
		BuildStartedAt:    &startedAt,
		BuildCompletedAt:  &completedAt,
		CacheCoveredMin:   cacheCoveredMin,
		CacheCoveredMax:   cacheCoveredMax,
		RetainedMinSeen:   retainedMin,
		RetainedMaxSeen:   retainedMax,
		CoverageMode:      string(computeCoverageMode(cacheCoveredMin, retainedMin)),
	}
	if err := replaceMetaTx(ctx, tx, finalMeta); err != nil {
		return Status{}, err
	}
	if err := tx.Commit(); err != nil {
		return Status{}, err
	}
	return s.Status(ctx, path)
}

func (s *Service) Query(ctx context.Context, opts QueryOptions) ([]string, [][]any, Status, error) {
	status, err := s.Build(ctx, BuildOptions{Path: opts.Path, Rebuild: opts.Rebuild})
	if err != nil {
		return nil, nil, Status{}, err
	}
	db, err := openDuckDB(status.Path)
	if err != nil {
		return nil, nil, Status{}, err
	}
	defer db.Close()
	if strings.TrimSpace(opts.SQL) != "" {
		if opts.Sensitive {
			return nil, nil, Status{}, errors.New("analytics query -sensitive cannot be combined with -sql")
		}
		sqlText := strings.TrimSpace(opts.SQL)
		if err := validateReadOnlyQuery(sqlText); err != nil {
			return nil, nil, Status{}, err
		}
		columns, rows, err := runQuery(ctx, db, sqlText)
		return columns, rows, status, err
	}

	spec, err := resolvePreset(opts.Preset)
	if err != nil {
		return nil, nil, Status{}, err
	}
	limit, err := effectivePresetLimit(opts.Limit, spec.DefaultLimit)
	if err != nil {
		return nil, nil, Status{}, err
	}
	result, err := spec.Run(ctx, db, status, limit, time.Now().UTC())
	if err != nil {
		return nil, nil, Status{}, err
	}
	if opts.Sensitive {
		if !spec.SupportsSensitive {
			return nil, nil, Status{}, fmt.Errorf("analytics preset %q does not support -sensitive", spec.Name)
		}
		result, err = s.augmentSensitivePreset(ctx, spec.Name, result)
		if err != nil {
			return nil, nil, Status{}, err
		}
	}
	columns, rows := result.queryRows()
	return columns, rows, status, nil
}

func (s *Service) Export(ctx context.Context, opts ExportOptions) (Manifest, Status, error) {
	status, err := s.Build(ctx, BuildOptions{Path: opts.Path, Rebuild: opts.Rebuild})
	if err != nil {
		return Manifest{}, Status{}, err
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		return Manifest{}, Status{}, errors.New("analytics export requires -output <dir>")
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return Manifest{}, Status{}, err
	}

	db, err := openDuckDB(status.Path)
	if err != nil {
		return Manifest{}, Status{}, err
	}
	defer db.Close()

	meta, _, err := loadMeta(ctx, db)
	if err != nil {
		return Manifest{}, Status{}, err
	}

	type exportSpec struct {
		View       string
		File       string
		PrimaryKey []string
		OrderBy    string
	}
	specs := []exportSpec{
		{View: "envelope_facts", File: "envelope_facts.parquet", PrimaryKey: []string{"global_envelope_key"}, OrderBy: "global_envelope_key"},
		{View: "event_facts", File: "event_facts.parquet", PrimaryKey: []string{"global_event_key"}, OrderBy: "global_event_key"},
		{View: "session_rollups", File: "session_rollups.parquet", PrimaryKey: []string{"global_session_key"}, OrderBy: "global_session_key"},
		{View: "command_rollups", File: "command_rollups.parquet", PrimaryKey: []string{"global_command_rollup_key"}, OrderBy: "global_command_rollup_key"},
	}

	manifest := Manifest{
		SnapshotID:                  snapshotID(meta, time.Now().UTC()),
		DatasetVersion:              DatasetVersion,
		DerivationVersion:           DerivationVersion,
		EngineVersion:               meta.EngineVersion,
		MachineID:                   meta.MachineID,
		MachineIDOrigin:             meta.MachineIDOrigin,
		LedgerInstanceID:            meta.LedgerInstanceID,
		ExportedAt:                  time.Now().UTC(),
		CoverageMode:                meta.CoverageMode,
		CacheCoveredOffsetRange:     offsetRangeFromPointers(meta.CacheCoveredMin, meta.CacheCoveredMax),
		RetainedOffsetRangeAtExport: status.RetainedOffsetRange,
		Tables:                      make([]ManifestTable, 0, len(specs)),
	}

	for _, spec := range specs {
		targetPath := filepath.Join(opts.OutputDir, spec.File)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			`COPY (SELECT * FROM %s ORDER BY %s) TO '%s' (FORMAT PARQUET)`,
			spec.View,
			spec.OrderBy,
			escapeDuckDBString(targetPath),
		)); err != nil {
			return Manifest{}, Status{}, err
		}
		rows, err := countRows(ctx, db, spec.View)
		if err != nil {
			return Manifest{}, Status{}, err
		}
		hash, err := sha256File(targetPath)
		if err != nil {
			return Manifest{}, Status{}, err
		}
		manifest.Tables = append(manifest.Tables, ManifestTable{
			Name:       spec.View,
			Rows:       rows,
			File:       spec.File,
			SHA256:     hash,
			PrimaryKey: append([]string(nil), spec.PrimaryKey...),
		})
	}

	manifestPath := filepath.Join(opts.OutputDir, "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, Status{}, err
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		return Manifest{}, Status{}, err
	}
	return manifest, status, nil
}

func (s *Service) Status(ctx context.Context, path string) (Status, error) {
	path = s.dbPath(path)
	status := Status{
		Path: path,
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return status, nil
		}
		return Status{}, err
	}
	status.Exists = true
	status.SizeBytes = info.Size()

	retainedBounds, err := s.ledger.Bounds()
	if err != nil {
		return Status{}, err
	}
	status.RetainedOffsetRange = offsetRangeFromBounds(retainedBounds)

	db, err := openDuckDB(path)
	if err != nil {
		return Status{}, err
	}
	defer db.Close()
	if err := ensureSchema(ctx, db); err != nil {
		return Status{}, err
	}
	meta, hasMeta, err := loadMeta(ctx, db)
	if err != nil {
		return Status{}, err
	}
	if !hasMeta {
		return status, nil
	}
	status.DatasetVersion = meta.DatasetVersion
	status.DerivationVersion = meta.DerivationVersion
	status.EngineVersion = meta.EngineVersion
	status.MachineID = meta.MachineID
	status.MachineIDOrigin = meta.MachineIDOrigin
	status.LedgerInstanceID = meta.LedgerInstanceID
	status.BuildState = meta.BuildState
	status.LastCleanOffset = meta.LastCleanOffset
	status.BuildStartedAt = meta.BuildStartedAt
	status.BuildCompletedAt = meta.BuildCompletedAt
	status.CacheCoveredOffsetRange = offsetRangeFromPointers(meta.CacheCoveredMin, meta.CacheCoveredMax)
	status.CoverageMode = meta.CoverageMode

	status.RowCounts = map[string]int64{}
	for _, view := range []string{"envelope_facts", "event_facts", "session_rollups", "command_rollups"} {
		rows, err := countRows(ctx, db, view)
		if err != nil {
			return Status{}, err
		}
		status.RowCounts[view] = rows
	}
	return status, nil
}

func (s *Service) dbPath(path string) string {
	if strings.TrimSpace(path) != "" {
		return path
	}
	return DefaultPath(s.cfg)
}

func (s *Service) validateAppendCompatibility(meta analyticsMeta, ledgerMeta ledger.Metadata) error {
	switch {
	case meta.DatasetVersion != DatasetVersion:
		return rebuildRequiredError{reason: "dataset version changed"}
	case meta.DerivationVersion != DerivationVersion:
		return rebuildRequiredError{reason: "derivation version changed"}
	case meta.BuildState != string(BuildStateReady):
		return rebuildRequiredError{reason: "previous build did not complete cleanly"}
	case meta.MachineID != s.cfg.MachineID:
		return rebuildRequiredError{reason: "config.machine_id changed"}
	case meta.LedgerInstanceID != ledgerMeta.InstanceID:
		return rebuildRequiredError{reason: "ledger lineage changed"}
	default:
		return nil
	}
}

func (s *Service) resolveMachineBinding(ctx context.Context, incremental bool, meta analyticsMeta) (string, MachineIDOrigin, error) {
	if incremental {
		switch {
		case meta.MachineID == "":
			return "", "", rebuildRequiredError{reason: "cache machine binding is missing"}
		case meta.MachineID != s.cfg.MachineID:
			return "", "", rebuildRequiredError{reason: "config.machine_id changed"}
		}
		return meta.MachineID, MachineIDOrigin(meta.MachineIDOrigin), nil
	}

	presentID := ""
	anyPresent := false
	anyMissing := false
	current := int64(0)
	for {
		records, err := s.ledger.ListAfter(current, s.cfg.BatchSize)
		if err != nil {
			return "", "", err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			select {
			case <-ctx.Done():
				return "", "", ctx.Err()
			default:
			}
			current = record.Offset
			if strings.TrimSpace(record.Envelope.MachineID) == "" {
				anyMissing = true
				continue
			}
			anyPresent = true
			if presentID == "" {
				presentID = record.Envelope.MachineID
				continue
			}
			if presentID != record.Envelope.MachineID {
				return "", "", errors.New("analytics found conflicting machine_id values in the retained ledger history")
			}
		}
	}

	switch {
	case anyPresent && anyMissing:
		return "", "", errors.New("analytics refuses to backfill machine_id because the retained ledger mixes captured and missing machine ids")
	case anyPresent:
		if presentID != s.cfg.MachineID {
			return "", "", rebuildRequiredError{reason: "config.machine_id does not match captured ledger machine_id"}
		}
		return presentID, MachineIDOriginCaptured, nil
	default:
		return s.cfg.MachineID, MachineIDOriginConfigBackfill, nil
	}
}

func effectiveEnvelopeMachineID(envelopeMachineID, boundMachineID string, origin MachineIDOrigin) (string, error) {
	envelopeMachineID = strings.TrimSpace(envelopeMachineID)
	if envelopeMachineID == "" {
		if origin == MachineIDOriginConfigBackfill {
			return boundMachineID, nil
		}
		return "", errors.New("analytics encountered a retained envelope without machine_id after the cache was bound to captured machine ids")
	}
	if envelopeMachineID != boundMachineID {
		return "", errors.New("analytics encountered a machine_id mismatch between the retained envelope and the bound cache")
	}
	return boundMachineID, nil
}

func openDuckDB(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS analytics_meta (
			singleton BOOLEAN PRIMARY KEY,
			dataset_version VARCHAR NOT NULL,
			derivation_version VARCHAR NOT NULL,
			engine_version VARCHAR NOT NULL,
			machine_id VARCHAR,
			machine_id_origin VARCHAR,
			ledger_instance_id VARCHAR,
			build_state VARCHAR NOT NULL,
			last_clean_offset BIGINT NOT NULL,
			build_started_at TIMESTAMP,
			build_completed_at TIMESTAMP,
			cache_covered_min_offset BIGINT,
			cache_covered_max_offset BIGINT,
			retained_min_offset_seen BIGINT,
			retained_max_offset_seen BIGINT,
			coverage_mode VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS analytics_envelope_facts (
			global_envelope_key VARCHAR PRIMARY KEY,
			machine_id VARCHAR NOT NULL,
			ledger_instance_id VARCHAR NOT NULL,
			ledger_offset BIGINT NOT NULL,
			envelope_id VARCHAR NOT NULL,
			source_type VARCHAR NOT NULL,
			source_instance_id VARCHAR NOT NULL,
			artifact_id VARCHAR NOT NULL,
			artifact_locator VARCHAR,
			project_locator VARCHAR,
			project_key VARCHAR,
			source_session_key VARCHAR,
			global_session_key VARCHAR NOT NULL,
			cursor_kind VARCHAR NOT NULL,
			cursor_value VARCHAR NOT NULL,
			observed_at TIMESTAMP NOT NULL,
			source_timestamp TIMESTAMP,
			raw_kind VARCHAR NOT NULL,
			content_hash VARCHAR NOT NULL,
			payload_bytes BIGINT NOT NULL,
			capabilities_csv VARCHAR,
			parse_confidence DOUBLE
		)`,
		`CREATE TABLE IF NOT EXISTS analytics_event_facts (
			global_event_key VARCHAR PRIMARY KEY,
			global_envelope_key VARCHAR NOT NULL,
			machine_id VARCHAR NOT NULL,
			ledger_instance_id VARCHAR NOT NULL,
			ledger_offset BIGINT NOT NULL,
			event_id VARCHAR NOT NULL,
			event_version INTEGER NOT NULL,
			source_type VARCHAR NOT NULL,
			source_instance_id VARCHAR NOT NULL,
			session_key VARCHAR NOT NULL,
			source_session_key VARCHAR,
			global_session_key VARCHAR NOT NULL,
			project_key VARCHAR,
			project_locator VARCHAR,
			sequence BIGINT NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			kind VARCHAR NOT NULL,
			actor_kind VARCHAR,
			actor_name VARCHAR,
			parse_status VARCHAR,
			capabilities_csv VARCHAR,
			tool_name VARCHAR,
			call_id VARCHAR,
			exit_code BIGINT,
			duration_ms BIGINT,
			envelope_id VARCHAR NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS analytics_command_rollups (
			global_command_rollup_key VARCHAR PRIMARY KEY,
			derivation_version VARCHAR NOT NULL,
			machine_id VARCHAR NOT NULL,
			global_session_key VARCHAR NOT NULL,
			source_instance_id VARCHAR NOT NULL,
			source_session_key VARCHAR,
			status VARCHAR NOT NULL,
			tool_name VARCHAR,
			actor_name VARCHAR,
			call_id VARCHAR,
			start_global_event_key VARCHAR,
			finish_global_event_key VARCHAR,
			start_global_envelope_key VARCHAR,
			finish_global_envelope_key VARCHAR,
			start_sequence BIGINT,
			finish_sequence BIGINT,
			start_timestamp TIMESTAMP,
			finish_timestamp TIMESTAMP,
			exit_code BIGINT,
			duration_ms BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS analytics_session_rollups (
			global_session_key VARCHAR PRIMARY KEY,
			derivation_version VARCHAR NOT NULL,
			machine_id VARCHAR NOT NULL,
			source_type VARCHAR NOT NULL,
			source_instance_id VARCHAR NOT NULL,
			session_key VARCHAR NOT NULL,
			source_session_key VARCHAR,
			project_key VARCHAR,
			project_locator VARCHAR,
			first_timestamp TIMESTAMP NOT NULL,
			last_timestamp TIMESTAMP NOT NULL,
			duration_ms BIGINT NOT NULL,
			event_count BIGINT NOT NULL,
			user_message_count BIGINT NOT NULL,
			agent_message_count BIGINT NOT NULL,
			system_message_count BIGINT NOT NULL,
			command_event_count BIGINT NOT NULL,
			command_rollup_count BIGINT NOT NULL,
			failed_command_rollup_count BIGINT NOT NULL,
			incomplete_command_rollup_count BIGINT NOT NULL,
			tool_failure_count BIGINT NOT NULL,
			file_patch_count BIGINT NOT NULL,
			failure_signal_count BIGINT NOT NULL,
			parse_failure_count BIGINT NOT NULL
		)`,
		`CREATE OR REPLACE VIEW envelope_facts AS
			SELECT
				global_envelope_key,
				machine_id,
				ledger_instance_id,
				ledger_offset,
				envelope_id,
				source_type,
				source_instance_id,
				artifact_id,
				artifact_locator,
				project_locator,
				project_key,
				source_session_key,
				global_session_key,
				cursor_kind,
				cursor_value,
				observed_at,
				source_timestamp,
				raw_kind,
				content_hash,
				payload_bytes,
				capabilities_csv,
				parse_confidence
			FROM analytics_envelope_facts`,
		`CREATE OR REPLACE VIEW event_facts AS
			SELECT
				global_event_key,
				global_envelope_key,
				machine_id,
				ledger_instance_id,
				ledger_offset,
				event_id,
				event_version,
				source_type,
				source_instance_id,
				session_key,
				source_session_key,
				global_session_key,
				project_key,
				project_locator,
				sequence,
				timestamp,
				kind,
				actor_kind,
				actor_name,
				parse_status,
				capabilities_csv,
				tool_name,
				call_id,
				exit_code,
				duration_ms,
				envelope_id
			FROM analytics_event_facts`,
		`CREATE OR REPLACE VIEW command_rollups AS
			SELECT
				global_command_rollup_key,
				derivation_version,
				machine_id,
				global_session_key,
				source_instance_id,
				source_session_key,
				status,
				tool_name,
				actor_name,
				call_id,
				start_global_event_key,
				finish_global_event_key,
				start_global_envelope_key,
				finish_global_envelope_key,
				start_sequence,
				finish_sequence,
				start_timestamp,
				finish_timestamp,
				exit_code,
				duration_ms
			FROM analytics_command_rollups`,
		`CREATE OR REPLACE VIEW session_rollups AS
			SELECT
				global_session_key,
				derivation_version,
				machine_id,
				source_type,
				source_instance_id,
				session_key,
				source_session_key,
				project_key,
				project_locator,
				first_timestamp,
				last_timestamp,
				duration_ms,
				event_count,
				user_message_count,
				agent_message_count,
				system_message_count,
				command_event_count,
				command_rollup_count,
				failed_command_rollup_count,
				incomplete_command_rollup_count,
				tool_failure_count,
				file_patch_count,
				failure_signal_count,
				parse_failure_count
			FROM analytics_session_rollups`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func loadMeta(ctx context.Context, db *sql.DB) (analyticsMeta, bool, error) {
	row := db.QueryRowContext(ctx, `
		SELECT
			dataset_version,
			derivation_version,
			engine_version,
			machine_id,
			machine_id_origin,
			ledger_instance_id,
			build_state,
			last_clean_offset,
			build_started_at,
			build_completed_at,
			cache_covered_min_offset,
			cache_covered_max_offset,
			retained_min_offset_seen,
			retained_max_offset_seen,
			coverage_mode
		FROM analytics_meta
		WHERE singleton = TRUE
	`)

	var (
		meta           analyticsMeta
		machineID      sql.NullString
		machineOrigin  sql.NullString
		ledgerInstance sql.NullString
		startedAt      sql.NullTime
		completedAt    sql.NullTime
		cacheMin       sql.NullInt64
		cacheMax       sql.NullInt64
		retainedMin    sql.NullInt64
		retainedMax    sql.NullInt64
		coverageMode   sql.NullString
	)
	err := row.Scan(
		&meta.DatasetVersion,
		&meta.DerivationVersion,
		&meta.EngineVersion,
		&machineID,
		&machineOrigin,
		&ledgerInstance,
		&meta.BuildState,
		&meta.LastCleanOffset,
		&startedAt,
		&completedAt,
		&cacheMin,
		&cacheMax,
		&retainedMin,
		&retainedMax,
		&coverageMode,
	)
	switch err {
	case nil:
	case sql.ErrNoRows:
		return analyticsMeta{}, false, nil
	default:
		return analyticsMeta{}, false, err
	}
	meta.MachineID = nullString(machineID)
	meta.MachineIDOrigin = nullString(machineOrigin)
	meta.LedgerInstanceID = nullString(ledgerInstance)
	meta.BuildStartedAt = nullTime(startedAt)
	meta.BuildCompletedAt = nullTime(completedAt)
	meta.CacheCoveredMin = nullInt64(cacheMin)
	meta.CacheCoveredMax = nullInt64(cacheMax)
	meta.RetainedMinSeen = nullInt64(retainedMin)
	meta.RetainedMaxSeen = nullInt64(retainedMax)
	meta.CoverageMode = nullString(coverageMode)
	return meta, true, nil
}

func replaceMeta(ctx context.Context, db *sql.DB, meta analyticsMeta) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := replaceMetaTx(ctx, tx, meta); err != nil {
		return err
	}
	return tx.Commit()
}

func replaceMetaTx(ctx context.Context, tx *sql.Tx, meta analyticsMeta) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM analytics_meta`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO analytics_meta (
			singleton, dataset_version, derivation_version, engine_version, machine_id,
			machine_id_origin, ledger_instance_id, build_state, last_clean_offset,
			build_started_at, build_completed_at, cache_covered_min_offset, cache_covered_max_offset,
			retained_min_offset_seen, retained_max_offset_seen, coverage_mode
		) VALUES (TRUE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		meta.DatasetVersion,
		meta.DerivationVersion,
		meta.EngineVersion,
		nullableString(meta.MachineID),
		nullableString(meta.MachineIDOrigin),
		nullableString(meta.LedgerInstanceID),
		firstNonEmpty(meta.BuildState, string(BuildStateEmpty)),
		meta.LastCleanOffset,
		timeValue(meta.BuildStartedAt),
		timeValue(meta.BuildCompletedAt),
		int64Value(meta.CacheCoveredMin),
		int64Value(meta.CacheCoveredMax),
		int64Value(meta.RetainedMinSeen),
		int64Value(meta.RetainedMaxSeen),
		nullableString(meta.CoverageMode),
	)
	return err
}

func clearAnalyticsData(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		`DELETE FROM analytics_command_rollups`,
		`DELETE FROM analytics_session_rollups`,
		`DELETE FROM analytics_event_facts`,
		`DELETE FROM analytics_envelope_facts`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func replaceCommandRollups(ctx context.Context, tx *sql.Tx, rows []commandRollupRow) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM analytics_command_rollups`); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO analytics_command_rollups (
			global_command_rollup_key, derivation_version, machine_id, global_session_key, source_instance_id,
			source_session_key, status, tool_name, actor_name, call_id,
			start_global_event_key, finish_global_event_key, start_global_envelope_key, finish_global_envelope_key,
			start_sequence, finish_sequence, start_timestamp, finish_timestamp, exit_code, duration_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row.args()...); err != nil {
			return err
		}
	}
	return nil
}

func replaceSessionRollups(ctx context.Context, tx *sql.Tx, rows []sessionRollupRow) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM analytics_session_rollups`); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO analytics_session_rollups (
			global_session_key, derivation_version, machine_id, source_type, source_instance_id,
			session_key, source_session_key, project_key, project_locator, first_timestamp,
			last_timestamp, duration_ms, event_count, user_message_count, agent_message_count,
			system_message_count, command_event_count, command_rollup_count, failed_command_rollup_count, incomplete_command_rollup_count,
			tool_failure_count, file_patch_count, failure_signal_count, parse_failure_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row.args()...); err != nil {
			return err
		}
	}
	return nil
}

func runQuery(ctx context.Context, db *sql.DB, query string) ([]string, [][]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	results := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, nil, err
		}
		results = append(results, values)
	}
	return columns, results, rows.Err()
}

func validateReadOnlyQuery(query string) error {
	normalized, err := normalizeQueryForReadOnlyCheck(query)
	if err != nil {
		return fmt.Errorf("analytics query -sql must be a single read-only SELECT or WITH query: %w", err)
	}
	tokens := strings.Fields(normalized)
	if len(tokens) == 0 {
		return errors.New("analytics query -sql must be a single read-only SELECT or WITH query")
	}
	if tokens[len(tokens)-1] == ";" {
		tokens = tokens[:len(tokens)-1]
	}
	for _, token := range tokens {
		if token == ";" {
			return errors.New("analytics query -sql must be a single statement")
		}
	}
	if len(tokens) == 0 {
		return errors.New("analytics query -sql must be a single read-only SELECT or WITH query")
	}
	switch tokens[0] {
	case "select":
	case "with":
		if !containsSQLToken(tokens, "select") {
			return errors.New("analytics query -sql WITH statements must terminate in a SELECT")
		}
	default:
		return errors.New("analytics query -sql only accepts read-only SELECT or WITH queries")
	}
	for _, token := range tokens {
		switch token {
		case "insert", "update", "delete", "merge", "copy", "call", "create", "alter", "drop", "truncate", "attach", "detach", "vacuum", "export", "import", "install", "load", "set", "reset", "use", "pragma":
			return fmt.Errorf("analytics query -sql rejects writable keyword %q", token)
		}
	}
	return nil
}

func normalizeQueryForReadOnlyCheck(query string) (string, error) {
	var builder strings.Builder
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	for idx := 0; idx < len(query); idx++ {
		ch := query[idx]
		next := byte(0)
		if idx+1 < len(query) {
			next = query[idx+1]
		}
		switch {
		case inLineComment:
			if ch == '\n' {
				inLineComment = false
				builder.WriteByte(' ')
			}
		case inBlockComment:
			if ch == '*' && next == '/' {
				inBlockComment = false
				builder.WriteByte(' ')
				idx++
			}
		case inSingle:
			if ch == '\'' {
				if next == '\'' {
					idx++
					continue
				}
				inSingle = false
				builder.WriteByte(' ')
			}
		case inDouble:
			if ch == '"' {
				if next == '"' {
					idx++
					continue
				}
				inDouble = false
				builder.WriteByte(' ')
			}
		default:
			switch {
			case ch == '-' && next == '-':
				inLineComment = true
				idx++
			case ch == '/' && next == '*':
				inBlockComment = true
				idx++
			case ch == '\'':
				inSingle = true
				builder.WriteByte(' ')
			case ch == '"':
				inDouble = true
				builder.WriteByte(' ')
			case ch == ';':
				builder.WriteString(" ; ")
			case isSQLIdentifierByte(ch):
				builder.WriteByte(lowerASCII(ch))
			default:
				builder.WriteByte(' ')
			}
		}
	}
	switch {
	case inSingle:
		return "", errors.New("unterminated single-quoted string")
	case inDouble:
		return "", errors.New("unterminated double-quoted identifier")
	case inBlockComment:
		return "", errors.New("unterminated block comment")
	default:
		return builder.String(), nil
	}
}

func containsSQLToken(tokens []string, target string) bool {
	for _, token := range tokens {
		if token == target {
			return true
		}
	}
	return false
}

func isSQLIdentifierByte(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func lowerASCII(ch byte) byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	return ch
}

func countRows(ctx context.Context, db *sql.DB, relation string) (int64, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, relation))
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func computeCoverageMode(cacheMin, retainedMin *int64) CoverageMode {
	switch {
	case cacheMin == nil:
		return CoverageCompleteHistory
	case retainedMin == nil:
		return CoverageCachePreservedHistory
	case *cacheMin < *retainedMin:
		return CoverageCachePreservedHistory
	case *retainedMin > 1:
		return CoverageRetainedWindowOnly
	default:
		return CoverageCompleteHistory
	}
}

func extendRange(minValue, maxValue *int64, value int64) (*int64, *int64) {
	if minValue == nil || value < *minValue {
		minValue = int64Ptr(value)
	}
	if maxValue == nil || value > *maxValue {
		maxValue = int64Ptr(value)
	}
	return minValue, maxValue
}

func boundsPointers(bounds ledger.Bounds) (*int64, *int64) {
	if !bounds.HasRecords {
		return nil, nil
	}
	return int64Ptr(bounds.MinOffset), int64Ptr(bounds.MaxOffset)
}

func offsetRangeFromBounds(bounds ledger.Bounds) *OffsetRange {
	if !bounds.HasRecords {
		return nil
	}
	return &OffsetRange{Min: bounds.MinOffset, Max: bounds.MaxOffset}
}

func offsetRangeFromPointers(minValue, maxValue *int64) *OffsetRange {
	if minValue == nil || maxValue == nil {
		return nil
	}
	return &OffsetRange{Min: *minValue, Max: *maxValue}
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return int64Ptr(value.Int64)
}

func nullTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	ts := value.Time.UTC()
	return &ts
}

func querySQLForPreset(preset, sqlText string) (string, error) {
	preset = strings.TrimSpace(preset)
	sqlText = strings.TrimSpace(sqlText)
	if preset != "" && sqlText != "" {
		return "", errors.New("analytics query accepts either -preset or -sql, not both")
	}
	if sqlText != "" {
		return sqlText, nil
	}
	switch firstNonEmpty(preset, "overview") {
	case "overview":
		return `
			SELECT
				(SELECT COUNT(*) FROM envelope_facts) AS envelopes,
				(SELECT COUNT(*) FROM event_facts) AS events,
				(SELECT COUNT(*) FROM session_rollups) AS sessions,
				(SELECT COUNT(*) FROM command_rollups) AS command_rollups,
				(SELECT COUNT(*) FROM session_rollups WHERE failure_signal_count > 0) AS sessions_with_failures
		`, nil
	case "sessions":
		return `
			SELECT
				global_session_key,
				source_type,
				source_instance_id,
				project_locator,
				event_count,
				failure_signal_count,
				command_rollup_count,
				last_timestamp
			FROM session_rollups
			ORDER BY last_timestamp DESC
			LIMIT 20
		`, nil
	case "failures":
		return `
			SELECT
				global_session_key,
				source_type,
				source_instance_id,
				failure_signal_count,
				failed_command_rollup_count,
				tool_failure_count,
				last_timestamp
			FROM session_rollups
			WHERE failure_signal_count > 0
			ORDER BY last_timestamp DESC
			LIMIT 20
		`, nil
	case "activity":
		return `
			SELECT
				date_trunc('day', timestamp) AS day,
				kind,
				COUNT(*) AS events
			FROM event_facts
			GROUP BY 1, 2
			ORDER BY day DESC, kind ASC
			LIMIT 100
		`, nil
	default:
		return "", fmt.Errorf("unknown analytics preset %q (supported: overview, sessions, failures, activity)", preset)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func escapeDuckDBString(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}

func snapshotID(meta analyticsMeta, exportedAt time.Time) string {
	return fmt.Sprintf("snapshot_%s", sha256String(strings.Join([]string{
		meta.LedgerInstanceID,
		meta.MachineID,
		exportedAt.UTC().Format(time.RFC3339Nano),
	}, "|"))[:16])
}

func sha256String(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func engineVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if version := strings.TrimSpace(info.Main.Version); version != "" && version != "(devel)" {
			return "oas " + version
		}
	}
	return "oas dev"
}

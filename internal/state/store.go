package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type Store struct {
	db *sql.DB
}

type PendingBatch struct {
	ID           int64
	SinkID       string
	FromOffset   int64
	ToOffset     int64
	Batch        sinkapi.Batch
	AttemptCount int
	LastError    string
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE IF NOT EXISTS source_checkpoints (
			source_id TEXT NOT NULL,
			artifact_id TEXT NOT NULL,
			cursor TEXT NOT NULL,
			artifact_fingerprint TEXT,
			last_observed_size INTEGER,
			last_observed_mtime TEXT,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (source_id, artifact_id)
		);`,
		`CREATE TABLE IF NOT EXISTS normalization_state (
			name TEXT PRIMARY KEY,
			last_offset INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS session_state (
			session_key TEXT PRIMARY KEY,
			last_sequence INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS sink_checkpoints (
			sink_id TEXT PRIMARY KEY,
			last_offset INTEGER NOT NULL,
			last_event_id TEXT,
			acked_at TEXT NOT NULL,
			delivery_count INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS sink_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sink_id TEXT NOT NULL,
			from_offset INTEGER NOT NULL,
			to_offset INTEGER NOT NULL,
			batch_json TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS dead_letters (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			reference TEXT NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS delivery_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sink_id TEXT NOT NULL,
			ledger_offset INTEGER NOT NULL,
			batch_json TEXT NOT NULL,
			last_event_id TEXT,
			event_count INTEGER NOT NULL,
			payload_bytes INTEGER NOT NULL,
			status TEXT NOT NULL,
			batch_id TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE (sink_id, ledger_offset)
		);`,
		`CREATE INDEX IF NOT EXISTS delivery_items_sink_status_offset_idx
		 ON delivery_items (sink_id, status, ledger_offset);`,
		`CREATE TABLE IF NOT EXISTS delivery_batches (
			batch_id TEXT PRIMARY KEY,
			sink_id TEXT NOT NULL,
			prepared_json TEXT NOT NULL,
			payload_bytes INTEGER NOT NULL,
			ledger_min_offset INTEGER NOT NULL,
			ledger_max_offset INTEGER NOT NULL,
			event_count INTEGER NOT NULL,
			status TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TEXT NOT NULL,
			last_error TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS delivery_batches_due_idx
		 ON delivery_batches (status, next_attempt_at, created_at);`,
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sink_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			outcome TEXT NOT NULL,
			event_count INTEGER NOT NULL,
			payload_bytes INTEGER NOT NULL,
			attempted_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS delivery_attempts_sink_time_idx
		 ON delivery_attempts (sink_id, attempted_at);`,
		`CREATE TABLE IF NOT EXISTS sink_progress (
			sink_id TEXT PRIMARY KEY,
			acked_contiguous_offset INTEGER NOT NULL DEFAULT 0,
			terminal_contiguous_offset INTEGER NOT NULL DEFAULT 0,
			gap_count INTEGER NOT NULL DEFAULT 0,
			successful_batches INTEGER NOT NULL DEFAULT 0,
			successful_events INTEGER NOT NULL DEFAULT 0,
			failed_attempts INTEGER NOT NULL DEFAULT 0,
			quarantined_batches INTEGER NOT NULL DEFAULT 0,
			last_terminal_error TEXT,
			last_success_at TEXT,
			updated_at TEXT NOT NULL
		);`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) GetSourceCheckpoint(sourceID, artifactID string) (sourceapi.Checkpoint, error) {
	row := s.db.QueryRow(
		`SELECT cursor, artifact_fingerprint, last_observed_size, last_observed_mtime
		 FROM source_checkpoints WHERE source_id = ? AND artifact_id = ?`,
		sourceID,
		artifactID,
	)
	var checkpoint sourceapi.Checkpoint
	var mtime string
	err := row.Scan(
		&checkpoint.Cursor,
		&checkpoint.ArtifactFingerprint,
		&checkpoint.LastObservedSize,
		&mtime,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceapi.Checkpoint{}, nil
	}
	if err != nil {
		return sourceapi.Checkpoint{}, err
	}
	if mtime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, mtime)
		if err == nil {
			checkpoint.LastObservedModTime = parsed
		}
	}
	return checkpoint, nil
}

func (s *Store) PutSourceCheckpoint(sourceID, artifactID string, checkpoint sourceapi.Checkpoint) error {
	_, err := s.db.Exec(
		`INSERT INTO source_checkpoints (source_id, artifact_id, cursor, artifact_fingerprint, last_observed_size, last_observed_mtime, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(source_id, artifact_id) DO UPDATE SET
		   cursor = excluded.cursor,
		   artifact_fingerprint = excluded.artifact_fingerprint,
		   last_observed_size = excluded.last_observed_size,
		   last_observed_mtime = excluded.last_observed_mtime,
		   updated_at = excluded.updated_at`,
		sourceID,
		artifactID,
		checkpoint.Cursor,
		checkpoint.ArtifactFingerprint,
		checkpoint.LastObservedSize,
		checkpoint.LastObservedModTime.UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) GetNormalizationOffset(name string) (int64, error) {
	row := s.db.QueryRow(`SELECT last_offset FROM normalization_state WHERE name = ?`, name)
	var offset int64
	err := row.Scan(&offset)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return offset, err
}

func (s *Store) SetNormalizationOffset(name string, offset int64) error {
	_, err := s.db.Exec(
		`INSERT INTO normalization_state (name, last_offset) VALUES (?, ?)
		 ON CONFLICT(name) DO UPDATE SET last_offset = excluded.last_offset`,
		name,
		offset,
	)
	return err
}

func (s *Store) GetSessionSequence(sessionKey string) (int, error) {
	row := s.db.QueryRow(`SELECT last_sequence FROM session_state WHERE session_key = ?`, sessionKey)
	var sequence int
	err := row.Scan(&sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return sequence, err
}

func (s *Store) SetSessionSequence(sessionKey string, sequence int) error {
	_, err := s.db.Exec(
		`INSERT INTO session_state (session_key, last_sequence) VALUES (?, ?)
		 ON CONFLICT(session_key) DO UPDATE SET last_sequence = excluded.last_sequence`,
		sessionKey,
		sequence,
	)
	return err
}

func (s *Store) PutSinkCheckpoint(checkpoint schema.SinkCheckpoint) error {
	_, err := s.db.Exec(
		`INSERT INTO sink_checkpoints (sink_id, last_offset, last_event_id, acked_at, delivery_count)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(sink_id) DO UPDATE SET
		   last_offset = excluded.last_offset,
		   last_event_id = excluded.last_event_id,
		   acked_at = excluded.acked_at,
		   delivery_count = excluded.delivery_count`,
		checkpoint.SinkID,
		checkpoint.LastLedgerOffset,
		checkpoint.LastEventID,
		checkpoint.AckedAt.UTC().Format(time.RFC3339Nano),
		checkpoint.DeliveryCount,
	)
	return err
}

func (s *Store) RecordDeadLetter(kind, reference, message string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO dead_letters (kind, reference, error_message, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		kind,
		reference,
		message,
		string(data),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) EnqueueSinkBatch(sinkID string, fromOffset, toOffset int64, batch sinkapi.Batch, lastError string) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO sink_queue (sink_id, from_offset, to_offset, batch_json, attempt_count, last_error, created_at)
		 VALUES (?, ?, ?, ?, 0, ?, ?)`,
		sinkID,
		fromOffset,
		toOffset,
		string(data),
		lastError,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) ListSinkBatches(sinkID string, limit int) ([]PendingBatch, error) {
	rows, err := s.db.Query(
		`SELECT id, sink_id, from_offset, to_offset, batch_json, attempt_count, COALESCE(last_error, '')
		 FROM sink_queue WHERE sink_id = ? ORDER BY id ASC LIMIT ?`,
		sinkID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pending []PendingBatch
	for rows.Next() {
		var (
			item      PendingBatch
			batchJSON string
		)
		if err := rows.Scan(&item.ID, &item.SinkID, &item.FromOffset, &item.ToOffset, &batchJSON, &item.AttemptCount, &item.LastError); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(batchJSON), &item.Batch); err != nil {
			return nil, err
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

func (s *Store) DeleteSinkBatch(id int64) error {
	_, err := s.db.Exec(`DELETE FROM sink_queue WHERE id = ?`, id)
	return err
}

func (s *Store) IncrementSinkBatchAttempt(id int64, lastError string) error {
	_, err := s.db.Exec(
		`UPDATE sink_queue SET attempt_count = attempt_count + 1, last_error = ? WHERE id = ?`,
		lastError,
		id,
	)
	return err
}

func (s *Store) MinimumSinkCheckpointOffset() (int64, bool, error) {
	row := s.db.QueryRow(`SELECT MIN(last_offset) FROM sink_checkpoints`)
	var offset sql.NullInt64
	if err := row.Scan(&offset); err != nil {
		return 0, false, err
	}
	if !offset.Valid {
		return 0, false, nil
	}
	return offset.Int64, true, nil
}

func (s *Store) Compact() error {
	for _, statement := range []string{
		`PRAGMA wal_checkpoint(TRUNCATE);`,
		`VACUUM;`,
	} {
		if _, err := s.db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

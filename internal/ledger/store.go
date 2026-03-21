package ledger

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

type Record struct {
	Offset   int64
	Envelope schema.RawEnvelope
}
type Store struct {
	db *sql.DB
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
		`CREATE TABLE IF NOT EXISTS raw_ledger (
			offset INTEGER PRIMARY KEY AUTOINCREMENT,
			envelope_json TEXT NOT NULL,
			source_type TEXT NOT NULL,
			source_instance_id TEXT NOT NULL,
			artifact_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			observed_at TEXT NOT NULL
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

func (s *Store) Append(envelope schema.RawEnvelope) (int64, error) {
	envelope = schema.EnsureEnvelopeID(envelope)
	payload, err := json.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	result, err := s.db.Exec(
		`INSERT INTO raw_ledger (envelope_json, source_type, source_instance_id, artifact_id, content_hash, observed_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(payload),
		envelope.SourceType,
		envelope.SourceInstanceID,
		envelope.ArtifactID,
		envelope.ContentHash,
		envelope.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Store) ListAfter(offset int64, limit int) ([]Record, error) {
	rows, err := s.db.Query(
		`SELECT offset, envelope_json FROM raw_ledger WHERE offset > ? ORDER BY offset ASC LIMIT ?`,
		offset,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var (
			recordOffset int64
			envelopeJSON string
		)
		if err := rows.Scan(&recordOffset, &envelopeJSON); err != nil {
			return nil, err
		}
		var envelope schema.RawEnvelope
		if err := json.Unmarshal([]byte(envelopeJSON), &envelope); err != nil {
			return nil, err
		}
		records = append(records, Record{Offset: recordOffset, Envelope: envelope})
	}
	return records, rows.Err()
}

func (s *Store) DeleteThrough(offset int64) (int64, error) {
	if offset <= 0 {
		return 0, nil
	}
	result, err := s.db.Exec(`DELETE FROM raw_ledger WHERE offset <= ?`, offset)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
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

package ledger

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

type Metadata struct {
	InstanceID string
	CreatedAt  time.Time
}

type Bounds struct {
	MinOffset  int64
	MaxOffset  int64
	HasRecords bool
}

type Store struct {
	db       *sql.DB
	metadata Metadata
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
		`CREATE TABLE IF NOT EXISTS ledger_meta (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			ledger_instance_id TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	metadata, err := ensureMetadata(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, metadata: metadata}, nil
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

func (s *Store) Metadata() Metadata {
	return s.metadata
}

func (s *Store) Bounds() (Bounds, error) {
	row := s.db.QueryRow(`SELECT MIN(offset), MAX(offset) FROM raw_ledger`)
	var (
		minOffset sql.NullInt64
		maxOffset sql.NullInt64
	)
	if err := row.Scan(&minOffset, &maxOffset); err != nil {
		return Bounds{}, err
	}
	if !minOffset.Valid || !maxOffset.Valid {
		return Bounds{}, nil
	}
	return Bounds{
		MinOffset:  minOffset.Int64,
		MaxOffset:  maxOffset.Int64,
		HasRecords: true,
	}, nil
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

func ensureMetadata(db *sql.DB) (Metadata, error) {
	row := db.QueryRow(`SELECT ledger_instance_id, created_at FROM ledger_meta WHERE singleton = 1`)
	var (
		metadata Metadata
		created  string
	)
	switch err := row.Scan(&metadata.InstanceID, &created); err {
	case nil:
		parsed, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return Metadata{}, err
		}
		metadata.CreatedAt = parsed
		return metadata, nil
	case sql.ErrNoRows:
	default:
		return Metadata{}, err
	}

	metadata = Metadata{
		InstanceID: "ledger_" + randomHex(8),
		CreatedAt:  time.Now().UTC(),
	}
	if _, err := db.Exec(
		`INSERT INTO ledger_meta (singleton, ledger_instance_id, created_at) VALUES (1, ?, ?)`,
		metadata.InstanceID,
		metadata.CreatedAt.Format(time.RFC3339Nano),
	); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(buf)
}

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg sinkapi.Config
	db  *sql.DB
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(context.Context) error {
	path := s.cfg.Options["path"]
	if path == "" {
		return errors.New("sqlite sink requires options.path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE IF NOT EXISTS canonical_events (
			event_id TEXT PRIMARY KEY,
			session_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			event_json TEXT NOT NULL
		);`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return err
		}
	}
	s.db = db
	return nil
}

func (s *Sink) SendBatch(_ context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	if s.db == nil {
		return sinkapi.Result{}, errors.New("sqlite sink not initialized")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return sinkapi.Result{}, err
	}
	for _, event := range batch.Events {
		payload, err := json.Marshal(event)
		if err != nil {
			_ = tx.Rollback()
			return sinkapi.Result{}, err
		}
		if _, err := tx.Exec(
			`INSERT OR REPLACE INTO canonical_events (event_id, session_key, kind, timestamp, event_json)
			 VALUES (?, ?, ?, ?, ?)`,
			event.EventID,
			event.SessionKey,
			event.Kind,
			event.Timestamp.UTC().Format(time.RFC3339Nano),
			string(payload),
		); err != nil {
			_ = tx.Rollback()
			return sinkapi.Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return sinkapi.Result{}, err
	}
	return sinkapi.Result{
		Acked: len(batch.Events),
		Checkpoint: schema.SinkCheckpoint{
			SinkID:        s.cfg.ID,
			AckedAt:       time.Now().UTC(),
			DeliveryCount: len(batch.Events),
		},
	}, nil
}

func (s *Sink) Flush(context.Context) error  { return nil }
func (s *Sink) Health(context.Context) error { return nil }
func (s *Sink) Close(context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

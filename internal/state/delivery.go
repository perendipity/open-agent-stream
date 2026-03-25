package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

const (
	DeliveryItemStatusPending     = "pending"
	DeliveryItemStatusSealed      = "sealed"
	DeliveryItemStatusAcked       = "acked"
	DeliveryItemStatusQuarantined = "quarantined"
	DeliveryItemStatusSkipped     = "skipped"

	DeliveryBatchStatusPending     = "pending"
	DeliveryBatchStatusRetrying    = "retrying"
	DeliveryBatchStatusQuarantined = "quarantined"
)

type DeliveryItem struct {
	ID           int64
	SinkID       string
	LedgerOffset int64
	Batch        sinkapi.Batch
	LastEventID  string
	EventCount   int
	PayloadBytes int
	Status       string
	BatchID      string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type DeliveryBatch struct {
	BatchID         string
	SinkID          string
	PreparedJSON    []byte
	PayloadBytes    int
	LedgerMinOffset int64
	LedgerMaxOffset int64
	EventCount      int
	Status          string
	AttemptCount    int
	NextAttemptAt   time.Time
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type DeliverySummary struct {
	SinkID                   string
	QueueDepth               int
	ReadyBatchCount          int
	RetryingBatchCount       int
	QuarantinedBatchCount    int
	OldestPendingAt          time.Time
	NextAttemptAt            time.Time
	AckedContiguousOffset    int64
	TerminalContiguousOffset int64
	GapCount                 int
	SuccessfulBatches        int
	SuccessfulEvents         int
	FailedAttempts           int
	QuarantinedBatches       int
	LastTerminalError        string
	EventsLastMinute         int
	AttemptsLastMinute       int
	SuccessesLastMinute      int
	AvgBatchEventsLastMinute float64
	MaxBatchEventsLastMinute int
}

func (s *Store) EnsureSinkProgress(sinkID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(
		`INSERT INTO sink_progress (sink_id, updated_at) VALUES (?, ?)
		 ON CONFLICT(sink_id) DO NOTHING`,
		sinkID,
		now,
	)
	return err
}

func (s *Store) EnqueueDeliveryItem(sinkID string, ledgerOffset int64, batch sinkapi.Batch, status string, createdAt time.Time) error {
	if err := s.EnsureSinkProgress(sinkID); err != nil {
		return err
	}
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	lastEventID := ""
	if len(batch.Events) > 0 {
		lastEventID = batch.Events[len(batch.Events)-1].EventID
	}
	ts := createdAt.UTC().Format(time.RFC3339Nano)
	_, err = s.db.Exec(
		`INSERT INTO delivery_items (
			sink_id, ledger_offset, batch_json, last_event_id, event_count, payload_bytes, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(sink_id, ledger_offset) DO NOTHING`,
		sinkID,
		ledgerOffset,
		string(data),
		lastEventID,
		len(batch.Events),
		len(data),
		status,
		ts,
		ts,
	)
	if err != nil {
		return err
	}
	if status == DeliveryItemStatusSkipped {
		return s.refreshSinkProgress(sinkID, createdAt)
	}
	return nil
}

func (s *Store) ListPendingDeliveryItems(sinkID string, limit int) ([]DeliveryItem, error) {
	rows, err := s.db.Query(
		`SELECT id, sink_id, ledger_offset, batch_json, COALESCE(last_event_id, ''), event_count, payload_bytes, status, COALESCE(batch_id, ''), created_at, updated_at
		 FROM delivery_items
		 WHERE sink_id = ? AND status = ?
		 ORDER BY ledger_offset ASC
		 LIMIT ?`,
		sinkID,
		DeliveryItemStatusPending,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveryItems(rows)
}

func (s *Store) SealDeliveryBatch(batch DeliveryBatch, itemIDs []int64) error {
	if len(itemIDs) == 0 {
		return errors.New("delivery batch requires at least one item")
	}
	return withTx(s.db, func(tx *sql.Tx) error {
		if err := ensureSinkProgressTx(tx, batch.SinkID, batch.UpdatedAt); err != nil {
			return err
		}
		_, err := tx.Exec(
			`INSERT INTO delivery_batches (
				batch_id, sink_id, prepared_json, payload_bytes, ledger_min_offset, ledger_max_offset, event_count,
				status, attempt_count, next_attempt_at, last_error, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			batch.BatchID,
			batch.SinkID,
			string(batch.PreparedJSON),
			batch.PayloadBytes,
			batch.LedgerMinOffset,
			batch.LedgerMaxOffset,
			batch.EventCount,
			batch.Status,
			batch.AttemptCount,
			batch.NextAttemptAt.UTC().Format(time.RFC3339Nano),
			batch.LastError,
			batch.CreatedAt.UTC().Format(time.RFC3339Nano),
			batch.UpdatedAt.UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			return err
		}
		if err := updateDeliveryItemsBatchTx(tx, batch.BatchID, itemIDs, batch.UpdatedAt); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) ListDueDeliveryBatches(now time.Time, limit int) ([]DeliveryBatch, error) {
	rows, err := s.db.Query(
		`SELECT batch_id, sink_id, prepared_json, payload_bytes, ledger_min_offset, ledger_max_offset, event_count,
		        status, attempt_count, next_attempt_at, COALESCE(last_error, ''), created_at, updated_at
		 FROM delivery_batches
		 WHERE status IN (?, ?) AND next_attempt_at <= ?
		 ORDER BY next_attempt_at ASC, created_at ASC
		 LIMIT ?`,
		DeliveryBatchStatusPending,
		DeliveryBatchStatusRetrying,
		now.UTC().Format(time.RFC3339Nano),
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveryBatches(rows)
}

func (s *Store) ListDeliveryBatches(sinkID string, statuses []string, limit int) ([]DeliveryBatch, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT batch_id, sink_id, prepared_json, payload_bytes, ledger_min_offset, ledger_max_offset, event_count,
	        status, attempt_count, next_attempt_at, COALESCE(last_error, ''), created_at, updated_at
	 FROM delivery_batches`
	args := make([]any, 0, len(statuses)+2)
	var clauses []string
	if sinkID != "" {
		clauses = append(clauses, "sink_id = ?")
		args = append(args, sinkID)
	}
	if len(statuses) > 0 {
		placeholders := make([]string, 0, len(statuses))
		for _, status := range statuses {
			placeholders = append(placeholders, "?")
			args = append(args, status)
		}
		clauses = append(clauses, "status IN ("+strings.Join(placeholders, ", ")+")")
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC, created_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveryBatches(rows)
}

func (s *Store) GetDeliveryBatch(batchID string) (DeliveryBatch, error) {
	return s.getDeliveryBatch(batchID)
}

func (s *Store) RetryDeliveryBatch(batchID string, updatedAt time.Time) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		batch, err := getDeliveryBatchTx(tx, batchID)
		if err != nil {
			return err
		}
		if batch.Status != DeliveryBatchStatusQuarantined {
			return fmt.Errorf("delivery batch %s is %s; only quarantined batches can be retried explicitly", batchID, batch.Status)
		}
		_, err = tx.Exec(
			`UPDATE delivery_batches
			 SET status = ?, next_attempt_at = ?, last_error = '', updated_at = ?
			 WHERE batch_id = ?`,
			DeliveryBatchStatusRetrying,
			updatedAt.UTC().Format(time.RFC3339Nano),
			updatedAt.UTC().Format(time.RFC3339Nano),
			batchID,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(
			`UPDATE sink_progress
			 SET updated_at = ?
			 WHERE sink_id = ?`,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batch.SinkID,
		)
		return err
	})
}

func (s *Store) MarkDeliveryBatchRetry(batchID, lastError string, nextAttemptAt, updatedAt time.Time) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		batch, err := getDeliveryBatchTx(tx, batchID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(
			`UPDATE delivery_batches
			 SET status = ?, attempt_count = attempt_count + 1, next_attempt_at = ?, last_error = ?, updated_at = ?
			 WHERE batch_id = ?`,
			DeliveryBatchStatusRetrying,
			nextAttemptAt.UTC().Format(time.RFC3339Nano),
			lastError,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batchID,
		)
		if err != nil {
			return err
		}
		if err := recordDeliveryAttemptTx(tx, batch.SinkID, batchID, "retry", batch.EventCount, batch.PayloadBytes, updatedAt); err != nil {
			return err
		}
		_, err = tx.Exec(
			`UPDATE sink_progress
			 SET failed_attempts = failed_attempts + 1, updated_at = ?
			 WHERE sink_id = ?`,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batch.SinkID,
		)
		return err
	})
}

func (s *Store) MarkDeliveryBatchSucceeded(batchID string, updatedAt time.Time) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		batch, err := getDeliveryBatchTx(tx, batchID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE delivery_items SET status = ?, updated_at = ? WHERE batch_id = ?`,
			DeliveryItemStatusAcked,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batchID,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM delivery_batches WHERE batch_id = ?`, batchID); err != nil {
			return err
		}
		if err := recordDeliveryAttemptTx(tx, batch.SinkID, batchID, "success", batch.EventCount, batch.PayloadBytes, updatedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE sink_progress
			 SET successful_batches = successful_batches + 1,
			     successful_events = successful_events + ?,
			     last_success_at = ?,
			     updated_at = ?
			 WHERE sink_id = ?`,
			batch.EventCount,
			updatedAt.UTC().Format(time.RFC3339Nano),
			updatedAt.UTC().Format(time.RFC3339Nano),
			batch.SinkID,
		); err != nil {
			return err
		}
		return refreshSinkProgressTx(tx, batch.SinkID, updatedAt)
	})
}

func (s *Store) QuarantineDeliveryBatch(batchID, lastError string, updatedAt time.Time) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		batch, err := getDeliveryBatchTx(tx, batchID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE delivery_items SET status = ?, updated_at = ? WHERE batch_id = ?`,
			DeliveryItemStatusQuarantined,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batchID,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE delivery_batches
			 SET status = ?, attempt_count = attempt_count + 1, last_error = ?, updated_at = ?
			 WHERE batch_id = ?`,
			DeliveryBatchStatusQuarantined,
			lastError,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batchID,
		); err != nil {
			return err
		}
		if err := recordDeliveryAttemptTx(tx, batch.SinkID, batchID, "quarantine", batch.EventCount, batch.PayloadBytes, updatedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE sink_progress
			 SET failed_attempts = failed_attempts + 1,
			     quarantined_batches = quarantined_batches + 1,
			     last_terminal_error = ?,
			     updated_at = ?
			 WHERE sink_id = ?`,
			lastError,
			updatedAt.UTC().Format(time.RFC3339Nano),
			batch.SinkID,
		); err != nil {
			return err
		}
		return refreshSinkProgressTx(tx, batch.SinkID, updatedAt)
	})
}

func (s *Store) DeliverySummary(sinkID string, now time.Time) (DeliverySummary, error) {
	var summary DeliverySummary
	summary.SinkID = sinkID
	if err := s.EnsureSinkProgress(sinkID); err != nil {
		return summary, err
	}
	row := s.db.QueryRow(
		`SELECT acked_contiguous_offset, terminal_contiguous_offset, gap_count,
		        successful_batches, successful_events, failed_attempts, quarantined_batches,
		        COALESCE(last_terminal_error, '')
		 FROM sink_progress
		 WHERE sink_id = ?`,
		sinkID,
	)
	if err := row.Scan(
		&summary.AckedContiguousOffset,
		&summary.TerminalContiguousOffset,
		&summary.GapCount,
		&summary.SuccessfulBatches,
		&summary.SuccessfulEvents,
		&summary.FailedAttempts,
		&summary.QuarantinedBatches,
		&summary.LastTerminalError,
	); err != nil {
		return summary, err
	}
	row = s.db.QueryRow(
		`SELECT COALESCE(SUM(CASE WHEN status IN (?, ?) THEN 1 ELSE 0 END), 0),
		        COALESCE(MIN(CASE WHEN status IN (?, ?) THEN created_at END), ''),
		        COALESCE(MIN(CASE WHEN status IN (?, ?) THEN next_attempt_at END), ''),
		        COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0)
		 FROM delivery_batches
		 WHERE sink_id = ?`,
		DeliveryBatchStatusPending,
		DeliveryBatchStatusRetrying,
		DeliveryBatchStatusPending,
		DeliveryBatchStatusRetrying,
		DeliveryBatchStatusPending,
		DeliveryBatchStatusRetrying,
		DeliveryBatchStatusPending,
		DeliveryBatchStatusRetrying,
		DeliveryBatchStatusQuarantined,
		sinkID,
	)
	var oldestPendingRaw, nextAttemptRaw string
	if err := row.Scan(
		&summary.QueueDepth,
		&oldestPendingRaw,
		&nextAttemptRaw,
		&summary.ReadyBatchCount,
		&summary.RetryingBatchCount,
		&summary.QuarantinedBatchCount,
	); err != nil {
		return summary, err
	}
	if oldestPendingRaw != "" {
		summary.OldestPendingAt, _ = time.Parse(time.RFC3339Nano, oldestPendingRaw)
	}
	if nextAttemptRaw != "" {
		summary.NextAttemptAt, _ = time.Parse(time.RFC3339Nano, nextAttemptRaw)
	}
	cutoff := now.Add(-1 * time.Minute).UTC().Format(time.RFC3339Nano)
	row = s.db.QueryRow(
		`SELECT COALESCE(SUM(event_count), 0),
		        COUNT(*),
		        COALESCE(SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END), 0),
		        COALESCE(AVG(event_count), 0),
		        COALESCE(MAX(event_count), 0)
		 FROM delivery_attempts
		 WHERE sink_id = ? AND attempted_at >= ?`,
		sinkID,
		cutoff,
	)
	if err := row.Scan(
		&summary.EventsLastMinute,
		&summary.AttemptsLastMinute,
		&summary.SuccessesLastMinute,
		&summary.AvgBatchEventsLastMinute,
		&summary.MaxBatchEventsLastMinute,
	); err != nil {
		return summary, err
	}
	return summary, nil
}

func (s *Store) MinimumSinkTerminalOffset() (int64, bool, error) {
	row := s.db.QueryRow(`SELECT MIN(terminal_contiguous_offset) FROM sink_progress`)
	var offset sql.NullInt64
	if err := row.Scan(&offset); err != nil {
		return 0, false, err
	}
	if !offset.Valid {
		return s.MinimumSinkCheckpointOffset()
	}
	return offset.Int64, true, nil
}

func (s *Store) refreshSinkProgress(sinkID string, updatedAt time.Time) error {
	return withTx(s.db, func(tx *sql.Tx) error {
		return refreshSinkProgressTx(tx, sinkID, updatedAt)
	})
}

func refreshSinkProgressTx(tx *sql.Tx, sinkID string, updatedAt time.Time) error {
	if err := ensureSinkProgressTx(tx, sinkID, updatedAt); err != nil {
		return err
	}
	row := tx.QueryRow(
		`SELECT acked_contiguous_offset, terminal_contiguous_offset FROM sink_progress WHERE sink_id = ?`,
		sinkID,
	)
	var acked, terminal int64
	if err := row.Scan(&acked, &terminal); err != nil {
		return err
	}
	lastEventID := ""
	nextAcked, lastEventID, err := advanceContiguousOffsetTx(tx, sinkID, acked, true)
	if err != nil {
		return err
	}
	nextTerminal, _, err := advanceContiguousOffsetTx(tx, sinkID, terminal, false)
	if err != nil {
		return err
	}
	gapCount := 0
	if nextTerminal > nextAcked {
		gapCount = int(nextTerminal - nextAcked)
	}
	if _, err := tx.Exec(
		`UPDATE sink_progress
		 SET acked_contiguous_offset = ?, terminal_contiguous_offset = ?, gap_count = ?, updated_at = ?
		 WHERE sink_id = ?`,
		nextAcked,
		nextTerminal,
		gapCount,
		updatedAt.UTC().Format(time.RFC3339Nano),
		sinkID,
	); err != nil {
		return err
	}
	checkpoint := schema.SinkCheckpoint{
		SinkID:           sinkID,
		LastLedgerOffset: nextAcked,
		LastEventID:      lastEventID,
		AckedAt:          updatedAt.UTC(),
	}
	return putSinkCheckpointTx(tx, checkpoint)
}

func advanceContiguousOffsetTx(tx *sql.Tx, sinkID string, start int64, successOnly bool) (int64, string, error) {
	current := start
	lastEventID := ""
	for {
		row := tx.QueryRow(
			`SELECT status, COALESCE(last_event_id, '') FROM delivery_items WHERE sink_id = ? AND ledger_offset = ?`,
			sinkID,
			current+1,
		)
		var status, eventID string
		switch err := row.Scan(&status, &eventID); err {
		case nil:
		case sql.ErrNoRows:
			return current, lastEventID, nil
		default:
			return current, lastEventID, err
		}
		if successOnly {
			if status != DeliveryItemStatusAcked && status != DeliveryItemStatusSkipped {
				return current, lastEventID, nil
			}
		} else {
			if status != DeliveryItemStatusAcked && status != DeliveryItemStatusSkipped && status != DeliveryItemStatusQuarantined {
				return current, lastEventID, nil
			}
		}
		current++
		if eventID != "" {
			lastEventID = eventID
		}
	}
}

func ensureSinkProgressTx(tx *sql.Tx, sinkID string, updatedAt time.Time) error {
	_, err := tx.Exec(
		`INSERT INTO sink_progress (sink_id, updated_at) VALUES (?, ?)
		 ON CONFLICT(sink_id) DO NOTHING`,
		sinkID,
		updatedAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func updateDeliveryItemsBatchTx(tx *sql.Tx, batchID string, itemIDs []int64, updatedAt time.Time) error {
	query := `UPDATE delivery_items SET status = ?, batch_id = ?, updated_at = ? WHERE id IN (?`
	args := []any{DeliveryItemStatusSealed, batchID, updatedAt.UTC().Format(time.RFC3339Nano), itemIDs[0]}
	for _, id := range itemIDs[1:] {
		query += `, ?`
		args = append(args, id)
	}
	query += `)`
	result, err := tx.Exec(query, args...)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != int64(len(itemIDs)) {
		return fmt.Errorf("sealed %d items, want %d", rowsAffected, len(itemIDs))
	}
	return nil
}

func getDeliveryBatchTx(tx *sql.Tx, batchID string) (DeliveryBatch, error) {
	row := tx.QueryRow(
		`SELECT batch_id, sink_id, prepared_json, payload_bytes, ledger_min_offset, ledger_max_offset, event_count,
		        status, attempt_count, next_attempt_at, COALESCE(last_error, ''), created_at, updated_at
		 FROM delivery_batches
		 WHERE batch_id = ?`,
		batchID,
	)
	return scanDeliveryBatchRow(row)
}

func (s *Store) getDeliveryBatch(batchID string) (DeliveryBatch, error) {
	row := s.db.QueryRow(
		`SELECT batch_id, sink_id, prepared_json, payload_bytes, ledger_min_offset, ledger_max_offset, event_count,
		        status, attempt_count, next_attempt_at, COALESCE(last_error, ''), created_at, updated_at
		 FROM delivery_batches
		 WHERE batch_id = ?`,
		batchID,
	)
	return scanDeliveryBatchRow(row)
}

func scanDeliveryBatchRow(row *sql.Row) (DeliveryBatch, error) {
	var batch DeliveryBatch
	var nextAttemptRaw, createdRaw, updatedRaw string
	if err := row.Scan(
		&batch.BatchID,
		&batch.SinkID,
		&batch.PreparedJSON,
		&batch.PayloadBytes,
		&batch.LedgerMinOffset,
		&batch.LedgerMaxOffset,
		&batch.EventCount,
		&batch.Status,
		&batch.AttemptCount,
		&nextAttemptRaw,
		&batch.LastError,
		&createdRaw,
		&updatedRaw,
	); err != nil {
		return DeliveryBatch{}, err
	}
	batch.NextAttemptAt, _ = time.Parse(time.RFC3339Nano, nextAttemptRaw)
	batch.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdRaw)
	batch.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedRaw)
	return batch, nil
}

func recordDeliveryAttemptTx(tx *sql.Tx, sinkID, batchID, outcome string, eventCount, payloadBytes int, attemptedAt time.Time) error {
	_, err := tx.Exec(
		`INSERT INTO delivery_attempts (sink_id, batch_id, outcome, event_count, payload_bytes, attempted_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		sinkID,
		batchID,
		outcome,
		eventCount,
		payloadBytes,
		attemptedAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func putSinkCheckpointTx(tx *sql.Tx, checkpoint schema.SinkCheckpoint) error {
	_, err := tx.Exec(
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

func scanDeliveryItems(rows *sql.Rows) ([]DeliveryItem, error) {
	var items []DeliveryItem
	for rows.Next() {
		var (
			item       DeliveryItem
			batchJSON  string
			createdRaw string
			updatedRaw string
		)
		if err := rows.Scan(
			&item.ID,
			&item.SinkID,
			&item.LedgerOffset,
			&batchJSON,
			&item.LastEventID,
			&item.EventCount,
			&item.PayloadBytes,
			&item.Status,
			&item.BatchID,
			&createdRaw,
			&updatedRaw,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(batchJSON), &item.Batch); err != nil {
			return nil, err
		}
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdRaw)
		item.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedRaw)
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanDeliveryBatches(rows *sql.Rows) ([]DeliveryBatch, error) {
	var batches []DeliveryBatch
	for rows.Next() {
		var (
			batch          DeliveryBatch
			preparedJSON   string
			nextAttemptRaw string
			createdRaw     string
			updatedRaw     string
		)
		if err := rows.Scan(
			&batch.BatchID,
			&batch.SinkID,
			&preparedJSON,
			&batch.PayloadBytes,
			&batch.LedgerMinOffset,
			&batch.LedgerMaxOffset,
			&batch.EventCount,
			&batch.Status,
			&batch.AttemptCount,
			&nextAttemptRaw,
			&batch.LastError,
			&createdRaw,
			&updatedRaw,
		); err != nil {
			return nil, err
		}
		batch.PreparedJSON = []byte(preparedJSON)
		batch.NextAttemptAt, _ = time.Parse(time.RFC3339Nano, nextAttemptRaw)
		batch.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdRaw)
		batch.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedRaw)
		batches = append(batches, batch)
	}
	return batches, rows.Err()
}

func withTx(db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

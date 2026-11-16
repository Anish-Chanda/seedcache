package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const timestampLayout = "2006-01-02T15:04:05.000000000Z"

type repository struct {
	db *sql.DB
}

func (r *repository) get(ctx context.Context, hash string) (Record, error) {
	return scanRecord(r.db.QueryRowContext(ctx, selectRecordSQL+" WHERE hash = ?", hash))
}

func (r *repository) list(ctx context.Context) ([]Record, error) {
	rows, err := r.db.QueryContext(ctx, selectRecordSQL+" ORDER BY created_at, hash")
	if err != nil {
		return nil, fmt.Errorf("list replications: %w", err)
	}
	defer rows.Close()

	records := make([]Record, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read replications: %w", err)
	}
	return records, nil
}

func (r *repository) summary(ctx context.Context) (Summary, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT state, COUNT(*) FROM replications GROUP BY state")
	if err != nil {
		return Summary{}, fmt.Errorf("summarize replications: %w", err)
	}
	defer rows.Close()
	var result Summary
	for rows.Next() {
		var state State
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return Summary{}, fmt.Errorf("read replication summary: %w", err)
		}
		switch state {
		case StatePending:
			result.Pending = count
		case StateTransferring:
			result.Transferring = count
			result.Active += count
		case StateVerifying:
			result.Verifying = count
			result.Active += count
		case StateReplicated:
			result.Replicated = count
		case StateFailed:
			result.Failed = count
		case StateBlocked:
			result.Blocked = count
		}
	}
	if err := rows.Err(); err != nil {
		return Summary{}, fmt.Errorf("read replication summary: %w", err)
	}
	return result, nil
}

func (r *repository) createIfAbsent(ctx context.Context, record Record) (Record, bool, error) {
	manifest, err := marshalManifest(record.Manifest)
	if err != nil {
		return Record{}, false, err
	}
	result, err := r.db.ExecContext(ctx, `INSERT INTO replications (
		hash, state, manifest, manifest_fingerprint, source_identity, destination_identity,
		attempts, created_at, updated_at, last_attempt_at, next_attempt_at, verified_at, last_error
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(hash) DO NOTHING`,
		record.Hash, record.State, manifest, record.ManifestFingerprint,
		record.SourceIdentity, record.DestinationIdentity, record.Attempts,
		formatTime(record.CreatedAt), formatTime(record.UpdatedAt), nullableTime(record.LastAttemptAt),
		nullableTime(record.NextAttemptAt), nullableTime(record.VerifiedAt), record.LastError)
	if err != nil {
		return Record{}, false, fmt.Errorf("create replication %s: %w", record.Hash, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return Record{}, false, fmt.Errorf("check replication insert %s: %w", record.Hash, err)
	}
	current, err := r.get(ctx, record.Hash)
	if err != nil {
		return Record{}, false, err
	}
	return current, inserted != 0, nil
}

func (r *repository) replace(ctx context.Context, record Record, expected ...State) (bool, error) {
	if len(expected) == 0 {
		return false, errors.New("replace replication requires an expected state")
	}
	manifest, err := marshalManifest(record.Manifest)
	if err != nil {
		return false, err
	}
	args := []any{
		record.State, manifest, record.ManifestFingerprint, record.SourceIdentity,
		record.DestinationIdentity, record.Attempts, formatTime(record.UpdatedAt),
		nullableTime(record.LastAttemptAt), nullableTime(record.NextAttemptAt),
		nullableTime(record.VerifiedAt), record.LastError, record.Hash,
	}
	placeholders := make([]string, 0, len(expected))
	for _, state := range expected {
		placeholders = append(placeholders, "?")
		args = append(args, state)
	}
	query := `UPDATE replications SET state = ?, manifest = ?, manifest_fingerprint = ?,
		source_identity = ?, destination_identity = ?, attempts = ?, updated_at = ?,
		last_attempt_at = ?, next_attempt_at = ?, verified_at = ?, last_error = ?
		WHERE hash = ? AND state IN (` + strings.Join(placeholders, ",") + ")"
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("update replication %s: %w", record.Hash, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check replication update %s: %w", record.Hash, err)
	}
	return changed != 0, nil
}

func (r *repository) start(ctx context.Context, hash string, now time.Time) (Record, bool, error) {
	result, err := r.db.ExecContext(ctx, `UPDATE replications SET state = ?, attempts = attempts + 1,
		last_attempt_at = ?, next_attempt_at = NULL, updated_at = ?, last_error = ''
		WHERE hash = ? AND state IN (?, ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?)`,
		StateTransferring, formatTime(now), formatTime(now), hash,
		StatePending, StateFailed, formatTime(now))
	if err != nil {
		return Record{}, false, fmt.Errorf("start replication %s: %w", hash, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Record{}, false, fmt.Errorf("check replication start %s: %w", hash, err)
	}
	if changed == 0 {
		return Record{}, false, nil
	}
	record, err := r.get(ctx, hash)
	return record, true, err
}

func (r *repository) nextReady(ctx context.Context, now time.Time) (Record, bool, error) {
	record, err := scanRecord(r.db.QueryRowContext(ctx, selectRecordSQL+` WHERE state = ? OR
		(state = ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
		ORDER BY created_at, hash LIMIT 1`, StatePending, StateFailed, formatTime(now)))
	if errors.Is(err, ErrNotFound) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

func (r *repository) nextDue(ctx context.Context, now time.Time) (*time.Time, error) {
	var due sql.NullString
	err := r.db.QueryRowContext(ctx, `SELECT MIN(CASE
		WHEN state = ? THEN ? ELSE next_attempt_at END)
		FROM replications WHERE state = ? OR state = ?`,
		StatePending, formatTime(now), StatePending, StateFailed).Scan(&due)
	if err != nil {
		return nil, fmt.Errorf("read next replication retry: %w", err)
	}
	if !due.Valid {
		return nil, nil
	}
	parsed, err := time.Parse(timestampLayout, due.String)
	if err != nil {
		return nil, fmt.Errorf("parse next replication retry: %w", err)
	}
	return &parsed, nil
}

func (r *repository) recoverInterrupted(ctx context.Context, now time.Time) error {
	_, err := r.db.ExecContext(ctx, `UPDATE replications SET state = ?, next_attempt_at = ?,
		updated_at = ?, last_error = 'transfer interrupted by daemon restart'
		WHERE state IN (?, ?)`, StateFailed, formatTime(now), formatTime(now), StateTransferring, StateVerifying)
	if err != nil {
		return fmt.Errorf("recover interrupted replications: %w", err)
	}
	return nil
}

const selectRecordSQL = `SELECT hash, state, manifest, manifest_fingerprint, source_identity,
	destination_identity, attempts, created_at, updated_at, last_attempt_at, next_attempt_at,
	verified_at, last_error FROM replications`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRecord(row rowScanner) (Record, error) {
	var record Record
	var manifestJSON string
	var createdAt, updatedAt string
	var lastAttemptAt, nextAttemptAt, verifiedAt sql.NullString
	if err := row.Scan(
		&record.Hash, &record.State, &manifestJSON, &record.ManifestFingerprint,
		&record.SourceIdentity, &record.DestinationIdentity, &record.Attempts,
		&createdAt, &updatedAt, &lastAttemptAt, &nextAttemptAt, &verifiedAt, &record.LastError,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, fmt.Errorf("%w", ErrNotFound)
		}
		return Record{}, fmt.Errorf("read replication: %w", err)
	}
	if err := json.Unmarshal([]byte(manifestJSON), &record.Manifest); err != nil {
		return Record{}, fmt.Errorf("decode replication %s manifest: %w", record.Hash, err)
	}
	var err error
	if record.CreatedAt, err = parseTime(createdAt); err != nil {
		return Record{}, fmt.Errorf("parse replication %s creation time: %w", record.Hash, err)
	}
	if record.UpdatedAt, err = parseTime(updatedAt); err != nil {
		return Record{}, fmt.Errorf("parse replication %s update time: %w", record.Hash, err)
	}
	if record.LastAttemptAt, err = parseNullableTime(lastAttemptAt); err != nil {
		return Record{}, fmt.Errorf("parse replication %s last attempt: %w", record.Hash, err)
	}
	if record.NextAttemptAt, err = parseNullableTime(nextAttemptAt); err != nil {
		return Record{}, fmt.Errorf("parse replication %s next attempt: %w", record.Hash, err)
	}
	if record.VerifiedAt, err = parseNullableTime(verifiedAt); err != nil {
		return Record{}, fmt.Errorf("parse replication %s verification time: %w", record.Hash, err)
	}
	return record, nil
}

func marshalManifest(manifest Manifest) (string, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode replication manifest: %w", err)
	}
	return string(encoded), nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(timestampLayout)
}

func parseTime(value string) (time.Time, error) {
	return time.Parse(timestampLayout, value)
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func parseNullableTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

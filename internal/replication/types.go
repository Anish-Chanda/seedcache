package replication

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound    = errors.New("replication not found")
	ErrConflict    = errors.New("replication state conflict")
	ErrUnavailable = errors.New("replication service unavailable")
	ErrIneligible  = errors.New("torrent is not eligible for replication")
)

type BlockedError struct {
	Reason string
}

func (e *BlockedError) Error() string {
	if e == nil || e.Reason == "" {
		return "torrent is blocked from replication"
	}
	return fmt.Sprintf("torrent is blocked from replication: %s", e.Reason)
}

func (e *BlockedError) Unwrap() error { return ErrIneligible }

type State string

const (
	StatePending      State = "pending"
	StateTransferring State = "transferring"
	StateVerifying    State = "verifying"
	StateReplicated   State = "replicated"
	StateFailed       State = "failed"
	StateBlocked      State = "blocked"
)

type Record struct {
	Hash                string     `json:"hash"`
	State               State      `json:"state"`
	Manifest            Manifest   `json:"manifest"`
	ManifestFingerprint string     `json:"manifest_fingerprint"`
	SourceIdentity      string     `json:"source_identity"`
	DestinationIdentity string     `json:"destination_identity"`
	Attempts            int        `json:"attempts"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	LastAttemptAt       *time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt       *time.Time `json:"next_attempt_at,omitempty"`
	VerifiedAt          *time.Time `json:"verified_at,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
}

type Summary struct {
	Pending      int `json:"pending"`
	Active       int `json:"active"`
	Transferring int `json:"transferring"`
	Verifying    int `json:"verifying"`
	Replicated   int `json:"replicated"`
	Failed       int `json:"failed"`
	Blocked      int `json:"blocked"`
}

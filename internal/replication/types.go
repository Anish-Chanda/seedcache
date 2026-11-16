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
	StateAdopted      State = "adopted"
	StateFailed       State = "failed"
	StateBlocked      State = "blocked"
)

type CacheState string

const (
	CachePresent   CacheState = "present"
	CacheEvicting  CacheState = "evicting"
	CacheEvicted   CacheState = "evicted"
	CacheRestoring CacheState = "restoring"
)

type CacheOperation string

const (
	CacheOperationEvict   CacheOperation = "evict"
	CacheOperationRestore CacheOperation = "restore"
	CacheOperationSeed    CacheOperation = "seed"
)

type Record struct {
	Hash                string         `json:"hash"`
	State               State          `json:"state"`
	Manifest            Manifest       `json:"manifest"`
	ManifestFingerprint string         `json:"manifest_fingerprint"`
	SourceIdentity      string         `json:"source_identity"`
	DestinationIdentity string         `json:"destination_identity"`
	Attempts            int            `json:"attempts"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
	LastAttemptAt       *time.Time     `json:"last_attempt_at,omitempty"`
	NextAttemptAt       *time.Time     `json:"next_attempt_at,omitempty"`
	VerifiedAt          *time.Time     `json:"verified_at,omitempty"`
	LastError           string         `json:"last_error,omitempty"`
	CacheState          CacheState     `json:"cache_state"`
	CacheOperation      CacheOperation `json:"cache_operation,omitempty"`
	CacheUpdatedAt      time.Time      `json:"cache_updated_at"`
	CacheError          string         `json:"cache_error,omitempty"`
	EvictedAt           *time.Time     `json:"evicted_at,omitempty"`
	RestoredAt          *time.Time     `json:"restored_at,omitempty"`
}

type EvictionPlan struct {
	Enabled           bool                `json:"enabled"`
	Triggered         bool                `json:"triggered"`
	FreeBytes         uint64              `json:"free_bytes"`
	TotalBytes        uint64              `json:"total_bytes"`
	FreePercent       float64             `json:"free_percent"`
	LowFreePercent    float64             `json:"low_free_percent"`
	TargetFreePercent float64             `json:"target_free_percent"`
	BytesToTarget     uint64              `json:"bytes_to_target"`
	Message           string              `json:"message,omitempty"`
	Candidates        []EvictionCandidate `json:"candidates"`
}

type EvictionCandidate struct {
	Hash         string        `json:"hash"`
	Name         string        `json:"name"`
	Ratio        float64       `json:"ratio"`
	SeedingTime  time.Duration `json:"seeding_time"`
	TotalSize    int64         `json:"total_size"`
	LastActivity time.Time     `json:"last_activity,omitempty"`
}

type Summary struct {
	Pending      int `json:"pending"`
	Active       int `json:"active"`
	Transferring int `json:"transferring"`
	Verifying    int `json:"verifying"`
	Replicated   int `json:"replicated"`
	Adopted      int `json:"adopted"`
	Failed       int `json:"failed"`
	Blocked      int `json:"blocked"`
}

type RemotePayloadError struct{ Path string }

func (e *RemotePayloadError) Error() string {
	return fmt.Sprintf("destination payload %q is missing or has an unexpected size", e.Path)
}
func (e *RemotePayloadError) Unwrap() error { return &BlockedError{Reason: e.Error()} }

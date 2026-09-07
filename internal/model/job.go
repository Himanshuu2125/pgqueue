// Package model defines jobs, events, states and sentinel errors.
package model

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Stored states, plus the derived "scheduled" state (pending with run_at > now()).
const (
	StatePending   = "pending"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateDead      = "dead"
	StateCancelled = "cancelled"
	StateScheduled = "scheduled"
)

// States lists every state as reported by the API.
var States = []string{StateScheduled, StatePending, StateRunning, StateSucceeded, StateDead, StateCancelled}

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidState  = errors.New("invalid state")
	ErrLeaseLost     = errors.New("lease lost")
	ErrDuplicateName = errors.New("duplicate name")
)

// Job is one row of jobs. State holds the reported state (may be "scheduled").
type Job struct {
	ID              uuid.UUID       `json:"id"`
	TenantID        string          `json:"-"`
	Queue           string          `json:"queue"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Priority        int             `json:"priority"`
	State           string          `json:"state"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	TimeoutSeconds  int             `json:"timeout_seconds"`
	RunAt           time.Time       `json:"run_at"`
	IdempotencyKey  *string         `json:"idempotency_key"`
	CancelRequested bool            `json:"cancel_requested"`
	LeaseOwner      *string         `json:"lease_owner"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at"`
	LeaseToken      *int64          `json:"-"`
	LastError       *string         `json:"last_error"`
	Result          json.RawMessage `json:"result"`
	CronID          *uuid.UUID      `json:"cron_id"`
	TraceParent     *string         `json:"-"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at"`
	FinishedAt      *time.Time      `json:"finished_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// Event is one row of job_events.
type Event struct {
	Event     string          `json:"event"`
	Attempt   int             `json:"attempt"`
	WorkerID  *string         `json:"worker_id"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt time.Time       `json:"created_at"`
}

// UTC converts every timestamp to UTC so JSON renders RFC3339Nano UTC.
func (j *Job) UTC() {
	j.RunAt, j.CreatedAt, j.UpdatedAt = j.RunAt.UTC(), j.CreatedAt.UTC(), j.UpdatedAt.UTC()
	for _, t := range []*time.Time{j.LeaseExpiresAt, j.StartedAt, j.FinishedAt} {
		if t != nil {
			*t = t.UTC()
		}
	}
}

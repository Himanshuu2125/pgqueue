package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

var queueRe = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

type submitReq struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority"`
	MaxAttempts    *int            `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	RunAt          *string         `json:"run_at"`
	DelaySeconds   *int            `json:"delay_seconds"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

func badReq(format string, a ...any) *apiErr {
	return &apiErr{status: http.StatusBadRequest, code: "invalid_request", message: fmt.Sprintf(format, a...)}
}

func intOr(p *int, def, lo, hi int, name string) (int, *apiErr) {
	if p == nil {
		return def, nil
	}
	if *p < lo || *p > hi {
		return 0, badReq("%s must be in %d..%d", name, lo, hi)
	}
	return *p, nil
}

// validateSubmit checks one submit object and applies defaults.
func (s *Server) validateSubmit(req submitReq, tenant string, queues map[string]bool, traceParent string) (store.NewJob, *apiErr) {
	n := store.NewJob{TenantID: tenant, Queue: req.Queue, Type: req.Type, Payload: req.Payload,
		IdempotencyKey: req.IdempotencyKey, DelaySeconds: req.DelaySeconds, TraceParent: traceParent}
	var e *apiErr
	switch {
	case req.Queue == "":
		return n, badReq("queue is required")
	case !queueRe.MatchString(req.Queue):
		return n, badReq("queue must match ^[a-z0-9_-]{1,64}$")
	case !queues[req.Queue]:
		return n, &apiErr{status: http.StatusBadRequest, code: "unknown_queue", message: "queue does not exist"}
	case req.Type == "":
		return n, badReq("type is required")
	case len(req.Payload) == 0 || !bytes.HasPrefix(bytes.TrimSpace(req.Payload), []byte("{")):
		return n, badReq("payload must be a JSON object")
	case len(req.Payload) > config.MaxPayloadBytes:
		return n, badReq("payload exceeds 256 KiB")
	}
	if err := s.reg.Validate(req.Type, req.Payload); err != nil {
		return n, badReq("payload: %v", err)
	}
	if n.Priority, e = intOr(req.Priority, 0, -100, 100, "priority"); e != nil {
		return n, e
	}
	if n.MaxAttempts, e = intOr(req.MaxAttempts, s.cfg.DefaultMaxAttempts, 1, 25, "max_attempts"); e != nil {
		return n, e
	}
	if n.TimeoutSeconds, e = intOr(req.TimeoutSeconds, s.cfg.DefaultTimeoutSeconds, 1, 3600, "timeout_seconds"); e != nil {
		return n, e
	}
	if req.RunAt != nil && req.DelaySeconds != nil {
		return n, badReq("run_at and delay_seconds are mutually exclusive")
	}
	if req.RunAt != nil {
		t, err := time.Parse(time.RFC3339, *req.RunAt)
		if err != nil {
			return n, badReq("run_at must be RFC3339")
		}
		n.RunAt = &t
	}
	if _, e = intOr(req.DelaySeconds, 0, 0, 31536000, "delay_seconds"); e != nil {
		return n, e
	}
	if k := req.IdempotencyKey; k != nil {
		if c := utf8.RuneCountInString(*k); c < 1 || c > 255 {
			return n, badReq("idempotency_key must be 1..255 chars")
		}
		if strings.HasPrefix(*k, "cron:") {
			return n, badReq("idempotency_key must not start with cron:")
		}
	}
	return n, nil
}

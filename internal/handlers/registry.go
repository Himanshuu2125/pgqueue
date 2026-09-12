// Package handlers holds the registered job types: http, noop, sleep, fail, record.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
)

// Handler executes one job attempt.
type Handler func(ctx context.Context, job model.Job) (json.RawMessage, error)

type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks err as non-retryable.
func Permanent(err error) error { return permanent{err} }

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool { return errors.As(err, new(permanent)) }

// Registry maps job types to handlers.
type Registry struct {
	db     *pgxpool.Pool
	client *http.Client
	cfg    config.Handlers
	byType map[string]Handler
}

// NewRegistry builds the registry. db and client may be nil when only Validate is used.
func NewRegistry(db *pgxpool.Pool, client *http.Client, cfg config.Handlers) *Registry {
	if client != nil {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client.Timeout = 0
	}
	r := &Registry{db: db, client: client, cfg: cfg}
	r.byType = map[string]Handler{"http": r.http, "noop": noop, "sleep": sleep, "fail": fail, "record": r.record}
	return r
}

// Run dispatches job to its handler.
func (r *Registry) Run(ctx context.Context, job model.Job) (json.RawMessage, error) {
	h, ok := r.byType[job.Type]
	if !ok {
		return nil, Permanent(fmt.Errorf("unknown job type %q", job.Type))
	}
	return h(ctx, job)
}

type httpPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}
type msPayload struct {
	MS *int `json:"ms"`
}

var forbiddenHeaders = []string{"authorization", "x-queue-token", "host", "x-job-id", "x-lease-token", "x-attempt", "traceparent"}

func decode(payload json.RawMessage, v any) error {
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if d.More() {
		return errors.New("payload: trailing data")
	}
	return nil
}

// Validate checks a payload for typ at submit time. It performs no I/O.
func (r *Registry) Validate(typ string, payload json.RawMessage) error {
	var ms msPayload
	var rate struct {
		Rate *float64 `json:"rate"`
	}
	switch typ {
	case "noop":
		return decode(payload, &struct{}{})
	case "sleep", "record":
		if err := decode(payload, &ms); err != nil {
			return err
		}
		hi := map[string]int{"sleep": 600000, "record": 60000}[typ]
		if (ms.MS == nil && typ == "sleep") || (ms.MS != nil && (*ms.MS < 0 || *ms.MS > hi)) {
			return fmt.Errorf("payload.ms must be an integer in 0..%d", hi)
		}
		return nil
	case "fail":
		if err := decode(payload, &rate); err != nil {
			return err
		}
		if rate.Rate == nil || *rate.Rate < 0 || *rate.Rate > 1 {
			return errors.New("payload.rate must be a number in 0..1")
		}
		return nil
	case "http":
		_, _, err := r.parseHTTP(payload)
		return err
	}
	return fmt.Errorf("unknown type %q", typ)
}

func (r *Registry) parseHTTP(payload json.RawMessage) (httpPayload, *url.URL, error) {
	var p httpPayload
	if err := decode(payload, &p); err != nil {
		return p, nil, err
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return p, nil, errors.New("payload.url must be an absolute http or https URL")
	}
	if !slices.Contains(r.cfg.AllowedHosts, strings.ToLower(u.Hostname())) {
		return p, nil, fmt.Errorf("payload.url host %q is not allowed", u.Hostname())
	}
	if p.Method == "" {
		p.Method = http.MethodPost
	}
	if !slices.Contains([]string{"GET", "POST", "PUT", "DELETE"}, p.Method) {
		return p, nil, errors.New("payload.method must be GET, POST, PUT or DELETE")
	}
	for k := range p.Headers {
		if slices.Contains(forbiddenHeaders, strings.ToLower(k)) {
			return p, nil, fmt.Errorf("payload.headers: %q is not allowed", k)
		}
	}
	return p, u, nil
}

func (r *Registry) http(ctx context.Context, job model.Job) (json.RawMessage, error) {
	p, u, err := r.parseHTTP(job.Payload)
	if err != nil {
		return nil, Permanent(err)
	}
	var body io.Reader
	if len(p.Body) > 0 {
		body = bytes.NewReader(p.Body)
	}
	req, err := http.NewRequestWithContext(ctx, p.Method, u.String(), body)
	if err != nil {
		return nil, Permanent(fmt.Errorf("build request: %w", err))
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Job-Id", job.ID.String())
	if job.LeaseToken != nil {
		req.Header.Set("X-Lease-Token", strconv.FormatInt(*job.LeaseToken, 10))
	}
	req.Header.Set("X-Attempt", strconv.Itoa(job.Attempts))
	if r.cfg.QueueToken != "" {
		req.Header.Set("X-Queue-Token", r.cfg.QueueToken)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, config.HTTPBodyReadCap))
	if err != nil {
		return nil, fmt.Errorf("http read body: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return json.Marshal(map[string]any{"status": resp.StatusCode, "body": strings.ToValidUTF8(string(b[:min(len(b), config.ResultBodyCap)]), "�")})
	}
	err = fmt.Errorf("http %d: %s", resp.StatusCode, b[:min(len(b), 512)])
	switch s := resp.StatusCode; {
	case s >= 400 && s < 500 && s != 408 && s != 409 && s != 429:
		return nil, Permanent(err)
	default:
		return nil, err
	}
}

func noop(context.Context, model.Job) (json.RawMessage, error) { return nil, nil }

func wait(ctx context.Context, ms int) error {
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// msOf reads the optional ms field; payloads were validated at submit.
func msOf(job model.Job) int {
	var p msPayload
	json.Unmarshal(job.Payload, &p) //nolint:errcheck
	if p.MS == nil {
		return 0
	}
	return *p.MS
}

func sleep(ctx context.Context, job model.Job) (json.RawMessage, error) {
	ms := msOf(job)
	if err := wait(ctx, ms); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]int{"slept_ms": ms})
}

func fail(_ context.Context, job model.Job) (json.RawMessage, error) {
	var p struct{ Rate float64 }
	json.Unmarshal(job.Payload, &p) //nolint:errcheck
	if rand.Float64() < p.Rate {
		return nil, errors.New("injected failure")
	}
	return nil, nil
}

func (r *Registry) record(ctx context.Context, job model.Job) (json.RawMessage, error) {
	if job.LeaseToken == nil || job.LeaseOwner == nil {
		return nil, Permanent(errors.New("record: job has no lease"))
	}
	if _, err := r.db.Exec(ctx, `INSERT INTO executions(job_id, lease_token, worker_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		job.ID, *job.LeaseToken, *job.LeaseOwner); err != nil {
		return nil, fmt.Errorf("record execution: %w", err)
	}
	return nil, wait(ctx, msOf(job))
}

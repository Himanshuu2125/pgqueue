package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Himanshuu2125/pgqueue/internal/api"
	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

type env struct {
	t     *testing.T
	pool  *pgxpool.Pool
	srv   *httptest.Server
	queue string
}

func newEnv(t *testing.T) *env {
	pool := needDB(t)
	cfg := config.Config{DefaultMaxAttempts: 5, DefaultTimeoutSeconds: 60, AdminUser: "admin", AdminPassword: "pw",
		Handlers: config.Handlers{AllowedHosts: []string{"hooks.internal"}}}
	st := store.New(pool, cfg)
	mr := miniredis.RunT(t)
	s, err := api.New(cfg, st, handlers.NewRegistry(nil, nil, cfg.Handlers), redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	require.NoError(t, err)
	srv := httptest.NewServer(s.Router())
	t.Cleanup(srv.Close)
	queue := "q" + randHex(6)
	_, err = pool.Exec(context.Background(), `INSERT INTO queues(name) VALUES ($1)`, queue)
	require.NoError(t, err)
	return &env{t: t, pool: pool, srv: srv, queue: queue}
}

// tenant seeds a fresh tenant and returns its API key.
func (e *env) tenant() string {
	id, key := "t"+randHex(6), "key-"+randHex(12)
	st := store.New(e.pool, config.Config{})
	require.NoError(e.t, st.SeedTenants(context.Background(), []config.Tenant{{ID: id, Key: key, Rate: 100000, Burst: 100000}}))
	return key
}

func (e *env) do(method, path, key string, body any) (int, map[string]any) {
	e.t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		r = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, r)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close()
	out := map[string]any{}
	json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck
	return resp.StatusCode, out
}

func (e *env) job(extra map[string]any) map[string]any {
	m := map[string]any{"queue": e.queue, "type": "noop", "payload": map[string]any{}}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func errCode(out map[string]any) string {
	if e, ok := out["error"].(map[string]any); ok {
		return e["code"].(string)
	}
	return ""
}

func TestSubmitValidation(t *testing.T) {
	e := newEnv(t)
	key := e.tenant()
	big := `{"queue":"` + e.queue + `","type":"record","payload":{"x":"` + strings.Repeat("a", 300<<10) + `"}}`
	cases := []struct {
		name   string
		body   any
		status int
		code   string
	}{
		{"missing queue", map[string]any{"type": "noop", "payload": map[string]any{}}, 400, "invalid_request"},
		{"bad queue name", e.job(map[string]any{"queue": "Bad Queue"}), 400, "invalid_request"},
		{"unknown queue", e.job(map[string]any{"queue": "nope"}), 400, "unknown_queue"},
		{"missing type", e.job(map[string]any{"type": ""}), 400, "invalid_request"},
		{"unknown type", e.job(map[string]any{"type": "exec"}), 400, "invalid_request"},
		{"payload not object", e.job(map[string]any{"payload": []int{1}}), 400, "invalid_request"},
		{"payload invalid for type", e.job(map[string]any{"type": "sleep", "payload": map[string]any{"ms": -1}}), 400, "invalid_request"},
		{"payload too large", big, 400, "invalid_request"},
		{"ssrf host", e.job(map[string]any{"type": "http", "payload": map[string]any{"url": "http://169.254.169.254/"}}), 400, "invalid_request"},
		{"priority", e.job(map[string]any{"priority": 101}), 400, "invalid_request"},
		{"max_attempts", e.job(map[string]any{"max_attempts": 0}), 400, "invalid_request"},
		{"timeout", e.job(map[string]any{"timeout_seconds": 3601}), 400, "invalid_request"},
		{"run_at and delay", e.job(map[string]any{"run_at": "2030-01-01T00:00:00Z", "delay_seconds": 5}), 400, "invalid_request"},
		{"bad run_at", e.job(map[string]any{"run_at": "tomorrow"}), 400, "invalid_request"},
		{"delay range", e.job(map[string]any{"delay_seconds": -1}), 400, "invalid_request"},
		{"key too long", e.job(map[string]any{"idempotency_key": strings.Repeat("k", 256)}), 400, "invalid_request"},
		{"cron prefix", e.job(map[string]any{"idempotency_key": "cron:x"}), 400, "invalid_request"},
		{"unknown field", e.job(map[string]any{"color": "red"}), 400, "invalid_request"},
		{"invalid json", `{"queue":`, 400, "invalid_json"},
		{"body too large", `{"queue":"` + strings.Repeat("a", 1<<20) + `"}`, 413, "payload_too_large"},
		{"ok", e.job(nil), 201, ""},
		{"ok delayed", e.job(map[string]any{"delay_seconds": 60, "priority": -5}), 201, ""},
	}
	for _, c := range cases {
		status, out := e.do("POST", "/v1/jobs", key, c.body)
		assert.Equal(t, c.status, status, c.name)
		assert.Equal(t, c.code, errCode(out), c.name)
	}
	status, out := e.do("POST", "/v1/jobs", "", e.job(nil))
	assert.Equal(t, 401, status)
	assert.Equal(t, "unauthorized", errCode(out))
	status, _ = e.do("GET", "/v1/jobs?state=scheduled", key, nil)
	assert.Equal(t, 200, status)
	status, _ = e.do("GET", "/v1/jobs?state=bogus", key, nil)
	assert.Equal(t, 400, status)
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv(t)
	a, b := e.tenant(), e.tenant()
	status, out := e.do("POST", "/v1/jobs", a, e.job(map[string]any{"delay_seconds": 600}))
	require.Equal(t, 201, status)
	job := out["job"].(map[string]any)
	assert.Equal(t, "scheduled", job["state"])
	assert.NotContains(t, job, "tenant_id")
	assert.NotContains(t, job, "lease_token")
	id := job["id"].(string)
	status, _ = e.do("GET", "/v1/jobs/"+id, a, nil)
	assert.Equal(t, 200, status)
	for _, p := range []string{"/v1/jobs/" + id} {
		status, out = e.do("GET", p, b, nil)
		assert.Equal(t, 404, status)
		assert.Equal(t, "not_found", errCode(out))
	}
	status, _ = e.do("POST", "/v1/jobs/"+id+"/cancel", b, nil)
	assert.Equal(t, 404, status)
	_, out = e.do("GET", "/v1/jobs", b, nil)
	assert.Empty(t, out["jobs"])
	status, _ = e.do("POST", "/v1/jobs/"+id+"/cancel", a, nil)
	assert.Equal(t, 200, status)
}

func TestPagination(t *testing.T) {
	e := newEnv(t)
	key := e.tenant()
	jobs := make([]any, 250)
	for i := range jobs {
		jobs[i] = e.job(nil)
	}
	status, _ := e.do("POST", "/v1/jobs/batch", key, map[string]any{"jobs": jobs})
	require.Equal(t, 201, status)
	seen := map[string]bool{}
	cursor, pages := "", []int{}
	for {
		_, out := e.do("GET", "/v1/jobs?limit=100&cursor="+cursor, key, nil)
		page := out["jobs"].([]any)
		pages = append(pages, len(page))
		for _, j := range page {
			seen[j.(map[string]any)["id"].(string)] = true
		}
		if out["next_cursor"] == nil {
			break
		}
		cursor = out["next_cursor"].(string)
	}
	assert.Equal(t, []int{100, 100, 50}, pages)
	assert.Len(t, seen, 250)
	status, _ = e.do("GET", "/v1/jobs?cursor=garbage", key, nil)
	assert.Equal(t, 400, status)
}

func TestIdempotency(t *testing.T) {
	e := newEnv(t)
	key := e.tenant()
	status1, out1 := e.do("POST", "/v1/jobs", key, e.job(map[string]any{"idempotency_key": "k1"}))
	status2, out2 := e.do("POST", "/v1/jobs", key, e.job(map[string]any{"idempotency_key": "k1", "priority": 9}))
	assert.Equal(t, 201, status1)
	assert.Equal(t, 200, status2)
	assert.Equal(t, out1["job"].(map[string]any)["id"], out2["job"].(map[string]any)["id"])

	var wg sync.WaitGroup
	statuses := make([]int, 10)
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i], _ = e.do("POST", "/v1/jobs", key, e.job(map[string]any{"idempotency_key": "concurrent"}))
		}()
	}
	wg.Wait()
	created := 0
	for _, s := range statuses {
		created += map[int]int{201: 1}[s]
		assert.Contains(t, []int{200, 201}, s)
	}
	assert.Equal(t, 1, created)
	var rows int
	require.NoError(t, e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE idempotency_key = 'concurrent' AND queue = $1`, e.queue).Scan(&rows))
	assert.Equal(t, 1, rows)
}

func TestBatchAllOrNothing(t *testing.T) {
	e := newEnv(t)
	key := e.tenant()
	jobs := []any{e.job(nil), e.job(nil), e.job(nil), e.job(map[string]any{"type": "bogus"}), e.job(nil)}
	status, out := e.do("POST", "/v1/jobs/batch", key, map[string]any{"jobs": jobs})
	assert.Equal(t, 400, status)
	assert.Equal(t, float64(3), out["error"].(map[string]any)["index"])
	var rows int
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE queue = $1`, e.queue).Scan(&rows))
	assert.Equal(t, 0, rows)

	jobs = []any{e.job(map[string]any{"idempotency_key": "a"}), e.job(map[string]any{"idempotency_key": "a"})}
	status, out = e.do("POST", "/v1/jobs/batch", key, map[string]any{"jobs": jobs})
	assert.Equal(t, 201, status)
	res := out["results"].([]any)
	assert.Equal(t, true, res[0].(map[string]any)["created"])
	assert.Equal(t, false, res[1].(map[string]any)["created"])
	status, _ = e.do("POST", "/v1/jobs/batch", key, map[string]any{"jobs": []any{}})
	assert.Equal(t, 400, status)
}

func TestCronEndpoints(t *testing.T) {
	e := newEnv(t)
	key := e.tenant()
	body := map[string]any{"name": "nightly", "expr": "0 3 * * *", "queue": e.queue, "type": "noop", "payload": map[string]any{}}
	status, out := e.do("POST", "/v1/cron", key, body)
	require.Equal(t, 201, status)
	id := out["schedule"].(map[string]any)["id"].(string)
	status, out = e.do("POST", "/v1/cron", key, body)
	assert.Equal(t, 409, status)
	assert.Equal(t, "duplicate_name", errCode(out))
	for _, expr := range []string{"TZ=UTC 0 3 * * *", "@every 10s", "bad"} {
		body["name"], body["expr"] = "x", expr
		status, _ = e.do("POST", "/v1/cron", key, body)
		assert.Equal(t, 400, status, expr)
	}
	status, out = e.do("PATCH", "/v1/cron/"+id, key, map[string]any{"enabled": false})
	assert.Equal(t, 200, status)
	assert.Equal(t, false, out["schedule"].(map[string]any)["enabled"])
	_, out = e.do("GET", "/v1/cron", key, nil)
	assert.Len(t, out["schedules"], 1)
	status, _ = e.do("GET", "/v1/cron/"+id, e.tenant(), nil)
	assert.Equal(t, 405, status)
	status, _ = e.do("DELETE", "/v1/cron/"+id, e.tenant(), nil)
	assert.Equal(t, 404, status, "other tenant")
	status, _ = e.do("DELETE", "/v1/cron/"+id, key, nil)
	assert.Equal(t, 204, status)
}

func TestAdminCSRF(t *testing.T) {
	e := newEnv(t)
	post := func(hx bool) int {
		req, _ := http.NewRequest("POST", e.srv.URL+"/admin/queues/"+e.queue+"/pause", nil)
		req.SetBasicAuth("admin", "pw")
		if hx {
			req.Header.Set("HX-Request", "true")
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	assert.Equal(t, 403, post(false))
	assert.Equal(t, 200, post(true))
	req, _ := http.NewRequest("GET", e.srv.URL+"/admin", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, 401, resp.StatusCode, "basic auth required")
	req.SetBasicAuth("admin", "pw")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(b), fmt.Sprintf(`/admin/queues/%s/resume`, e.queue))
}

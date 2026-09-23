// Command loadgen drives load against the API (run) and verifies chaos results in Postgres (verify).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

func main() {
	observability.SetupLogging("loadgen", "", "info")
	var err error
	switch cmd := append(os.Args, "")[1]; cmd {
	case "run":
		err = runCmd(os.Args[2:])
	case "verify":
		err = verifyCmd(os.Args[2:])
	default:
		err = fmt.Errorf("usage: loadgen run|verify [flags] (got %q)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

type client struct{ api, key string }

var httpClient = &http.Client{Timeout: 30 * time.Second}

// do sends a request, retrying 429, 5xx and network errors up to 5 times.
func (c client) do(method, path string, body, out any) error {
	b, _ := json.Marshal(body)
	var err error
	for try := 0; try < 5; try++ {
		req, _ := http.NewRequest(method, c.api+path, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+c.key)
		resp, rerr := httpClient.Do(req)
		if err = rerr; err == nil {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return json.Unmarshal(data, out)
			}
			if err = fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, data); resp.StatusCode != 429 && resp.StatusCode < 500 {
				return err
			}
			if s, _ := strconv.Atoi(resp.Header.Get("Retry-After")); s > 0 {
				time.Sleep(time.Duration(s) * time.Second)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

type jobJSON struct {
	ID        string     `json:"id"`
	RunAt     time.Time  `json:"run_at"`
	CreatedAt time.Time  `json:"created_at"`
	Started   *time.Time `json:"started_at"`
	Finished  *time.Time `json:"finished_at"`
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	slices.Sort(xs)
	return xs[min(len(xs)-1, int(p*float64(len(xs))))]
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	api := fs.String("api", "http://localhost:8081", "API base URL")
	key := fs.String("key", "", "tenant API key")
	n := fs.Int("n", 10000, "jobs to submit")
	rate := fs.Int("rate", 1000, "jobs/s, 0 = unlimited")
	conc := fs.Int("concurrency", 32, "concurrent submitters")
	typ := fs.String("type", "noop", "noop|sleep|record")
	payload := fs.String("payload", "{}", "job payload JSON")
	queue := fs.String("queue", "default", "queue")
	batch := fs.Int("batch", 0, "0 = single submits, else batch size <= 500")
	maxAttempts := fs.Int("max-attempts", 5, "max_attempts per job")
	runID := fs.String("run-id", strconv.FormatInt(time.Now().Unix(), 10), "run id")
	workers := fs.Int("workers", 0, "worker count (informational)")
	waitTimeout := fs.Duration("wait-timeout", 10*time.Minute, "completion timeout")
	out := fs.String("out", "bench/results", "results dir")
	fs.Parse(args) //nolint:errcheck
	c := client{*api, *key}

	size, chunks, ids := max(*batch, 1), make(chan []int), make([]string, *n)
	var errCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range chunks {
				objs := make([]map[string]any, len(idx))
				for k, i := range idx {
					objs[k] = map[string]any{"queue": *queue, "type": *typ, "payload": json.RawMessage(*payload),
						"max_attempts": *maxAttempts, "idempotency_key": fmt.Sprintf("run:%s:%d", *runID, i)}
				}
				var res struct {
					Job     jobJSON
					Results []struct{ Job jobJSON }
				}
				var err error
				if *batch == 0 {
					err = c.do("POST", "/v1/jobs", objs[0], &res)
					res.Results = append(res.Results, struct{ Job jobJSON }{res.Job})
				} else {
					err = c.do("POST", "/v1/jobs/batch", map[string]any{"jobs": objs}, &res)
				}
				if err != nil {
					errCount.Add(int64(len(idx)))
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				for k, r := range res.Results {
					ids[idx[k]] = r.Job.ID
				}
			}
		}()
	}
	start := time.Now()
	for i := 0; i < *n; i += size {
		if *rate > 0 {
			time.Sleep(time.Until(start.Add(time.Duration(float64(i) / float64(*rate) * float64(time.Second)))))
		}
		idx := []int{}
		for k := i; k < min(i+size, *n); k++ {
			idx = append(idx, k)
		}
		chunks <- idx
	}
	close(chunks)
	wg.Wait()
	submitSecs := time.Since(start).Seconds()

	for deadline := time.Now().Add(*waitTimeout); time.Now().Before(deadline); time.Sleep(time.Second) {
		var st struct {
			ByQueue map[string]map[string]int64 `json:"by_queue"`
		}
		open := int64(-1)
		if c.do("GET", "/v1/stats", nil, &st) == nil {
			open = 0
			for _, q := range st.ByQueue {
				open += q["scheduled"] + q["pending"] + q["running"]
			}
		}
		if open == 0 {
			break
		}
	}
	completeSecs := time.Since(start).Seconds()

	ids = slices.DeleteFunc(ids, func(s string) bool { return s == "" })
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	var mu sync.Mutex
	var waits, e2es []float64
	sem := make(chan struct{}, 32)
	for _, id := range ids[:min(len(ids), 2000)] {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			var r struct{ Job jobJSON }
			if c.do("GET", "/v1/jobs/"+id, nil, &r) != nil || r.Job.Started == nil || r.Job.Finished == nil {
				return
			}
			mu.Lock()
			waits = append(waits, float64(r.Job.Started.Sub(r.Job.RunAt).Milliseconds()))
			e2es = append(e2es, float64(r.Job.Finished.Sub(r.Job.CreatedAt).Milliseconds()))
			mu.Unlock()
		}()
	}
	wg.Wait()

	wc := os.Getenv("WORKER_CONCURRENCY")
	if wc == "" {
		wc = "16"
	}
	table := "| run_id | workers | worker_concurrency | n | type | submit_jobs_per_s | complete_jobs_per_min | wait_p50_ms | wait_p95_ms | wait_p99_ms | e2e_p50_ms | e2e_p95_ms | e2e_p99_ms | errors |\n|" +
		strings.Repeat("---|", 14) + fmt.Sprintf("\n| %s | %d | %s | %d | %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %d |\n",
		*runID, *workers, wc, *n, *typ, float64(*n)/submitSecs, float64(*n)/completeSecs*60,
		pct(waits, .5), pct(waits, .95), pct(waits, .99), pct(e2es, .5), pct(e2es, .95), pct(e2es, .99), errCount.Load())
	fmt.Print(table)
	return os.WriteFile(filepath.Join(*out, time.Now().UTC().Format(time.RFC3339)+"-"+*runID+".md"), []byte(table), 0o644)
}

const verifySQL = `WITH j AS (SELECT id, state, lease_token FROM jobs WHERE tenant_id = 'bench' AND idempotency_key LIKE $1),
ex AS (SELECT e.job_id, count(*) c FROM executions e JOIN j ON j.id = e.job_id GROUP BY 1),
rc AS (SELECT EXTRACT(EPOCH FROM c.created_at - (r.detail->>'expired_at')::timestamptz)::float8 s
  FROM job_events r JOIN j ON j.id = r.job_id CROSS JOIN LATERAL (SELECT created_at FROM job_events c
  WHERE c.job_id = r.job_id AND c.event = 'claimed' AND c.id > r.id ORDER BY c.id LIMIT 1) c WHERE r.event = 'reclaimed')
SELECT (SELECT count(*) FROM j), (SELECT count(*) FROM j WHERE state = 'succeeded'),
  (SELECT count(*) FROM j WHERE state = 'succeeded' AND NOT EXISTS
    (SELECT 1 FROM executions e WHERE e.job_id = j.id AND e.lease_token = j.lease_token)),
  (SELECT count(*) FROM ex WHERE c > 1), (SELECT COALESCE(sum(c - 1), 0) FROM ex),
  (SELECT COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY s), 0) FROM rc),
  (SELECT COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY s), 0) FROM rc), (SELECT COALESCE(max(s), 0) FROM rc)`

func verifyCmd(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	runID := fs.String("run-id", "", "run id")
	timeout := fs.Duration("timeout", 10*time.Minute, "wait timeout")
	out := fs.String("out", "bench/results", "results dir")
	fs.Parse(args) //nolint:errcheck
	ctx := context.Background()
	db, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close(ctx)
	like := "run:" + *runID + ":%"
	for deadline := time.Now().Add(*timeout); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		var open int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id = 'bench' AND idempotency_key LIKE $1
			AND state IN ('pending', 'running')`, like).Scan(&open); err != nil {
			return fmt.Errorf("poll: %w", err)
		} else if open == 0 {
			break
		}
	}
	var total, succeeded, missing, dupJobs, extra int
	var p50, p95, pmax float64
	if err := db.QueryRow(ctx, verifySQL, like).Scan(&total, &succeeded, &missing, &dupJobs, &extra, &p50, &p95, &pmax); err != nil {
		return fmt.Errorf("verify query: %w", err)
	}
	logFile, _ := os.ReadFile(filepath.Join(*out, "chaos-"+*runID+".log"))
	lost := total - succeeded
	report := fmt.Sprintf("| total | succeeded | lost | acked_execution_missing | duplicate_jobs | duplicate_rate | extra_executions | reclaim_p50_s | reclaim_p95_s | reclaim_max_s | kills |\n"+
		"|%s\n| %d | %d | %d | %d | %d | %.4f | %d | %.2f | %.2f | %.2f | %d |\n", strings.Repeat("---|", 11), total, succeeded, lost,
		missing, dupJobs, float64(dupJobs)/float64(max(total, 1)), extra, p50, p95, pmax, strings.Count(string(logFile), "\n"))
	fmt.Print(report)
	if err := os.WriteFile(filepath.Join(*out, "chaos-"+*runID+".md"), []byte(report), 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if lost > 0 || missing > 0 {
		return errors.New("FAIL: lost jobs or acked executions missing")
	}
	return nil
}

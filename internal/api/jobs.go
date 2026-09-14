package api

import (
	"context"
	"net/http"
	"slices"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

func (s *Server) queueSet(ctx context.Context) (map[string]bool, error) {
	qs, err := s.st.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(qs))
	for _, q := range qs {
		m[q.Name] = true
	}
	return m, nil
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var req submitReq
	if e := decode(r, &req); e != nil {
		writeAPIErr(w, e)
		return
	}
	s.enqueue(w, r, []submitReq{req}, false)
}

func (s *Server) submitBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Jobs []submitReq `json:"jobs"`
	}
	if e := decode(r, &body); e != nil {
		writeAPIErr(w, e)
		return
	}
	if len(body.Jobs) < 1 || len(body.Jobs) > config.BatchMax {
		writeAPIErr(w, badReq("jobs must contain 1..%d items", config.BatchMax))
		return
	}
	s.enqueue(w, r, body.Jobs, true)
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, reqs []submitReq, batch bool) {
	ctx, t := r.Context(), tenantOf(r)
	if ok, retry := s.lim.Allow(ctx, t.ID, t.Rate, t.Burst, len(reqs)); !ok {
		if retry > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retry))
		}
		writeErr(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
		return
	}
	queues, err := s.queueSet(ctx)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	jobs := make([]store.NewJob, len(reqs))
	for i, req := range reqs {
		n, e := s.validateSubmit(req, t.ID, queues, carrier["traceparent"])
		if e != nil {
			if batch {
				e.index = &i
			}
			writeAPIErr(w, e)
			return
		}
		jobs[i] = n
	}
	res, err := s.st.Enqueue(ctx, jobs)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	status := http.StatusOK
	results := make([]map[string]any, len(res))
	for i, er := range res {
		if er.Created {
			status = http.StatusCreated
			observability.JobsSubmitted.WithLabelValues(er.Job.Queue, er.Job.Type).Inc()
		}
		results[i] = map[string]any{"job": er.Job, "created": er.Created}
	}
	if batch {
		writeJSON(w, status, map[string]any{"results": results})
	} else {
		writeJSON(w, status, map[string]any{"job": res[0].Job})
	}
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		return id, model.ErrNotFound
	}
	return id, nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	job, events, err := s.st.Get(r.Context(), tenantOf(r).ID, id)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	if events == nil {
		events = []model.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "events": events})
}

// parseFilter reads state, queue, type, limit and cursor query params.
func parseFilter(r *http.Request, tenant string) (store.Filter, *apiErr) {
	q := r.URL.Query()
	f := store.Filter{TenantID: tenant, State: q.Get("state"), Queue: q.Get("queue"), Type: q.Get("type"),
		Cursor: q.Get("cursor"), Limit: config.ListDefault}
	if f.State != "" && !slices.Contains(model.States, f.State) {
		return f, badReq("state is not a valid state")
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > config.ListMax {
			return f, badReq("limit must be in 1..%d", config.ListMax)
		}
		f.Limit = n
	}
	if f.Cursor != "" {
		if _, _, err := store.DecodeCursor(f.Cursor); err != nil {
			return f, badReq("cursor is invalid")
		}
	}
	return f, nil
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	f, e := parseFilter(r, tenantOf(r).ID)
	if e != nil {
		writeAPIErr(w, e)
		return
	}
	jobs, next, err := s.st.List(r.Context(), f)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if jobs == nil {
		jobs = []model.Job{}
	}
	var nc any
	if next != "" {
		nc = next
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "next_cursor": nc})
}

func (s *Server) jobAction(w http.ResponseWriter, r *http.Request, fn func(context.Context, string, uuid.UUID) (model.Job, error)) {
	id, err := pathID(r)
	if err == nil {
		var job model.Job
		if job, err = fn(r.Context(), tenantOf(r).ID, id); err == nil {
			status := http.StatusOK
			if job.State == model.StateRunning {
				status = http.StatusAccepted
			}
			writeJSON(w, status, map[string]any{"job": job})
			return
		}
	}
	s.storeErr(w, r, err)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) { s.jobAction(w, r, s.st.Cancel) }
func (s *Server) replayJob(w http.ResponseWriter, r *http.Request) { s.jobAction(w, r, s.st.Replay) }

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.st.Stats(r.Context(), tenantOf(r).ID)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"by_queue": st})
}

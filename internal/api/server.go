// Package api is the HTTP API and admin UI .
package api

import (
	"context"
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/ratelimit"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

// Server holds the API dependencies.
type Server struct {
	cfg   config.Config
	st    *store.Postgres
	reg   *handlers.Registry
	lim   *ratelimit.Limiter
	rdb   *redis.Client
	pages map[string]*template.Template
}

// New builds a Server.
func New(cfg config.Config, st *store.Postgres, reg *handlers.Registry, rdb *redis.Client) (*Server, error) {
	pages, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	for _, t := range cfg.SeedTenants { // export a zero series per tenant from the start
		observability.RateLimited.WithLabelValues(t.ID)
	}
	return &Server{cfg: cfg, st: st, reg: reg, lim: ratelimit.New(rdb), rdb: rdb, pages: pages}, nil
}

type ctxKey int

const (
	keyInfo ctxKey = iota
	keyTenant
)

// reqInfo lets inner middleware report the tenant to the request log.
type reqInfo struct{ tenant string }

// Router builds the chi router . Middleware order matters: the request log
// and metrics wrap auth so they see every response, including 401s.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(func(next http.Handler) http.Handler { return otelhttp.NewHandler(next, "http") })
	r.Use(observe)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }) //nolint:errcheck
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", promhttp.Handler())
	r.Route("/v1", func(r chi.Router) {
		r.Use(bodyLimit, s.auth)
		r.Post("/jobs", s.submit)
		r.Post("/jobs/batch", s.submitBatch)
		r.Get("/jobs", s.listJobs)
		r.Get("/jobs/{id}", s.getJob)
		r.Post("/jobs/{id}/cancel", s.cancelJob)
		r.Post("/jobs/{id}/replay", s.replayJob)
		r.Post("/cron", s.createCron)
		r.Get("/cron", s.listCron)
		r.Patch("/cron/{id}", s.patchCron)
		r.Delete("/cron/{id}", s.deleteCron)
		r.Get("/stats", s.stats)
	})
	r.Route("/admin", s.adminRoutes)
	return r
}

// observe records metrics and the request log once the route pattern is known.
func observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, info := time.Now(), &reqInfo{}
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r.WithContext(context.WithValue(r.Context(), keyInfo, info)))
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		trace.SpanFromContext(r.Context()).SetName(r.Method + " " + route)
		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		observability.HTTPRequests.WithLabelValues(route, strconv.Itoa(status)).Inc()
		observability.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		slog.InfoContext(r.Context(), "http request", "request_id", middleware.GetReqID(r.Context()),
			"tenant_id", info.tenant, "method", r.Method, "route", route, "status", status,
			"duration_ms", time.Since(start).Milliseconds(), "trace_id", observability.TraceID(r.Context()))
	})
}

func bodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := int64(config.SubmitBodyLimit)
		if strings.HasSuffix(r.URL.Path, "/batch") {
			limit = config.BatchBodyLimit
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		t, err := s.st.TenantByKey(r.Context(), key)
		if !ok || key == "" || errors.Is(err, model.ErrNotFound) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeErr(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
			return
		} else if err != nil {
			s.internal(w, r, err)
			return
		}
		if info, ok := r.Context().Value(keyInfo).(*reqInfo); ok {
			info.tenant = t.ID
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), keyTenant, t)))
	})
}

func tenantOf(r *http.Request) store.Tenant { return r.Context().Value(keyTenant).(store.Tenant) }

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.st.Pool().Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"postgres": "error"})
		return
	}
	redisState := "ok"
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		redisState = "error"
	}
	writeJSON(w, http.StatusOK, map[string]string{"postgres": "ok", "redis": redisState})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

type apiErr struct {
	status        int
	code, message string
	index         *int
}

func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeAPIErr(w, &apiErr{status: status, code: code, message: message})
}

func writeAPIErr(w http.ResponseWriter, e *apiErr) {
	body := map[string]any{"code": e.code, "message": e.message}
	if e.index != nil {
		body["index"] = *e.index
	}
	writeJSON(w, e.status, map[string]any{"error": body})
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "internal error", "request_id", middleware.GetReqID(r.Context()), "error", err)
	writeErr(w, http.StatusInternalServerError, "internal", "internal error")
}

// storeErr maps sentinel errors to the envelope.
func (s *Server) storeErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, model.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, model.ErrInvalidState):
		writeErr(w, http.StatusConflict, "invalid_state", "job is not in a state that allows this")
	case errors.Is(err, model.ErrDuplicateName):
		writeErr(w, http.StatusConflict, "duplicate_name", "a schedule with this name exists")
	default:
		s.internal(w, r, err)
	}
}

// decode reads a JSON body with DisallowUnknownFields.
func decode(r *http.Request, v any) *apiErr {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	err := d.Decode(v)
	if err == nil && d.More() {
		err = errors.New("trailing data after JSON value")
	}
	var mbe *http.MaxBytesError
	var ute *json.UnmarshalTypeError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &mbe):
		return &apiErr{status: http.StatusRequestEntityTooLarge, code: "payload_too_large", message: "request body too large"}
	case errors.As(err, &ute):
		return &apiErr{status: http.StatusBadRequest, code: "invalid_request", message: ute.Field + ": wrong type"}
	case strings.HasPrefix(err.Error(), "json: unknown field"):
		return &apiErr{status: http.StatusBadRequest, code: "invalid_request", message: err.Error()}
	}
	return &apiErr{status: http.StatusBadRequest, code: "invalid_json", message: err.Error()}
}

package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/web"
)

func loadTemplates() (map[string]*template.Template, error) {
	pages := map[string]*template.Template{}
	for _, name := range []string{"dashboard", "jobs", "job"} {
		t, err := template.ParseFS(web.FS, "templates/layout.html", "templates/"+name+".html")
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", name, err)
		}
		pages[name] = t
	}
	return pages, nil
}

func (s *Server) adminRoutes(r chi.Router) {
	r.Use(s.basicAuth, csrf)
	r.Get("/", s.dashboard)
	r.Get("/jobs", s.adminJobs)
	r.Get("/jobs/{id}", s.adminJob)
	r.Post("/jobs/{id}/cancel", s.adminAction(s.st.Cancel))
	r.Post("/jobs/{id}/replay", s.adminAction(s.st.Replay))
	r.Post("/dead/replay", s.bulkReplay)
	r.Post("/queues/{name}/pause", s.pause(true))
	r.Post("/queues/{name}/resume", s.pause(false))
	r.Handle("/static/*", http.StripPrefix("/admin/", http.FileServerFS(web.FS)))
}

func (s *Server) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(u), []byte(s.cfg.AdminUser)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(s.cfg.AdminPassword)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="admin"`)
			writeErr(w, http.StatusUnauthorized, "unauthorized", "admin credentials required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// csrf requires HX-Request: true on every non-GET request.
func csrf(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Header.Get("HX-Request") != "true" {
			writeErr(w, http.StatusForbidden, "forbidden", "HX-Request header required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// render writes the full page, or only the fragment for ?partial=1 and POSTs.
func (s *Server) render(w http.ResponseWriter, r *http.Request, page, tmpl string, data any) {
	if tmpl == "" {
		tmpl = "layout"
		if r.URL.Query().Get("partial") == "1" || r.Method != http.MethodGet {
			tmpl = "content"
		}
	}
	var buf bytes.Buffer
	if err := s.pages[page].ExecuteTemplate(&buf, tmpl, data); err != nil {
		s.internal(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf.WriteTo(w) //nolint:errcheck
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err1 := s.st.Stats(ctx, "")
	queues, err2 := s.st.ListQueues(ctx)
	leader, err3 := s.st.LeaderID(ctx)
	if err := errors.Join(err1, err2, err3); err != nil {
		s.internal(w, r, err)
		return
	}
	s.render(w, r, "dashboard", "", map[string]any{"Stats": stats, "Queues": queues, "Leader": leader, "States": model.States})
}

func (s *Server) adminJobs(w http.ResponseWriter, r *http.Request) {
	f, e := parseFilter(r, r.URL.Query().Get("tenant"))
	if e != nil {
		writeAPIErr(w, e)
		return
	}
	jobs, next, err := s.st.List(r.Context(), f)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	s.render(w, r, "jobs", "", map[string]any{"Jobs": jobs, "Next": next, "F": f, "States": model.States})
}

func pretty(raw json.RawMessage) string {
	var buf bytes.Buffer
	if json.Indent(&buf, raw, "", "  ") != nil {
		return string(raw)
	}
	return buf.String()
}

func (s *Server) renderJob(w http.ResponseWriter, r *http.Request, id uuid.UUID, msg string) {
	job, events, err := s.st.Get(r.Context(), "", id)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	s.render(w, r, "job", "", map[string]any{"Job": job, "Events": events, "Msg": msg,
		"Payload": pretty(job.Payload), "Result": pretty(job.Result)})
}

func (s *Server) adminJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	s.renderJob(w, r, id, "")
}

func (s *Server) adminAction(fn func(context.Context, string, uuid.UUID) (model.Job, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathID(r)
		if err == nil {
			_, err = fn(r.Context(), "", id)
		}
		msg := ""
		if errors.Is(err, model.ErrInvalidState) {
			msg = "Not allowed in the current state."
		} else if err != nil {
			s.storeErr(w, r, err)
			return
		}
		s.renderJob(w, r, id, msg)
	}
}

func (s *Server) bulkReplay(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || len(r.PostForm["ids"]) > 100 {
		writeAPIErr(w, badReq("ids: at most 100 ids"))
		return
	}
	var counts struct{ OK, Failed int }
	for _, raw := range r.PostForm["ids"] {
		id, err := uuid.Parse(raw)
		if err == nil {
			_, err = s.st.Replay(r.Context(), "", id)
		}
		if err != nil {
			counts.Failed++
		} else {
			counts.OK++
		}
	}
	s.render(w, r, "jobs", "replayed", counts)
}

func (s *Server) pause(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.st.SetQueuePaused(r.Context(), chi.URLParam(r, "name"), paused); err != nil {
			s.storeErr(w, r, err)
			return
		}
		s.dashboard(w, r)
	}
}

package api

import (
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/Himanshuu2125/pgqueue/internal/store"
)

var cronNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func (s *Server) createCron(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string          `json:"name"`
		Expr    string          `json:"expr"`
		Queue   string          `json:"queue"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
		Enabled *bool           `json:"enabled"`
	}
	if e := decode(r, &req); e != nil {
		writeAPIErr(w, e)
		return
	}
	if !cronNameRe.MatchString(req.Name) {
		writeAPIErr(w, badReq("name must match ^[a-zA-Z0-9_-]{1,64}$"))
		return
	}
	if _, err := store.ParseCron(req.Expr); err != nil {
		writeAPIErr(w, badReq("expr: %v", err))
		return
	}
	queues, err := s.queueSet(r.Context())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	t := tenantOf(r)
	// Reuse submit validation for queue, type and payload.
	if _, e := s.validateSubmit(submitReq{Queue: req.Queue, Type: req.Type, Payload: req.Payload}, t.ID, queues, ""); e != nil {
		writeAPIErr(w, e)
		return
	}
	c, err := s.st.CreateCron(r.Context(), store.Cron{TenantID: t.ID, Name: req.Name, Expr: req.Expr, Queue: req.Queue,
		Type: req.Type, Payload: req.Payload, Enabled: req.Enabled == nil || *req.Enabled})
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"schedule": c})
}

func (s *Server) listCron(w http.ResponseWriter, r *http.Request) {
	cs, err := s.st.ListCron(r.Context(), tenantOf(r).ID)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if cs == nil {
		cs = []store.Cron{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": cs})
}

func (s *Server) patchCron(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if e := decode(r, &req); e != nil {
		writeAPIErr(w, e)
		return
	}
	if req.Enabled == nil {
		writeAPIErr(w, badReq("enabled is required"))
		return
	}
	id, err := pathID(r)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	c, err := s.st.SetCronEnabled(r.Context(), tenantOf(r).ID, id, *req.Enabled)
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": c})
}

func (s *Server) deleteCron(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err == nil {
		err = s.st.DeleteCron(r.Context(), tenantOf(r).ID, id)
	}
	if err != nil {
		s.storeErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

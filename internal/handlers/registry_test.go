package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
)

func TestValidate(t *testing.T) {
	r := NewRegistry(nil, nil, config.Handlers{AllowedHosts: []string{"hooks.internal"}})
	cases := []struct {
		typ, payload string
		ok           bool
	}{
		{"noop", `{}`, true},
		{"noop", `{"x":1}`, false},
		{"sleep", `{"ms":10}`, true},
		{"sleep", `{}`, false},
		{"sleep", `{"ms":600001}`, false},
		{"sleep", `{"ms":-1}`, false},
		{"fail", `{"rate":0.5}`, true},
		{"fail", `{"rate":1.5}`, false},
		{"fail", `{}`, false},
		{"record", `{}`, true},
		{"record", `{"ms":60000}`, true},
		{"record", `{"ms":60001}`, false},
		{"http", `{"url":"http://hooks.internal:8000/x"}`, true},
		{"http", `{"url":"https://HOOKS.INTERNAL/x","method":"PUT","headers":{"X-A":"b"},"body":{"a":[1]}}`, true},
		{"http", `{"url":"http://hooks.internal.evil.com/x"}`, false},
		{"http", `{"url":"http://evil/x"}`, false},
		{"http", `{"url":"ftp://hooks.internal/x"}`, false},
		{"http", `{"url":"http://hooks.internal/x","method":"PATCH"}`, false},
		{"http", `{"url":"http://hooks.internal/x","extra":1}`, false},
		{"unknown", `{}`, false},
	}
	for _, c := range cases {
		err := r.Validate(c.typ, json.RawMessage(c.payload))
		assert.Equal(t, c.ok, err == nil, "%s %s: %v", c.typ, c.payload, err)
	}
	for _, h := range []string{"Authorization", "x-queue-token", "HOST", "X-Job-Id", "X-Lease-Token", "X-Attempt", "Traceparent"} {
		err := r.Validate("http", json.RawMessage(`{"url":"http://hooks.internal/x","headers":{"`+h+`":"v"}}`))
		assert.Error(t, err, h)
	}
}

func TestHTTPHandler(t *testing.T) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var got http.Header
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		b := make([]byte, 100)
		n, _ := r.Body.Read(b)
		gotBody = string(b[:n])
		switch r.URL.Path {
		case "/ok":
			w.Write([]byte("fine")) //nolint:errcheck
		case "/missing":
			http.Error(w, "nope", http.StatusNotFound)
		case "/busy":
			http.Error(w, "later", http.StatusServiceUnavailable)
		case "/redirect":
			http.Redirect(w, r, "/ok", http.StatusFound)
		}
	}))
	defer srv.Close()
	reg := NewRegistry(nil, &http.Client{}, config.Handlers{AllowedHosts: []string{"127.0.0.1"}, QueueToken: "secret-token"})
	tok := int64(42)
	run := func(path string) (json.RawMessage, error) {
		job := model.Job{ID: uuid.New(), Type: "http", Attempts: 2, LeaseToken: &tok,
			Payload: json.RawMessage(`{"url":"` + srv.URL + path + `","body":{"doc":"1"},"headers":{"X-Custom":"c"}}`)}
		ctx, span := otel.Tracer("test").Start(context.Background(), "job.execute")
		defer span.End()
		return reg.Run(ctx, job)
	}

	res, err := run("/ok")
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":200,"body":"fine"}`, string(res))
	assert.Equal(t, "42", got.Get("X-Lease-Token"))
	assert.Equal(t, "2", got.Get("X-Attempt"))
	assert.NotEmpty(t, got.Get("X-Job-Id"))
	assert.Equal(t, "secret-token", got.Get("X-Queue-Token"))
	assert.NotEmpty(t, got.Get("traceparent"))
	assert.Equal(t, "c", got.Get("X-Custom"))
	assert.Equal(t, "application/json", got.Get("Content-Type"))
	assert.JSONEq(t, `{"doc":"1"}`, gotBody)

	_, err = run("/missing")
	require.Error(t, err)
	assert.True(t, IsPermanent(err), "404 is permanent")
	assert.Contains(t, err.Error(), "http 404: nope")

	_, err = run("/busy")
	require.Error(t, err)
	assert.False(t, IsPermanent(err), "503 is retried")

	_, err = run("/redirect")
	require.Error(t, err, "3xx is a failure")
	assert.False(t, IsPermanent(err))
}

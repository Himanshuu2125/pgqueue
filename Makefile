.PHONY: up down test-unit test lint bench chaos kind-up kind-down

BENCH_KEY ?= bench-key-change-me

up:
	docker compose up -d --build

down:
	docker compose down -v

test-unit:
	go test ./... -short

test:
	go test ./... -race -count=1

lint:
	go vet ./... && staticcheck ./...

# Scale workers 1, 2, 4, 8 and collect one loadgen row per run.
bench:
	printf '| run_id | workers | worker_concurrency | n | type | submit_jobs_per_s | complete_jobs_per_min | wait_p50_ms | wait_p95_ms | wait_p99_ms | e2e_p50_ms | e2e_p95_ms | e2e_p99_ms | errors |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n' > bench/results/scaling.md
	for n in 1 2 4 8 16; do \
		OTEL_TRACES_SAMPLER_RATIO=0.01 docker compose up -d --scale worker=$$n && sleep 10 && \
		go run ./cmd/loadgen run -key $(BENCH_KEY) -n 20000 -rate 0 -type sleep -payload '{"ms":50}' -workers $$n | tail -1 >> bench/results/scaling.md || exit 1; \
	done
	cat bench/results/scaling.md

chaos:
	scripts/chaos.sh

kind-up:
	kind create cluster --name jq
	docker build --build-arg CMD=api -t jobqueue-api:dev .
	docker build --build-arg CMD=worker -t jobqueue-worker:dev .
	kind load docker-image jobqueue-api:dev jobqueue-worker:dev --name jq
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/

kind-down:
	kind delete cluster --name jq

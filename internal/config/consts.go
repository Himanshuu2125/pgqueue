package config

import "time"

// Fixed tunables that are deliberately not configurable.
const (
	LeaderLockID          = 42
	ClaimBatchMax         = 10
	LeaderRetryInterval   = 5 * time.Second
	CronTick              = 1 * time.Second
	GaugeInterval         = 5 * time.Second
	EventsOnGet           = 20
	BatchMax              = 500
	ListMax               = 100
	ListDefault           = 50
	ReaperBatch           = 500
	HTTPBodyReadCap       = 64 << 10
	ResultBodyCap         = 4096
	LastErrorCap          = 2048
	MaxPayloadBytes       = 256 << 10
	SubmitBodyLimit       = 1 << 20
	BatchBodyLimit        = 16 << 20
	RateLimitRedisTimeout = 100 * time.Millisecond
	ShutdownExtraWait     = 2 * time.Second
)

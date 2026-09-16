package worker

import (
	"math/rand/v2"
	"time"
)

// Backoff returns a full-jitter delay uniform in [0, min(max, base·2^(attempts−1))].
func Backoff(r *rand.Rand, attempts int, base, maxDelay time.Duration) time.Duration {
	shift := min(max(attempts-1, 0), 30)
	capMS := min(maxDelay.Milliseconds(), base.Milliseconds()<<shift)
	return time.Duration(r.Int64N(capMS+1)) * time.Millisecond
}

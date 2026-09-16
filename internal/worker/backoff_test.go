package worker

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffBounds(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	base, maxD := time.Second, 300*time.Second
	for i := 0; i < 10000; i++ {
		a := i%25 + 1
		d := Backoff(r, a, base, maxD)
		limit := min(maxD, base*time.Duration(1<<min(a-1, 30)))
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.LessOrEqual(t, d, limit, "attempts=%d", a)
	}
	for i := 0; i < 1000; i++ {
		d := Backoff(r, 25, base, maxD) // no overflow at attempts=25
		require.True(t, d >= 0 && d <= maxD)
	}
	require.Equal(t, time.Duration(0), Backoff(r, 1, 0, 0))
}

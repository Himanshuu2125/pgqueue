package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.FixedZone("x", 3600))
	id := uuid.New()
	gotTS, gotID, err := DecodeCursor(EncodeCursor(ts, id))
	require.NoError(t, err)
	assert.True(t, ts.Equal(gotTS))
	assert.Equal(t, id, gotID)
	for _, bad := range []string{"!!!", "", "bm9waXBl", EncodeCursor(ts, id)[:10]} {
		_, _, err := DecodeCursor(bad)
		assert.Error(t, err, bad)
	}
}

func TestCronNextUTC(t *testing.T) {
	s, err := ParseCron("30 2 * * *")
	require.NoError(t, err)
	now := time.Date(2026, 3, 1, 23, 0, 0, 0, time.FixedZone("IST", 5*3600+1800)) // 17:30 UTC
	next := NextRun(s, now)
	assert.Equal(t, time.UTC, next.Location())
	assert.Equal(t, time.Date(2026, 3, 2, 2, 30, 0, 0, time.UTC), next)

	s, err = ParseCron("@hourly")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 3, 1, 18, 0, 0, 0, time.UTC), NextRun(s, now))
	_, err = ParseCron("@every 5m")
	assert.NoError(t, err)
}

func TestCronRejects(t *testing.T) {
	for _, expr := range []string{"TZ=Asia/Kolkata 0 * * * *", "CRON_TZ=UTC 0 * * * *", "@every 30s", "@every 59s", "* * *", "not cron"} {
		_, err := ParseCron(expr)
		assert.Error(t, err, expr)
	}
}

func TestClean(t *testing.T) {
	assert.Equal(t, "ab", clean("a\x00b", 10))
	s := clean("héllo", 2) // cut inside the 2-byte é
	assert.Equal(t, "h", s)
}

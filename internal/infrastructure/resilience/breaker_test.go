package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
)

func TestExecuteWithBreaker(t *testing.T) {
	// Create a fast breaker for testing
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "TestBreaker",
		MaxRequests: 1,
		Interval:    time.Second,
		Timeout:     10 * time.Millisecond, // very short timeout before Half-Open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2 // Trip after 2 failures
		},
	})
	
	ctx := context.Background()

	t.Run("Success Function", func(t *testing.T) {
		res, err := ExecuteWithBreaker(ctx, cb, "TestOperation", func() (string, error) {
			return "hello", nil
		})

		assert.NoError(t, err)
		assert.Equal(t, "hello", res)
		assert.Equal(t, gobreaker.StateClosed, cb.State())
	})

	t.Run("Fail Function & Trip", func(t *testing.T) {
		failCount := 0
		errFunc := func() (int, error) {
			failCount++
			return 0, errors.New("simulated error")
		}

		// First failure
		_, err1 := ExecuteWithBreaker(ctx, cb, "TestError", errFunc)
		assert.Error(t, err1)
		assert.Equal(t, gobreaker.StateClosed, cb.State()) // still closed because 1 < 2

		// Second failure -> Trips the breaker
		_, err2 := ExecuteWithBreaker(ctx, cb, "TestError", errFunc)
		assert.Error(t, err2)
		assert.Equal(t, gobreaker.StateOpen, cb.State()) // Now it's OPEN

		// Third call should fail instantly without calling errFunc
		_, err3 := ExecuteWithBreaker(ctx, cb, "TestError", errFunc)
		assert.Error(t, err3)
		assert.Contains(t, err3.Error(), "circuit breaker")
		assert.Equal(t, 2, failCount) // Did not increment because it was blocked by breaker
	})
}

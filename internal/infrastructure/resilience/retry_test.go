package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryWithBackoff_SuccessAfterRetry(t *testing.T) {
	attempt := 0
	maxAttempts := 3

	operation := func() error {
		attempt++
		if attempt < maxAttempts {
			return errors.New("transient error")
		}
		return nil
	}

	ctx := context.Background()
	err := RetryWithBackoff(ctx, operation, 5, 1*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, maxAttempts, attempt)
}

func TestRetryWithBackoff_AllAttemptsFailed(t *testing.T) {
	attempt := 0
	expectedErr := errors.New("persistent error")

	operation := func() error {
		attempt++
		return expectedErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := RetryWithBackoff(ctx, operation, 10, 1*time.Millisecond)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persistent error")
	assert.GreaterOrEqual(t, attempt, 1)
}

func TestRetryWithBackoff_FirstAttemptSuccess(t *testing.T) {
	attempt := 0

	operation := func() error {
		attempt++
		return nil
	}

	err := RetryWithBackoff(context.Background(), operation, 3, 1*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, 1, attempt)
}

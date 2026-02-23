package resilience

import (
	"context"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
)

func RetryWithBackoff(ctx context.Context, operation func() error, maxRetries int, initialInterval time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = initialInterval
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 30 * time.Second

	notify := func(err error, duration time.Duration) {
		log.Printf("Retry attempt after %v: %v", duration, err)
	}

	err := backoff.RetryNotify(operation, bo, notify)
	if err != nil {
		return err
	}

	return nil
}

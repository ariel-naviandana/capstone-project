package resilience

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
)

func RetryWithBackoff(ctx context.Context, operation func() error, maxRetries int, initialInterval time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = initialInterval
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 30 * time.Second

	notify := func(err error, duration time.Duration) {
		log.Warn().
			Err(err).
			Dur("duration", duration).
			Msg("Retry attempt")
	}

	err := backoff.RetryNotify(operation, bo, notify)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("Retry failed after max attempts")
		return err
	}

	return nil
}

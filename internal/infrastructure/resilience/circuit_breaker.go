package resilience

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
)

var (
	KafkaProducerBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "KafkaProducer",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 2 },
		Timeout:     5 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Info().
				Str("breaker_name", name).
				Str("from_state", from.String()).
				Str("to_state", to.String()).
				Msg("Circuit Breaker state changed")
		},
	})

	KafkaConsumerBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "KafkaConsumer",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 5 },
		Timeout:     10 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Info().
				Str("breaker_name", name).
				Str("from_state", from.String()).
				Str("to_state", to.String()).
				Msg("Circuit Breaker state changed")
		},
	})

	PostgresBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "Postgres",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 3 },
		Timeout:     10 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Info().
				Str("breaker_name", name).
				Str("from_state", from.String()).
				Str("to_state", to.String()).
				Msg("Circuit Breaker state changed")
		},
	})

	MongoBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "Mongo",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 3 },
		Timeout:     5 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Info().
				Str("breaker_name", name).
				Str("from_state", from.String()).
				Str("to_state", to.String()).
				Msg("Circuit Breaker state changed")
		},
	})
)

func ExecuteWithBreaker[T any](ctx context.Context, breaker *gobreaker.CircuitBreaker, name string, fn func() (T, error)) (T, error) {
	log.Debug().
		Str("breaker_name", name).
		Msg("Executing breaker")

	result, err := breaker.Execute(func() (interface{}, error) {
		return fn()
	})
	if err != nil {
		log.Warn().
			Err(err).
			Str("breaker_name", name).
			Msg("Circuit Breaker rejected")
		var zero T
		return zero, err
	}

	val, ok := result.(T)
	if !ok {
		log.Error().
			Str("breaker_name", name).
			Msg("Type assertion failed for breaker result")
		var zero T
		return zero, fmt.Errorf("type assertion failed")
	}

	return val, nil
}

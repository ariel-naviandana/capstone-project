package resilience

import (
	"context"
	"fmt"
	"log"
	"time"

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
			log.Printf("Circuit Breaker %s changed from %s to %s", name, from, to)
		},
	})

	KafkaConsumerBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "KafkaConsumer",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 5 },
		Timeout:     10 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Printf("Circuit Breaker %s changed from %s to %s", name, from, to)
		},
	})
)

func ExecuteWithBreaker[T any](ctx context.Context, breaker *gobreaker.CircuitBreaker, name string, fn func() (T, error)) (T, error) {
	log.Printf("Executing breaker %s", name)
	result, err := breaker.Execute(func() (interface{}, error) {
		return fn()
	})
	if err != nil {
		log.Printf("Circuit Breaker %s rejected: %v", name, err)
		var zero T
		return zero, err
	}
	val, ok := result.(T)
	if !ok {
		log.Printf("Type assertion failed for breaker %s", name)
		var zero T
		return zero, fmt.Errorf("type assertion failed")
	}
	return val, nil
}

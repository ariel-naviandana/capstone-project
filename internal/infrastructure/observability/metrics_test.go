package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsRegistered(t *testing.T) {
	// The init() function in metrics.go already registers all metrics.
	// This test verifies that the metric variables are not nil after init.
	t.Run("BreakerState is registered", func(t *testing.T) {
		assert.NotNil(t, BreakerState)
	})

	t.Run("BreakerTripsTotal is registered", func(t *testing.T) {
		assert.NotNil(t, BreakerTripsTotal)
	})

	t.Run("CacheHitsTotal is registered", func(t *testing.T) {
		assert.NotNil(t, CacheHitsTotal)
	})

	t.Run("CacheMissesTotal is registered", func(t *testing.T) {
		assert.NotNil(t, CacheMissesTotal)
	})
}

func TestBreakerStateLabels(t *testing.T) {
	// Verify that we can get metrics with the expected label values
	labels := []string{"KafkaProducer", "KafkaConsumer", "Postgres", "Mongo"}

	for _, label := range labels {
		t.Run("BreakerState_"+label, func(t *testing.T) {
			metric := BreakerState.WithLabelValues(label)
			assert.NotNil(t, metric)
		})

		t.Run("BreakerTripsTotal_"+label, func(t *testing.T) {
			metric := BreakerTripsTotal.WithLabelValues(label)
			assert.NotNil(t, metric)
		})
	}
}

func TestCacheMetricsLabels(t *testing.T) {
	t.Run("CacheHitsTotal redis label", func(t *testing.T) {
		metric := CacheHitsTotal.WithLabelValues("redis")
		assert.NotNil(t, metric)
	})

	t.Run("CacheMissesTotal redis label", func(t *testing.T) {
		metric := CacheMissesTotal.WithLabelValues("redis")
		assert.NotNil(t, metric)
	})
}

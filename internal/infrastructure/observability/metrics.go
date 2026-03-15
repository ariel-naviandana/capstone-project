package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	BreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "breaker_state",
			Help: "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{"breaker_name"},
	)

	BreakerTripsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "breaker_trips_total",
			Help: "Total number of circuit breaker trips",
		},
		[]string{"breaker_name"},
	)

	CacheHitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total cache hits",
		},
		[]string{"cache_type"},
	)

	CacheMissesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total cache misses",
		},
		[]string{"cache_type"},
	)
)

func init() {
	prometheus.MustRegister(BreakerState)
	prometheus.MustRegister(BreakerTripsTotal)
	prometheus.MustRegister(CacheHitsTotal)
	prometheus.MustRegister(CacheMissesTotal)

	BreakerState.WithLabelValues("KafkaProducer").Set(0)
	BreakerState.WithLabelValues("KafkaConsumer").Set(0)
	BreakerState.WithLabelValues("Postgres").Set(0)
	BreakerState.WithLabelValues("Mongo").Set(0)

	BreakerTripsTotal.WithLabelValues("KafkaProducer").Add(0)
	BreakerTripsTotal.WithLabelValues("KafkaConsumer").Add(0)
	BreakerTripsTotal.WithLabelValues("Postgres").Add(0)
	BreakerTripsTotal.WithLabelValues("Mongo").Add(0)

	CacheHitsTotal.WithLabelValues("redis").Add(0)
	CacheMissesTotal.WithLabelValues("redis").Add(0)
}

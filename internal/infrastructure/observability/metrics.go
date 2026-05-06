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

	// PgBouncer pool gauges
	PgBouncerActiveConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgbouncer_pool_active_connections",
			Help: "Number of active server connections in PgBouncer pool",
		},
		[]string{"database", "user"},
	)

	PgBouncerWaitingConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgbouncer_pool_waiting_connections",
			Help: "Number of waiting client connections in PgBouncer pool",
		},
		[]string{"database", "user"},
	)

	PgBouncerIdleConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgbouncer_pool_idle_connections",
			Help: "Number of idle server connections in PgBouncer pool",
		},
		[]string{"database", "user"},
	)

	// pgxpool stats gauges
	PgxpoolAcquiredConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgxpool_acquired_connections",
			Help: "Number of currently acquired pgxpool connections",
		},
		[]string{"pool"},
	)

	PgxpoolIdleConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgxpool_idle_connections",
			Help: "Number of idle pgxpool connections",
		},
		[]string{"pool"},
	)

	PgxpoolTotalConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgxpool_total_connections",
			Help: "Total pgxpool connections (idle + acquired + constructing)",
		},
		[]string{"pool"},
	)
	// RequestsRejectedTotal counts requests rejected by protective middleware,
	// labeled by the reason so a Grafana panel can show whether traffic is
	// being blocked by the rate limiter, the DDoS shield, or a circuit breaker.
	RequestsRejectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "requests_rejected_total",
			Help: "Total requests rejected before reaching business logic, by reason",
		},
		[]string{"reason", "path"},
	)
)

func init() {
	prometheus.MustRegister(BreakerState)
	prometheus.MustRegister(BreakerTripsTotal)
	prometheus.MustRegister(CacheHitsTotal)
	prometheus.MustRegister(CacheMissesTotal)
	prometheus.MustRegister(PgBouncerActiveConns)
	prometheus.MustRegister(PgBouncerWaitingConns)
	prometheus.MustRegister(PgBouncerIdleConns)
	prometheus.MustRegister(PgxpoolAcquiredConns)
	prometheus.MustRegister(PgxpoolIdleConns)
	prometheus.MustRegister(PgxpoolTotalConns)
	prometheus.MustRegister(RequestsRejectedTotal)

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

package observability

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// StartPgBouncerMetrics starts a background goroutine that polls PgBouncer's
// admin DB every 15 seconds and publishes pool stats to Prometheus.
// adminDSN must point to: postgres://<user>:<pass>@pgbouncer:6432/pgbouncer
func StartPgBouncerMetrics(ctx context.Context, adminDSN string, writePool, readPool *pgxpool.Pool) {
	go func() {
		adminCfg, err := pgxpool.ParseConfig(adminDSN)
		if err != nil {
			log.Warn().Err(err).Msg("PgBouncer metrics: failed to parse admin DSN — metrics disabled")
			return
		}
		adminCfg.MaxConns = 2
		adminCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		adminPool, err := pgxpool.NewWithConfig(ctx, adminCfg)
		if err != nil {
			log.Warn().Err(err).Msg("PgBouncer metrics: failed to connect to admin DB — metrics disabled")
			return
		}
		defer adminPool.Close()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		log.Info().Msg("PgBouncer metrics collector started")

		for {
			select {
			case <-ticker.C:
				collectPgBouncerStats(ctx, adminPool)
				collectPgxpoolStats(writePool, readPool)
			case <-ctx.Done():
				log.Info().Msg("PgBouncer metrics collector stopped")
				return
			}
		}
	}()
}

func collectPgBouncerStats(ctx context.Context, adminPool *pgxpool.Pool) {
	collectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := adminPool.Query(collectCtx, "SHOW POOLS")
	if err != nil {
		log.Warn().Err(err).Msg("PgBouncer metrics: SHOW POOLS failed")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var database, user string
		var clActive, clWaiting, svActive, svIdle, svUsed, svTested, svLogin, maxWait int
		var maxWaitUs float64
		var poolMode string

		if err := rows.Scan(&database, &user, &clActive, &clWaiting,
			&svActive, &svIdle, &svUsed, &svTested, &svLogin,
			&maxWait, &maxWaitUs, &poolMode); err != nil {
			// Column count may vary by PgBouncer version — skip on scan error
			continue
		}

		PgBouncerActiveConns.WithLabelValues(database, user).Set(float64(svActive))
		PgBouncerWaitingConns.WithLabelValues(database, user).Set(float64(clWaiting))
		PgBouncerIdleConns.WithLabelValues(database, user).Set(float64(svIdle))
	}
}

func collectPgxpoolStats(writePool, readPool *pgxpool.Pool) {
	if writePool != nil {
		s := writePool.Stat()
		PgxpoolAcquiredConns.WithLabelValues("write").Set(float64(s.AcquiredConns()))
		PgxpoolIdleConns.WithLabelValues("write").Set(float64(s.IdleConns()))
		PgxpoolTotalConns.WithLabelValues("write").Set(float64(s.TotalConns()))
	}
	if readPool != nil {
		s := readPool.Stat()
		PgxpoolAcquiredConns.WithLabelValues("read").Set(float64(s.AcquiredConns()))
		PgxpoolIdleConns.WithLabelValues("read").Set(float64(s.IdleConns()))
		PgxpoolTotalConns.WithLabelValues("read").Set(float64(s.TotalConns()))
	}
}

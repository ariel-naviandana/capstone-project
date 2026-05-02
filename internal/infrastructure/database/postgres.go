package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var WritePool *pgxpool.Pool
var ReadPool *pgxpool.Pool

func ConnectPostgres() {
	primaryDSN := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable&pool_max_conns=100&pool_min_conns=2&pool_max_conn_idle_time=5m",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		config.AppConfig.PgBouncerAddr,
		"capstone",
	)

	replicaDSN := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable&pool_max_conns=100&pool_min_conns=2&pool_max_conn_idle_time=5m",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		config.AppConfig.PgBouncerAddr,
		"capstone_read",
	)

	wPoolConfig, err := pgxpool.ParseConfig(primaryDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to parse PostgreSQL Primary DSN")
	}
	wPoolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	wPool, err := pgxpool.NewWithConfig(context.Background(), wPoolConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Primary via PgBouncer")
	}
	WritePool = wPool

	rPoolConfig, err := pgxpool.ParseConfig(replicaDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to parse PostgreSQL Replica DSN")
	}
	rPoolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	rPool, err := pgxpool.NewWithConfig(context.Background(), rPoolConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Replica via PgBouncer")
	}
	ReadPool = rPool

	log.Info().
		Str("pgbouncer_addr", config.AppConfig.PgBouncerAddr).
		Msg("Connected to PostgreSQL via PgBouncer (write: capstone, read: capstone_read)")
}

func ClosePostgres() {
	if WritePool != nil {
		WritePool.Close()
	}
	if ReadPool != nil {
		ReadPool.Close()
	}
	log.Info().Msg("PostgreSQL connections closed")
}

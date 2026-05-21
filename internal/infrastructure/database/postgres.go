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
	var primaryAddr, replicaAddr, writeDB, readDB string

	writeDB = config.AppConfig.PostgresDBName
	readDB = config.AppConfig.PostgresDBName

	if config.AppConfig.PgBouncerAddr != "" {
		// Docker/PgBouncer mode: semua koneksi melalui pooler.
		// PgBouncer mengekspos dua logical database: <db> (primary) dan <db>_read (replica).
		// Untuk bypass ke direct mode di Kubernetes, set PGBOUNCER_ADDR="" di env.
		primaryAddr = config.AppConfig.PgBouncerAddr
		replicaAddr = config.AppConfig.PgBouncerAddr
		readDB = config.AppConfig.PostgresDBName + "_read"
	} else {
		// Kubernetes/direct mode: koneksi langsung ke masing-masing Postgres service.
		primaryHost := config.AppConfig.PostgresHost
		if primaryHost == "" {
			primaryHost = "postgres-primary"
		}
		primaryPort := config.AppConfig.PostgresPort
		if primaryPort == "" {
			primaryPort = "5432"
		}
		primaryAddr = primaryHost + ":" + primaryPort

		replicaHost := config.AppConfig.PostgresReplicaHost
		if replicaHost == "" {
			if config.AppConfig.PostgresHost != "" {
				replicaHost = config.AppConfig.PostgresHost
			} else {
				replicaHost = "postgres-replica"
			}
		}
		replicaPort := config.AppConfig.PostgresReplicaPort
		if replicaPort == "" {
			replicaPort = primaryPort
		}
		replicaAddr = replicaHost + ":" + replicaPort
	}

	const poolParams = "sslmode=disable&pool_max_conns=100&pool_min_conns=2&pool_max_conn_idle_time=5m"

	primaryDSN := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?%s",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		primaryAddr,
		writeDB,
		poolParams,
	)

	replicaDSN := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?%s",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		replicaAddr,
		readDB,
		poolParams,
	)

	wPoolConfig, err := pgxpool.ParseConfig(primaryDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to parse PostgreSQL Primary DSN")
	}
	wPoolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	wPool, err := pgxpool.NewWithConfig(context.Background(), wPoolConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Primary")
	}
	WritePool = wPool

	rPoolConfig, err := pgxpool.ParseConfig(replicaDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to parse PostgreSQL Replica DSN")
	}
	rPoolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	rPool, err := pgxpool.NewWithConfig(context.Background(), rPoolConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Replica")
	}
	ReadPool = rPool

	if config.AppConfig.PgBouncerAddr != "" {
		log.Info().
			Str("pgbouncer_addr", config.AppConfig.PgBouncerAddr).
			Str("write_db", writeDB).
			Str("read_db", readDB).
			Msg("Connected to PostgreSQL via PgBouncer")
	} else {
		log.Info().
			Str("primary_addr", primaryAddr).
			Str("replica_addr", replicaAddr).
			Str("database", writeDB).
			Msg("Connected to PostgreSQL directly (Kubernetes mode)")
	}
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

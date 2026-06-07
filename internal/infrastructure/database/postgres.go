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
var ShardWritePools []*pgxpool.Pool

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

	connectTransactionShards()
}

func connectTransactionShards() {
	shards := []struct {
		id   int
		host string
		port string
		db   string
	}{
		{0, config.AppConfig.PostgresShard0Host, config.AppConfig.PostgresShard0Port, config.AppConfig.PostgresShard0DB},
		{1, config.AppConfig.PostgresShard1Host, config.AppConfig.PostgresShard1Port, config.AppConfig.PostgresShard1DB},
	}

	ShardWritePools = make([]*pgxpool.Pool, 0, len(shards))
	const poolParams = "sslmode=disable&pool_max_conns=100&pool_min_conns=2&pool_max_conn_idle_time=5m"

	for _, shard := range shards {
		addr := shard.host + ":" + shard.port
		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s/%s?%s",
			config.AppConfig.PostgresUser,
			config.AppConfig.PostgresPassword,
			addr,
			shard.db,
			poolParams,
		)

		poolConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			log.Fatal().Err(err).Int("shard_id", shard.id).Msg("Unable to parse PostgreSQL shard DSN")
		}
		poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			log.Fatal().Err(err).Int("shard_id", shard.id).Msg("Unable to connect to PostgreSQL shard")
		}

		ShardWritePools = append(ShardWritePools, pool)
		log.Info().
			Int("shard_id", shard.id).
			Str("addr", addr).
			Str("database", shard.db).
			Msg("Connected to PostgreSQL transaction shard")
	}
}

func ClosePostgres() {
	if WritePool != nil {
		WritePool.Close()
	}
	if ReadPool != nil {
		ReadPool.Close()
	}
	for _, pool := range ShardWritePools {
		if pool != nil {
			pool.Close()
		}
	}
	log.Info().Msg("PostgreSQL connections closed")
}

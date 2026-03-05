package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var WritePool *pgxpool.Pool
var ReadPool *pgxpool.Pool

func ConnectPostgres() {
	primaryDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&pool_max_conns=100&pool_min_conns=10",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		"postgres-primary", // mapped in docker-compose
		config.AppConfig.PostgresPort,
		config.AppConfig.PostgresDBName,
	)

	replicaDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&pool_max_conns=100&pool_min_conns=10",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		"postgres-replica",            // mapped in docker-compose
		config.AppConfig.PostgresPort, // internal docker port 5432
		config.AppConfig.PostgresDBName,
	)

	wPool, err := pgxpool.New(context.Background(), primaryDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Primary")
	}
	WritePool = wPool

	rPool, err := pgxpool.New(context.Background(), replicaDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to PostgreSQL Replica")
	}
	ReadPool = rPool

	log.Info().Msg("Connected to PostgreSQL Primary and Replica pools")
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

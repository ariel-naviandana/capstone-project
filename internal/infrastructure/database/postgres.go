package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var PostgresPool *pgxpool.Pool

func ConnectPostgres() {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresPassword,
		config.AppConfig.PostgresHost,
		config.AppConfig.PostgresPort,
		config.AppConfig.PostgresDBName,
	)

	safeDSN := fmt.Sprintf("postgres://%s:***@%s:%s/%s?sslmode=disable",
		config.AppConfig.PostgresUser,
		config.AppConfig.PostgresHost,
		config.AppConfig.PostgresPort,
		config.AppConfig.PostgresDBName,
	)

	log.Debug().
		Str("safe_dsn", safeDSN).
		Msg("DSN yang dipakai untuk connect Postgres")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatal().
			Err(err).
			Str("safe_dsn", safeDSN).
			Msg("Unable to connect to PostgreSQL")
	}

	PostgresPool = pool
	log.Info().
		Str("safe_dsn", safeDSN).
		Msg("Connected to PostgreSQL with connection pool")
}

func ClosePostgres() {
	if PostgresPool != nil {
		PostgresPool.Close()
		log.Info().Msg("PostgreSQL connection closed")
	}
}

package database

import (
	"context"
	"fmt"
	"log"

	"github.com/capstone-b4/capstone-go/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
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

	log.Printf("DSN yang dipakai: %s", dsn) // ← tambah ini untuk debug

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}

	PostgresPool = pool
	log.Println("Connected to PostgreSQL with connection pool")
}

func ClosePostgres() {
	if PostgresPool != nil {
		PostgresPool.Close()
		log.Println("PostgreSQL connection closed")
	}
}

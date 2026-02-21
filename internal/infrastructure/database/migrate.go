package database

import (
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/stdlib"
)

func RunMigrations() {
	if PostgresPool == nil {
		log.Fatal("PostgresPool not initialized before migration")
	}

	// Wrap pgxpool menjadi *sql.DB (driver stdlib pgx)
	sqlDB := stdlib.OpenDBFromPool(PostgresPool)
	if sqlDB == nil {
		log.Fatal("Failed to wrap pgxpool to sql.DB")
	}
	defer sqlDB.Close() // close wrapper (tidak tutup pool asli)

	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	if err != nil {
		log.Fatalf("Failed to create postgres driver for migrate: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver,
	)
	if err != nil {
		log.Fatalf("Failed to create migrate instance: %v", err)
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		log.Printf("Migration failed: %v", err)
		// Tidak fatal supaya app tetap jalan kalau sudah migrasi
	}

	log.Println("Database migrations completed (or no changes)")
}

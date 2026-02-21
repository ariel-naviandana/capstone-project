package database

import (
	"context"
	"log"

	"github.com/capstone-b4/capstone-go/internal/domain"
)

func SeedData() {
	if PostgresPool == nil {
		log.Fatal("PostgresPool not initialized before seeding")
	}

	ctx := context.Background()

	// Cek jumlah record di tabel transactions
	var count int
	err := PostgresPool.QueryRow(ctx, "SELECT COUNT(*) FROM transactions").Scan(&count)
	if err != nil {
		log.Printf("Failed to count transactions: %v", err)
		return
	}

	if count > 0 {
		log.Println("Table transactions sudah ada data, skip seeding")
		return
	}

	log.Println("Seeding dummy transactions...")

	// Dummy data
	dummies := []domain.TransactionCreate{
		{UserID: 101, Amount: 50000.0},
		{UserID: 102, Amount: 120000.0},
		{UserID: 103, Amount: 75000.0},
		{UserID: 104, Amount: 30000.0},
		{UserID: 105, Amount: 200000.0},
	}

	tx, err := PostgresPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for seeding: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	for _, d := range dummies {
		query := `
			INSERT INTO transactions (user_id, amount, status, created_at, updated_at)
			VALUES ($1, $2, 'pending', NOW(), NOW())
		`
		_, err := tx.Exec(ctx, query, d.UserID, d.Amount)
		if err != nil {
			log.Printf("Failed to insert dummy transaction: %v", err)
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit seeding transaction: %v", err)
		return
	}

	log.Printf("Successfully seeded %d dummy transactions", len(dummies))
}

package database

import (
	"context"
	"log"
)

func SeedData() {
	if PostgresPool == nil {
		log.Fatal("PostgresPool not initialized before seeding")
	}

	ctx := context.Background()

	// Cek jumlah record di tabel users
	var userCount int
	err := PostgresPool.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		log.Printf("Failed to count users: %v", err)
		return
	}

	if userCount > 0 {
		log.Println("Table users sudah ada data, skip seeding")
		return
	}

	log.Println("Seeding dummy users and transactions...")

	tx, err := PostgresPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for seeding: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	// Dummy users dengan balance
	dummyUsers := []struct {
		Username string
		Balance  float64
	}{
		{"user1", 100000.0},
		{"user2", 50000.0},
		{"user3", 200000.0},
		{"user4", 0.0},
		{"user5", 150000.0},
	}

	userIDs := make([]int64, len(dummyUsers))
	for i, u := range dummyUsers {
		err := tx.QueryRow(ctx, "INSERT INTO users (username, balance) VALUES ($1, $2) RETURNING id", u.Username, u.Balance).Scan(&userIDs[i])
		if err != nil {
			log.Printf("Failed to seed user: %v", err)
			return
		}
	}

	// Dummy transactions (mix type: deposit, withdraw, transfer)
	dummies := []struct {
		UserID      int64
		RecipientID int64
		Amount      float64
		Type        string
		Description string
	}{
		{userIDs[0], 0, 50000.0, "deposit", "Topup awal"}, // deposit (recipient_id 0 or NULL)
		{userIDs[1], 0, 20000.0, "withdraw", "Tarik tunai"},
		{userIDs[2], userIDs[0], 30000.0, "transfer", "Transfer ke user1"},
		{userIDs[3], 0, 100000.0, "deposit", "Topup besar"},
		{userIDs[4], userIDs[1], 50000.0, "transfer", "Transfer ke user2"},
	}

	for _, d := range dummies {
		query := `
			INSERT INTO transactions (user_id, recipient_id, amount, type, status, description, created_at, updated_at)
			VALUES ($1, $2, $3, $4, 'pending', $5, NOW(), NOW())
		`
		recipientID := d.RecipientID
		if recipientID == 0 {
			recipientID = 0 // NULL in DB
		}
		_, err := tx.Exec(ctx, query, d.UserID, recipientID, d.Amount, d.Type, d.Description)
		if err != nil {
			log.Printf("Failed to seed transaction: %v", err)
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit seeding transaction: %v", err)
		return
	}

	log.Printf("Successfully seeded %d dummy users and %d dummy transactions", len(dummyUsers), len(dummies))
}

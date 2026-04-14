package database

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
)

func TestTransactionRepository_GetUserBalance(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		mockPool.ExpectQuery(`SELECT id, username, balance FROM users`).
			WithArgs(int64(1)).
			WillReturnRows(pgxmock.NewRows([]string{"id", "username", "balance"}).AddRow(int64(1), "user1", float64(100000)))

		balance, err := repo.GetUserBalance(ctx, 1)

		assert.NoError(t, err)
		assert.Equal(t, float64(100000), balance.Balance)
		assert.Equal(t, "user1", balance.Username)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
	
	t.Run("Not Found", func(t *testing.T) {
		// Mock pgx.ErrNoRows behaviour
		mockPool.ExpectQuery(`SELECT id, username, balance FROM users`).
			WithArgs(int64(999)).
			WillReturnError(pgx.ErrNoRows)

		balance, err := repo.GetUserBalance(ctx, 999)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "user not found")
		assert.Nil(t, balance)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

func TestTransactionRepository_GetUserTransactions(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success Returns Data", func(t *testing.T) {
		timeNow := time.Now()
		rows := pgxmock.NewRows([]string{"tx_id", "id", "user_id", "recipient_id", "amount", "type", "status", "created_at", "updated_at"}).
			AddRow("tx123", int64(10), int64(1), int64(0), float64(50000), "deposit", "success", timeNow, timeNow).
			AddRow("tx456", int64(11), int64(1), int64(2), float64(20000), "transfer", "pending", timeNow, timeNow)

		mockPool.ExpectQuery(`SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions`).
			WithArgs(int64(1), 10, 0).
			WillReturnRows(rows)

		txs, err := repo.GetUserTransactions(ctx, 1, 10, 0)

		assert.NoError(t, err)
		assert.Len(t, txs, 2)
		assert.Equal(t, "deposit", txs[0].Type)
		assert.Equal(t, "tx456", txs[1].TxID)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Success Returns Empty Database", func(t *testing.T) {
		rows := pgxmock.NewRows([]string{"tx_id", "id", "user_id", "recipient_id", "amount", "type", "status", "created_at", "updated_at"})

		mockPool.ExpectQuery(`SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions`).
			WithArgs(int64(3), 10, 0).
			WillReturnRows(rows)

		txs, err := repo.GetUserTransactions(ctx, 3, 10, 0)

		assert.NoError(t, err)
		assert.Len(t, txs, 0)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

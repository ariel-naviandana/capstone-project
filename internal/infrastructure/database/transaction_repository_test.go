package database

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
)

func TestTransactionRepository_GetAccountBalance(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		mockPool.ExpectQuery(`SELECT account_no, balance FROM accounts`).
			WithArgs("ACC-1").
			WillReturnRows(pgxmock.NewRows([]string{"account_no", "balance"}).AddRow("ACC-1", float64(100000)))

		balance, err := repo.GetAccountBalance(ctx, "ACC-1")

		assert.NoError(t, err)
		assert.Equal(t, float64(100000), balance.Balance)
		assert.Equal(t, "ACC-1", balance.AccountNo)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Not Found", func(t *testing.T) {
		// Mock pgx.ErrNoRows behaviour
		mockPool.ExpectQuery(`SELECT account_no, balance FROM accounts`).
			WithArgs("ACC-999").
			WillReturnError(pgx.ErrNoRows)

		balance, err := repo.GetAccountBalance(ctx, "ACC-999")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "account not found")
		assert.Nil(t, balance)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

func TestTransactionRepository_GetAccountTransactions(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success Returns Data", func(t *testing.T) {
		timeNow := time.Now()
		var nilRef *string
		rows := pgxmock.NewRows([]string{"trx_id", "account_no", "amount", "type", "status", "ref_no", "created_at", "updated_at"}).
			AddRow("tx123", "ACC-1", float64(50000), "deposit", "success", nilRef, timeNow, timeNow).
			AddRow("tx456", "ACC-1", float64(20000), "transfer", "pending", nilRef, timeNow, timeNow)

		mockPool.ExpectQuery(`SELECT trx_id, account_no, amount, type, status, ref_no, created_at, updated_at FROM transactions`).
			WithArgs("ACC-1", 10, 0).
			WillReturnRows(rows)

		txs, err := repo.GetAccountTransactions(ctx, "ACC-1", 10, 0)

		assert.NoError(t, err)
		assert.Len(t, txs, 2)
		assert.Equal(t, "deposit", txs[0].Type)
		assert.Equal(t, "tx456", txs[1].TrxID)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Success Returns Empty Database", func(t *testing.T) {
		rows := pgxmock.NewRows([]string{"trx_id", "account_no", "amount", "type", "status", "ref_no", "created_at", "updated_at"})

		mockPool.ExpectQuery(`SELECT trx_id, account_no, amount, type, status, ref_no, created_at, updated_at FROM transactions`).
			WithArgs("ACC-3", 10, 0).
			WillReturnRows(rows)

		txs, err := repo.GetAccountTransactions(ctx, "ACC-3", 10, 0)

		assert.NoError(t, err)
		assert.Len(t, txs, 0)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

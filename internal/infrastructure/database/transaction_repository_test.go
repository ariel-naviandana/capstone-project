package database

import (
	"context"
	"testing"
	"time"

	"github.com/capstone-b4/capstone-go/internal/domain"
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

// ==================== TEST Create METHOD ====================

func TestTransactionRepository_Create(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success - Create Deposit", func(t *testing.T) {
		input := &domain.TransactionCreate{
			UserID: 1,
			Amount: 50000,
			Type:   "deposit",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(input.UserID, int64(0), input.Amount, input.Type, input.Description).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

		id, err := repo.Create(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, int64(1), id)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Success - Create Transfer", func(t *testing.T) {
		input := &domain.TransactionCreate{
			UserID:      1,
			RecipientID: 2,
			Amount:      50000,
			Type:        "transfer",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(input.UserID, input.RecipientID, input.Amount, input.Type, input.Description).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))

		id, err := repo.Create(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, int64(2), id)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Database Failure", func(t *testing.T) {
		input := &domain.TransactionCreate{
			UserID: 1,
			Amount: 50000,
			Type:   "deposit",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(input.UserID, int64(0), input.Amount, input.Type, input.Description).
			WillReturnError(pgx.ErrTxClosed)

		id, err := repo.Create(ctx, input)

		assert.Error(t, err)
		assert.Equal(t, int64(0), id)
		assert.Contains(t, err.Error(), "failed to insert transaction")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

// ==================== TEST GetByTxID METHOD ====================

func TestTransactionRepository_GetByTxID(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success - Transaction Found", func(t *testing.T) {
		txID := "tx-123"
		now := time.Now()

		rows := pgxmock.NewRows([]string{
			"tx_id", "id", "user_id", "recipient_id",
			"amount", "type", "status", "created_at", "updated_at",
		}).AddRow(txID, int64(10), int64(1), int64(2), float64(50000), "transfer", "success", now, now)

		mockPool.ExpectQuery(`SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions WHERE tx_id = \$1`).
			WithArgs(txID).
			WillReturnRows(rows)

		result, err := repo.GetByTxID(ctx, txID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, txID, result.TxID)
		assert.Equal(t, int64(10), result.ID)
		assert.Equal(t, int64(1), result.UserID)
		assert.Equal(t, int64(2), result.RecipientID)
		assert.Equal(t, float64(50000), result.Amount)
		assert.Equal(t, "transfer", result.Type)
		assert.Equal(t, "success", result.Status)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Transaction Not Found", func(t *testing.T) {
		txID := "tx-notfound"

		mockPool.ExpectQuery(`SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions WHERE tx_id = \$1`).
			WithArgs(txID).
			WillReturnError(pgx.ErrNoRows)

		result, err := repo.GetByTxID(ctx, txID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "transaction not found")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Database Failure", func(t *testing.T) {
		txID := "tx-456"

		mockPool.ExpectQuery(`SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions WHERE tx_id = \$1`).
			WithArgs(txID).
			WillReturnError(pgx.ErrTxClosed)

		result, err := repo.GetByTxID(ctx, txID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get transaction")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

package database

import (
	"context"
	"testing"
	"time"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
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
		var nilRecipient *string
		rows := pgxmock.NewRows([]string{"trx_id", "account_no", "recipient_no", "amount", "type", "status", "ref_no", "created_at", "updated_at"}).
			AddRow("tx123", "ACC-1", nilRecipient, float64(50000), "deposit", "success", nilRef, timeNow, timeNow).
			AddRow("tx456", "ACC-1", nilRecipient, float64(20000), "transfer", "pending", nilRef, timeNow, timeNow)

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions`).
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
		rows := pgxmock.NewRows([]string{"trx_id", "account_no", "recipient_no", "amount", "type", "status", "ref_no", "created_at", "updated_at"})

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions`).
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
			AccountNo: "ACC-1",
			Amount:    50000,
			Type:      "deposit",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(pgxmock.AnyArg(), input.AccountNo, input.RecipientNo, input.Type, input.Amount, input.RefNo).
			WillReturnRows(pgxmock.NewRows([]string{"trx_id"}).AddRow("TRX-1"))

		id, err := repo.Create(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, "TRX-1", id)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Success - Create Transfer", func(t *testing.T) {
		input := &domain.TransactionCreate{
			AccountNo:   "ACC-1",
			RecipientNo: "ACC-2",
			Amount:      50000,
			Type:        "transfer",
			RefNo:       "REF-9",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(pgxmock.AnyArg(), input.AccountNo, input.RecipientNo, input.Type, input.Amount, input.RefNo).
			WillReturnRows(pgxmock.NewRows([]string{"trx_id"}).AddRow("TRX-2"))

		id, err := repo.Create(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, "TRX-2", id)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Database Failure", func(t *testing.T) {
		input := &domain.TransactionCreate{
			AccountNo: "ACC-1",
			Amount:    50000,
			Type:      "deposit",
		}

		mockPool.ExpectQuery(`INSERT INTO transactions`).
			WithArgs(pgxmock.AnyArg(), input.AccountNo, input.RecipientNo, input.Type, input.Amount, input.RefNo).
			WillReturnError(pgx.ErrTxClosed)

		id, err := repo.Create(ctx, input)

		assert.Error(t, err)
		assert.Equal(t, "", id)
		assert.Contains(t, err.Error(), "failed to insert transaction")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

// ==================== TEST GetAccountBalance Cache-Aside ====================

func TestTransactionRepository_GetAccountBalance_CacheAside(t *testing.T) {
	// When RedisClient is nil, it must still work (falls through to DB)
	cache.RedisClient = nil

	mockPool, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	mockPool.ExpectQuery(`SELECT account_no, balance FROM accounts`).
		WithArgs("ACC-1").
		WillReturnRows(pgxmock.NewRows([]string{"account_no", "balance"}).
			AddRow("ACC-1", float64(100000)))

	result, err := repo.GetAccountBalance(ctx, "ACC-1")
	assert.NoError(t, err)
	assert.Equal(t, float64(100000), result.Balance)
	assert.Equal(t, "ACC-1", result.AccountNo)
	assert.NoError(t, mockPool.ExpectationsWereMet())
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
		trxID := "TRX-123"
		now := time.Now()
		refNo := "REF-1"
		var recipientNo *string

		rows := pgxmock.NewRows([]string{
			"trx_id", "account_no", "recipient_no", "amount", "type", "status", "ref_no", "created_at", "updated_at",
		}).AddRow(trxID, "ACC-1", recipientNo, float64(50000), "transfer", "success", &refNo, now, now)

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnRows(rows)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, trxID, result.TrxID)
		assert.Equal(t, "ACC-1", result.AccountNo)
		assert.Equal(t, float64(50000), result.Amount)
		assert.Equal(t, "transfer", result.Type)
		assert.Equal(t, "success", result.Status)
		assert.Equal(t, refNo, result.RefNo)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Transaction Not Found", func(t *testing.T) {
		trxID := "TRX-notfound"

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrNoRows)
		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrNoRows)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "transaction not found")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Database Failure", func(t *testing.T) {
		trxID := "TRX-456"

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrTxClosed)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get transaction")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})
}

// ==================== TEST GetByTxID Cache-Aside ====================

func TestTransactionRepository_GetByTxID_CacheAside(t *testing.T) {
	// When RedisClient is nil, GetByTxID must still work (falls through to DB)
	cache.RedisClient = nil

	mockPool, err := pgxmock.NewPool()
	assert.NoError(t, err)
	defer mockPool.Close()

	repo := NewTransactionRepository(mockPool, mockPool)
	ctx := context.Background()

	t.Run("Success - Transaction Found", func(t *testing.T) {
		trxID := "TRX-123"
		now := time.Now()
		refNo := "REF-2"
		var recipientNo *string

		rows := pgxmock.NewRows([]string{
			"trx_id", "account_no", "recipient_no", "amount", "type", "status", "ref_no", "created_at", "updated_at",
		}).AddRow(trxID, "ACC-2", recipientNo, float64(50000), "transfer", "success", &refNo, now, now)

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnRows(rows)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, trxID, result.TrxID)
		assert.Equal(t, "ACC-2", result.AccountNo)
		assert.Equal(t, float64(50000), result.Amount)
		assert.Equal(t, "transfer", result.Type)
		assert.Equal(t, "success", result.Status)
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Transaction Not Found", func(t *testing.T) {
		trxID := "TRX-notfound"

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrNoRows)
		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrNoRows)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "transaction not found")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	t.Run("Error - Database Failure", func(t *testing.T) {
		trxID := "TRX-456"

		mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
			WithArgs(trxID).
			WillReturnError(pgx.ErrTxClosed)

		result, err := repo.GetByTxID(ctx, trxID)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get transaction")
		assert.NoError(t, mockPool.ExpectationsWereMet())
	})

	timeNow := time.Now()
	refNo3 := "REF-3"
	var recipientNo3 *string
	mockPool.ExpectQuery(`SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at FROM transactions WHERE trx_id = \$1`).
		WithArgs("TRX-abc").
		WillReturnRows(pgxmock.NewRows([]string{"trx_id", "account_no", "recipient_no", "amount", "type", "status", "ref_no", "created_at", "updated_at"}).
			AddRow("TRX-abc", "ACC-3", recipientNo3, float64(500), "deposit", "success", &refNo3, timeNow, timeNow))

	result, err := repo.GetByTxID(ctx, "TRX-abc")
	assert.NoError(t, err)
	assert.Equal(t, "TRX-abc", result.TrxID)
	assert.NoError(t, mockPool.ExpectationsWereMet())
}

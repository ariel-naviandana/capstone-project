package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
)

// querySelectTxCols adalah pola regex yang match query SELECT GetUserTransactions.
// Ditulis eksplisit supaya test tidak pecah kalau query di-reformat (whitespace).
const querySelectTxCols = `SELECT tx_id, id, user_id, COALESCE\(recipient_id, 0\) as recipient_id, amount, type, status, created_at, updated_at FROM transactions`

// newRepoMockPool mengembalikan repository yang di-inject dengan single mock pool
// untuk both writeDb dan readDb. Cukup untuk read-only test GetUserTransactions.
func newRepoMockPool(t *testing.T) (*transactionRepository, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)
	repo := &transactionRepository{writeDb: mock, readDb: mock}
	return repo, mock
}

func TestTransactionRepository_GetUserTransactions_SuccessWithData(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	now := time.Now()
	rows := pgxmock.NewRows([]string{
		"tx_id", "id", "user_id", "recipient_id",
		"amount", "type", "status", "created_at", "updated_at",
	}).
		AddRow("tx-a", int64(10), int64(1), int64(0), float64(50000), "deposit", "success", now, now).
		AddRow("tx-b", int64(11), int64(1), int64(2), float64(25000), "transfer", "success", now, now).
		AddRow("tx-c", int64(12), int64(3), int64(1), float64(15000), "transfer", "pending", now, now)

	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(1), 10, 0).
		WillReturnRows(rows)

	txs, err := repo.GetUserTransactions(ctx, 1, 10, 0)

	assert.NoError(t, err)
	assert.Len(t, txs, 3)
	assert.Equal(t, "tx-a", txs[0].TxID)
	assert.Equal(t, "deposit", txs[0].Type)
	assert.Equal(t, int64(1), txs[0].UserID)
	assert.Equal(t, "transfer", txs[1].Type)
	assert.Equal(t, int64(2), txs[1].RecipientID)
	// Baris ke-3: user 1 sebagai receiver (recipient_id=1)
	assert.Equal(t, int64(1), txs[2].RecipientID)
	assert.Equal(t, "pending", txs[2].Status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepository_GetUserTransactions_EmptyResult(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	empty := pgxmock.NewRows([]string{
		"tx_id", "id", "user_id", "recipient_id",
		"amount", "type", "status", "created_at", "updated_at",
	})
	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(999), 10, 0).
		WillReturnRows(empty)

	txs, err := repo.GetUserTransactions(ctx, 999, 10, 0)

	assert.NoError(t, err)
	assert.Len(t, txs, 0)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepository_GetUserTransactions_PaginationArgsPassedCorrectly(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	now := time.Now()
	rows := pgxmock.NewRows([]string{
		"tx_id", "id", "user_id", "recipient_id",
		"amount", "type", "status", "created_at", "updated_at",
	}).AddRow("tx-p", int64(99), int64(5), int64(0), float64(1000), "deposit", "success", now, now)

	// Pastikan args yang di-pass ke Query adalah (userID, limit, offset) urutan benar
	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(5), 25, 50).
		WillReturnRows(rows)

	txs, err := repo.GetUserTransactions(ctx, 5, 25, 50)

	assert.NoError(t, err)
	assert.Len(t, txs, 1)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepository_GetUserTransactions_QueryError(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	// Simulate DB connection/query error
	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(1), 10, 0).
		WillReturnError(errors.New("connection refused"))

	txs, err := repo.GetUserTransactions(ctx, 1, 10, 0)

	assert.Error(t, err)
	assert.Nil(t, txs)
	assert.Contains(t, err.Error(), "failed to execute query")
	assert.Contains(t, err.Error(), "user_id=1")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepository_GetUserTransactions_ScanError(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	// Row dengan tipe kolom salah → scan akan fail
	badRows := pgxmock.NewRows([]string{
		"tx_id", "id", "user_id", "recipient_id",
		"amount", "type", "status", "created_at", "updated_at",
	}).AddRow("tx-err", "NOT_AN_INT64", int64(1), int64(0),
		float64(100), "deposit", "success", time.Now(), time.Now())

	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(1), 10, 0).
		WillReturnRows(badRows)

	txs, err := repo.GetUserTransactions(ctx, 1, 10, 0)

	assert.Error(t, err)
	assert.Nil(t, txs)
	assert.Contains(t, err.Error(), "failed to scan transaction row")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepository_GetUserTransactions_RowsIterationError(t *testing.T) {
	repo, mock := newRepoMockPool(t)
	defer mock.Close()
	ctx := context.Background()

	now := time.Now()
	rows := pgxmock.NewRows([]string{
		"tx_id", "id", "user_id", "recipient_id",
		"amount", "type", "status", "created_at", "updated_at",
	}).
		AddRow("tx-ok", int64(1), int64(1), int64(0), float64(100), "deposit", "success", now, now).
		RowError(0, errors.New("stream interrupted"))

	mock.ExpectQuery(querySelectTxCols).
		WithArgs(int64(1), 10, 0).
		WillReturnRows(rows)

	txs, err := repo.GetUserTransactions(ctx, 1, 10, 0)

	assert.Error(t, err)
	assert.Nil(t, txs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

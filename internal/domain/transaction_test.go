package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTransactionStructFields(t *testing.T) {
	now := time.Now()
	tx := Transaction{
		ID:          1,
		UserID:      100,
		RecipientID: 200,
		Amount:      50000.50,
		Type:        "transfer",
		Status:      "success",
		Description: "Test transfer",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	assert.Equal(t, int64(1), tx.ID)
	assert.Equal(t, int64(100), tx.UserID)
	assert.Equal(t, int64(200), tx.RecipientID)
	assert.Equal(t, 50000.50, tx.Amount)
	assert.Equal(t, "transfer", tx.Type)
	assert.Equal(t, "success", tx.Status)
	assert.Equal(t, "Test transfer", tx.Description)
	assert.Equal(t, now, tx.CreatedAt)
	assert.Equal(t, now, tx.UpdatedAt)
}

func TestTransactionCreateStruct(t *testing.T) {
	t.Run("Deposit", func(t *testing.T) {
		input := TransactionCreate{
			UserID: 1,
			Amount: 10000,
			Type:   "deposit",
		}
		assert.Equal(t, int64(1), input.UserID)
		assert.Equal(t, 10000.0, input.Amount)
		assert.Equal(t, "deposit", input.Type)
		assert.Zero(t, input.RecipientID)
	})

	t.Run("Transfer with recipient", func(t *testing.T) {
		input := TransactionCreate{
			UserID:      1,
			Amount:      5000,
			Type:        "transfer",
			RecipientID: 2,
			Description: "Test",
		}
		assert.Equal(t, int64(1), input.UserID)
		assert.Equal(t, 5000.0, input.Amount)
		assert.Equal(t, "transfer", input.Type)
		assert.Equal(t, int64(2), input.RecipientID)
		assert.Equal(t, "Test", input.Description)
	})
}

func TestKafkaTransactionEventStruct(t *testing.T) {
	event := KafkaTransactionEvent{
		TxID:        "tx-abc-123",
		UserID:      1,
		RecipientID: 2,
		Amount:      75000,
		Type:        "transfer",
		Timestamp:   "2026-01-01T00:00:00Z",
	}

	assert.Equal(t, "tx-abc-123", event.TxID)
	assert.Equal(t, int64(1), event.UserID)
	assert.Equal(t, int64(2), event.RecipientID)
	assert.Equal(t, 75000.0, event.Amount)
	assert.Equal(t, "transfer", event.Type)
	assert.Equal(t, "2026-01-01T00:00:00Z", event.Timestamp)
}

func TestTransactionDetailStruct(t *testing.T) {
	now := time.Now().UTC()
	detail := TransactionDetail{
		TxID:        "tx-detail-456",
		ID:          10,
		UserID:      100,
		RecipientID: 200,
		Amount:      30000,
		Type:        "transfer",
		Status:      "success",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	assert.Equal(t, "tx-detail-456", detail.TxID)
	assert.Equal(t, int64(10), detail.ID)
	assert.Equal(t, int64(100), detail.UserID)
	assert.Equal(t, int64(200), detail.RecipientID)
	assert.Equal(t, 30000.0, detail.Amount)
	assert.Equal(t, "transfer", detail.Type)
	assert.Equal(t, "success", detail.Status)
	assert.Equal(t, now, detail.CreatedAt)
	assert.Equal(t, now, detail.UpdatedAt)
}

func TestUserBalanceStruct(t *testing.T) {
	balance := UserBalance{
		ID:       1,
		Username: "testuser",
		Balance:  500000,
	}

	assert.Equal(t, int64(1), balance.ID)
	assert.Equal(t, "testuser", balance.Username)
	assert.Equal(t, 500000.0, balance.Balance)
}

func TestUserBalanceZeroValue(t *testing.T) {
	var balance UserBalance
	assert.Zero(t, balance.ID)
	assert.Empty(t, balance.Username)
	assert.Zero(t, balance.Balance)
}

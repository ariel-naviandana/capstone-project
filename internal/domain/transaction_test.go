package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTransactionStructFields(t *testing.T) {
	now := time.Now()
	tx := Transaction{
		TrxID:     "TRX-1",
		AccountNo: "ACC-1",
		Amount:    50000.50,
		Type:      "transfer",
		Status:    "success",
		RefNo:     "REF-1",
		CreatedAt: now,
		UpdatedAt: now,
	}

	assert.Equal(t, "TRX-1", tx.TrxID)
	assert.Equal(t, "ACC-1", tx.AccountNo)
	assert.Equal(t, 50000.50, tx.Amount)
	assert.Equal(t, "transfer", tx.Type)
	assert.Equal(t, "success", tx.Status)
	assert.Equal(t, "REF-1", tx.RefNo)
	assert.Equal(t, now, tx.CreatedAt)
	assert.Equal(t, now, tx.UpdatedAt)
}

func TestTransactionCreateStruct(t *testing.T) {
	t.Run("Deposit", func(t *testing.T) {
		input := TransactionCreate{
			AccountNo: "ACC-1",
			Amount:    10000,
			Type:      "deposit",
		}
		assert.Equal(t, "ACC-1", input.AccountNo)
		assert.Equal(t, 10000.0, input.Amount)
		assert.Equal(t, "deposit", input.Type)
		assert.Empty(t, input.RecipientNo)
	})

	t.Run("Transfer with recipient", func(t *testing.T) {
		input := TransactionCreate{
			AccountNo:   "ACC-1",
			Amount:      5000,
			Type:        "transfer",
			RecipientNo: "ACC-2",
			RefNo:       "REF-2",
		}
		assert.Equal(t, "ACC-1", input.AccountNo)
		assert.Equal(t, 5000.0, input.Amount)
		assert.Equal(t, "transfer", input.Type)
		assert.Equal(t, "ACC-2", input.RecipientNo)
		assert.Equal(t, "REF-2", input.RefNo)
	})
}

func TestKafkaTransactionEventStruct(t *testing.T) {
	event := KafkaTransactionEvent{
		TrxID:       "tx-abc-123",
		AccountNo:   "ACC-1",
		RecipientNo: "ACC-2",
		Amount:      75000,
		Type:        "transfer",
		Timestamp:   "2026-01-01T00:00:00Z",
	}

	assert.Equal(t, "tx-abc-123", event.TrxID)
	assert.Equal(t, "ACC-1", event.AccountNo)
	assert.Equal(t, "ACC-2", event.RecipientNo)
	assert.Equal(t, 75000.0, event.Amount)
	assert.Equal(t, "transfer", event.Type)
	assert.Equal(t, "2026-01-01T00:00:00Z", event.Timestamp)
}

func TestTransactionDetailStruct(t *testing.T) {
	now := time.Now().UTC()
	detail := TransactionDetail{
		TrxID:       "tx-detail-456",
		AccountNo:   "ACC-1",
		RecipientNo: "ACC-2",
		Amount:      30000,
		Type:        "transfer",
		Status:      "success",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	assert.Equal(t, "tx-detail-456", detail.TrxID)
	assert.Equal(t, "ACC-1", detail.AccountNo)
	assert.Equal(t, "ACC-2", detail.RecipientNo)
	assert.Equal(t, 30000.0, detail.Amount)
	assert.Equal(t, "transfer", detail.Type)
	assert.Equal(t, "success", detail.Status)
	assert.Equal(t, now, detail.CreatedAt)
	assert.Equal(t, now, detail.UpdatedAt)
}

func TestAccountBalanceStruct(t *testing.T) {
	balance := AccountBalance{
		AccountNo: "ACC-1",
		Balance:   500000,
	}

	assert.Equal(t, "ACC-1", balance.AccountNo)
	assert.Equal(t, 500000.0, balance.Balance)
}

func TestAccountBalanceZeroValue(t *testing.T) {
	var balance AccountBalance
	assert.Empty(t, balance.AccountNo)
	assert.Zero(t, balance.Balance)
}

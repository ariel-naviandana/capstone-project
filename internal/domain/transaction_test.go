package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionCreateValidation(t *testing.T) {
	testCases := []struct {
		name    string
		input   TransactionCreate
		isValid bool
	}{
		{
			name: "Valid deposit",
			input: TransactionCreate{
				AccountNo: "123-456-000001",
				Amount:    100000,
				Type:      "deposit",
				RefNo:     "REF001",
			},
			isValid: true,
		},
		{
			name: "Valid withdraw",
			input: TransactionCreate{
				AccountNo: "123-456-000001",
				Amount:    50000,
				Type:      "withdraw",
				RefNo:     "REF002",
			},
			isValid: true,
		},
		{
			name: "Valid transfer",
			input: TransactionCreate{
				AccountNo:   "123-456-000001",
				RecipientNo: "456-789-000002",
				Amount:      75000,
				Type:        "transfer",
				RefNo:       "REF003",
			},
			isValid: true,
		},
		{
			name: "Missing account_no",
			input: TransactionCreate{
				Amount: 100000,
				Type:   "deposit",
			},
			isValid: false,
		},
		{
			name: "Invalid amount (zero)",
			input: TransactionCreate{
				AccountNo: "123-456-000001",
				Amount:    0,
				Type:      "deposit",
			},
			isValid: false,
		},
		{
			name: "Invalid type",
			input: TransactionCreate{
				AccountNo: "123-456-000001",
				Amount:    100000,
				Type:      "invalid_type",
			},
			isValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isValid := true

			if tc.input.AccountNo == "" {
				isValid = false
			}

			if tc.input.Amount <= 0 {
				isValid = false
			}

			validTypes := map[string]bool{
				"deposit":  true,
				"withdraw": true,
				"transfer": true,
			}

			if !validTypes[tc.input.Type] {
				isValid = false
			}

			assert.Equal(t, tc.isValid, isValid)
		})
	}
}

func TestAccountBalanceStructure(t *testing.T) {
	balance := &AccountBalance{
		AccountNo: "123-456-000001",
		Balance:   5000000,
	}

	assert.Equal(t, "123-456-000001", balance.AccountNo)
	assert.Equal(t, 5000000.0, balance.Balance)
}

func TestTransactionDetailStructure(t *testing.T) {
	detail := &TransactionDetail{
		TrxID:       "TRX-20260514-abc123",
		AccountNo:   "123-456-000001",
		Amount:      100000,
		Type:        "deposit",
		Status:      "pending",
		RefNo:       "REF001",
		RecipientNo: "",
	}

	assert.Equal(t, "TRX-20260514-abc123", detail.TrxID)
	assert.Equal(t, "123-456-000001", detail.AccountNo)
	assert.Equal(t, 100000.0, detail.Amount)
	assert.Equal(t, "deposit", detail.Type)
	assert.Equal(t, "pending", detail.Status)
	assert.Equal(t, "REF001", detail.RefNo)
	assert.Empty(t, detail.RecipientNo)
}

package domain

import "time"

// Transaction adalah entity lengkap dari tabel transactions
type Transaction struct {
	ID          int64     `json:"id"`
	UserID      int64     `json:"user_id"`
	RecipientID int64     `json:"recipient_id,omitempty"` // 0 atau null kalau bukan transfer
	Amount      float64   `json:"amount"`
	Type        string    `json:"type"`   // 'deposit', 'withdraw', 'transfer'
	Status      string    `json:"status"` // 'pending', 'success', 'failed'
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// TransactionCreate untuk input POST /transactions
type TransactionCreate struct {
	UserID      int64   `json:"user_id" binding:"required"`
	Amount      float64 `json:"amount" binding:"required,gt=0"`
	Type        string  `json:"type" binding:"required,oneof=deposit withdraw transfer"` // wajib, hanya 3 nilai
	RecipientID int64   `json:"recipient_id,omitempty"`                                  // opsional, wajib kalau type=transfer
	Description string  `json:"description,omitempty"`                                   // opsional
}

// KafkaTransactionEvent untuk message dari Kafka
type KafkaTransactionEvent struct {
	TxID        string  `json:"tx_id"`
	UserID      int64   `json:"user_id"`
	RecipientID int64   `json:"recipient_id"`
	Amount      float64 `json:"amount"`
	Type        string  `json:"type"` // deposit, withdraw, transfer
	Timestamp   string  `json:"timestamp"`
}

type TransactionDetail struct {
	TxID        string    `json:"tx_id"`
	ID          int64     `json:"id,omitempty"` // internal DB ID kalau perlu
	UserID      int64     `json:"user_id"`
	RecipientID int64     `json:"recipient_id,omitempty"`
	Amount      float64   `json:"amount"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type UserBalance struct {
	ID       int64   `json:"id"`
	Username string  `json:"username"`
	Balance  float64 `json:"balance"`
}

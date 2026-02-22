package domain

import "time"

type Transaction struct {
	ID          int64     `json:"id"`
	UserID      int64     `json:"user_id"`
	RecipientID int64     `json:"recipient_id,omitempty"`
	Amount      float64   `json:"amount"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type TransactionCreate struct {
	UserID      int64   `json:"user_id" binding:"required"`
	Amount      float64 `json:"amount" binding:"required,gt=0"`
	Type        string  `json:"type" binding:"required,oneof=deposit withdraw transfer"`
	RecipientID int64   `json:"recipient_id,omitempty"`
	Description string  `json:"description,omitempty"`
}

type KafkaTransactionEvent struct {
	TxID        string  `json:"tx_id"`
	UserID      int64   `json:"user_id"`
	RecipientID int64   `json:"recipient_id"`
	Amount      float64 `json:"amount"`
	Type        string  `json:"type"`
	Timestamp   string  `json:"timestamp"`
}

type TransactionDetail struct {
	TxID        string    `json:"tx_id"`
	ID          int64     `json:"id,omitempty"`
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

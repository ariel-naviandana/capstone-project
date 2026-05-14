package domain

import "time"

type Transaction struct {
	TrxID     string    `json:"trx_id"`
	AccountNo string    `json:"account_no"`
	Type      string    `json:"type"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	RefNo     string    `json:"ref_no,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TransactionCreate struct {
	AccountNo   string  `json:"account_no" binding:"required"`
	Amount      float64 `json:"amount" binding:"required,gt=0"`
	Type        string  `json:"type" binding:"required,oneof=deposit withdraw transfer"`
	RecipientNo string  `json:"recipient_no,omitempty"` // Used for transfers
	RefNo       string  `json:"ref_no,omitempty"`
}

type KafkaTransactionEvent struct {
	TrxID       string  `json:"trx_id"`
	AccountNo   string  `json:"account_no"`
	RecipientNo string  `json:"recipient_no,omitempty"`
	Amount      float64 `json:"amount"`
	Type        string  `json:"type"`
	RefNo       string  `json:"ref_no,omitempty"`
	Timestamp   string  `json:"timestamp"`
}

type TransactionDetail struct {
	TrxID       string    `json:"trx_id"`
	AccountNo   string    `json:"account_no"`
	RecipientNo string    `json:"recipient_no,omitempty"`
	Amount      float64   `json:"amount"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	RefNo       string    `json:"ref_no,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type AccountBalance struct {
	AccountNo string  `json:"account_no"`
	Balance   float64 `json:"balance"`
}
package domain

import "time"

type Transaction struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"` // 'pending', 'success', 'failed'
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TransactionCreate struct {
	UserID int64   `json:"user_id" binding:"required"`
	Amount float64 `json:"amount" binding:"required,gt=0"`
}

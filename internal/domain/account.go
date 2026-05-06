package domain

import "time"

type Account struct {
	AccountNo  string    `json:"account_no"`
	CustomerID string    `json:"customer_id"`
	Type       string    `json:"type"`
	Balance    float64   `json:"balance"`
	Currency   string    `json:"currency"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

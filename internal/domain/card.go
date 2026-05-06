package domain

import "time"

type Card struct {
	CardNoMasked string    `json:"card_no_masked"`
	AccountNo    string    `json:"account_no"`
	Type         string    `json:"type"`
	Expiry       string    `json:"expiry"`
	Status       string    `json:"status"`
	Limit        *float64  `json:"limit,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

package domain

import "time"

type Notification struct {
	NotifID   string    `json:"notif_id"`
	AccountNo string    `json:"account_no"`
	Channel   string    `json:"channel"`
	Title     string    `json:"title"`
	Read      bool      `json:"read"`
	TrxRef    string    `json:"trx_ref"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

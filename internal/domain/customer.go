package domain

import "time"

type Customer struct {
	CustomerID string    `json:"customer_id"`
	FullName   string    `json:"full_name"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CustomerIdentity struct {
	CustomerID  string    `json:"customer_id"`
	NIK         string    `json:"nik"`
	KTPPhotoURL string    `json:"ktp_photo_url"`
	SelfieURL   string    `json:"selfie_url"`
	KYCStatus   string    `json:"kyc_status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CustomerContact struct {
	CustomerID string    `json:"customer_id"`
	Phone      string    `json:"phone"`
	Email      string    `json:"email"`
	Address    string    `json:"address"`
	City       string    `json:"city"`
	Province   string    `json:"province"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CustomerAuth struct {
	CustomerID       string    `json:"customer_id"`
	PINHash          string    `json:"pin_hash"`
	BiometricEnabled bool      `json:"biometric_enabled"`
	DeviceID         string    `json:"device_id"`
	MPINAttempts     int       `json:"mpin_attempts"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type CustomerPreference struct {
	CustomerID     string    `json:"customer_id"`
	Language       string    `json:"language"`
	NotifEnabled   bool      `json:"notif_enabled"`
	DarkMode       bool      `json:"dark_mode"`
	DefaultAccount string    `json:"default_account"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

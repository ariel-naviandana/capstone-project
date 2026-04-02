package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Global semaphore untuk membatasi eksekusi http agar tidak meledak di memori saat DDOS
var ddosSemaphore chan struct{}

func InitDDosShield(maxConcurrent int) {
	ddosSemaphore = make(chan struct{}, maxConcurrent)
}

func DDosShield() gin.HandlerFunc {
	return func(c *gin.Context) {
		if ddosSemaphore == nil {
			c.Next()
			return
		}

		select {
		case ddosSemaphore <- struct{}{}:
			// Berhasil masuk ke dalam pool kapasitas
			defer func() { <-ddosSemaphore }()
			c.Next()
		default:
			// Jatuh ke sini jika koneksi / antrean di aplikasi sudah penuh (misal > 500 koneksi)
			// Berikan rejeksi langsung TANPA menyentuh logic, DB, maupun Redis
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error":   "Service Unavailable",
				"message": "Sistem sedang menghadapi traffic tinggi, silakan coba beberapa saat lagi.",
			})
			return
		}
	}
}

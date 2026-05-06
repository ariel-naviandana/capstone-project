package middleware

import (
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/infrastructure/observability"
	"github.com/gin-gonic/gin"
)

// Global semaphore untuk membatasi eksekusi http agar tidak meledak di memori saat DDOS
var ddosSemaphore chan struct{}

func InitDDosShield(maxConcurrent int) {
	if maxConcurrent <= 0 {
		ddosSemaphore = nil
		return
	}
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
			defer func() { <-ddosSemaphore }()
			c.Next()
		default:
			path := c.FullPath()
			if path == "" {
				path = "unknown"
			}
			observability.RequestsRejectedTotal.WithLabelValues("shield", path).Inc()
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error":   "Service Unavailable",
				"message": "Sistem sedang menghadapi traffic tinggi, silakan coba beberapa saat lagi.",
			})
			return
		}
	}
}

package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/gin-gonic/gin"
)

func RateLimiter() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetHeader("X-User-ID")
		var identifier string

		if userID != "" {
			identifier = "user:" + userID
		} else {
			identifier = "ip:" + c.ClientIP()
		}

		limit := config.AppConfig.RateLimitRequests
		if strings.Contains(c.FullPath(), "/transactions") && c.Request.Method == "POST" {
			limit = 50
		}

		key := "rate_limit:" + identifier + ":" + c.FullPath()

		count, err := cache.IncrementWithExpiry(c.Request.Context(), key, time.Duration(config.AppConfig.RateLimitWindow)*time.Second)
		if err != nil {
			c.Next()
			return
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(limit-count))
		c.Header("X-RateLimit-Reset", strconv.Itoa(int(time.Now().Unix())+config.AppConfig.RateLimitWindow))

		c.Header("X-RateLimit-Identifier", identifier)

		if count > limit {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "Too many requests",
				"message":     "Anda telah mencapai batas request, silakan coba lagi nanti",
				"retry_after": config.AppConfig.RateLimitWindow,
				"identifier":  identifier,
			})
			return
		}

		c.Next()
	}
}

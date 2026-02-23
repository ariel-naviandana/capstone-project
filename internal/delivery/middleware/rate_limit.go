package middleware

import (
	"log"
	"net/http"
	"strconv"
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
		windowSec := config.AppConfig.RateLimitWindow
		if c.FullPath() == "/transactions" && c.Request.Method == "POST" {
			limit = 50
		}

		key := "rate_limit:" + identifier + ":" + c.FullPath()

		count, err := cache.IncrementWithExpiry(c.Request.Context(), key, time.Duration(windowSec)*time.Second)
		if err != nil {
			log.Printf("Rate limit Redis error: %v", err)
			c.Next()
			return
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(limit-count))
		ttl, _ := cache.RedisClient.TTL(c.Request.Context(), key).Result()
		resetTime := time.Now().Add(ttl).Unix()
		c.Header("X-RateLimit-Reset", strconv.FormatInt(resetTime, 10))
		c.Header("X-RateLimit-Window", strconv.Itoa(windowSec))

		if count > limit {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "Too many requests",
				"message":     "Anda telah mencapai batas request, silakan coba lagi nanti",
				"retry_after": int(ttl.Seconds()),
				"identifier":  identifier,
			})
			return
		}

		c.Next()
	}
}

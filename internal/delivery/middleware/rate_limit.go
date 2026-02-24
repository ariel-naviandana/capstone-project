package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/gin-gonic/gin"
)

func RateLimiter() gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := logging.GetLogger(c)

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
			logger.Warn().
				Err(err).
				Str("key", key).
				Str("identifier", identifier).
				Str("path", c.FullPath()).
				Msg("Rate limit Redis error, continuing without limit check")
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
			logger.Warn().
				Int("count", count).
				Int("limit", limit).
				Str("identifier", identifier).
				Str("path", c.FullPath()).
				Msg("Rate limit exceeded")

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

package middleware

import (
	"fmt"
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

// CustomRecovery membungkus panic Gin standard ke dalam format JSON ErrorResponse
func CustomRecovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		logger := logging.GetLogger(c)
		
		errDetail := fmt.Sprintf("%v", recovered)
		logger.Error().
			Str("panic", errDetail).
			Msg("Telah terjadi panic (Internal Server Error)")

		// Hentikan request processing dengan HTTP 500
		c.AbortWithStatusJSON(http.StatusInternalServerError, response.ErrorJSON(
			response.ErrInternalError,
			"Terjadi kesalahan internal pada server",
			errDetail, // Dalam produksi sebaiknya tidak memaparkan detail panic. Tapi kita tampilkan di sini utk debug.
		))
	})
}

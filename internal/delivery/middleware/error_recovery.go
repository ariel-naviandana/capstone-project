package middleware

import (
	"fmt"
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

// CustomRecovery menangkap panic di handler downstream dan mengubahnya menjadi
// response JSON ErrorResponse standar (status 500, kode ERR_INTERNAL_ERROR).
// Menggunakan gin.CustomRecovery untuk integrasi dengan pipeline Gin
// sehingga stack trace juga dicetak ke stderr (default Gin behavior).
func CustomRecovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		logger := logging.GetLogger(c)

		errDetail := fmt.Sprintf("%v", recovered)
		logger.Error().
			Str("panic", errDetail).
			Str("path", c.FullPath()).
			Str("method", c.Request.Method).
			Msg("Panic recovered oleh CustomRecovery")

		c.AbortWithStatusJSON(http.StatusInternalServerError, response.ErrorJSON(
			response.ErrInternalError,
			"Terjadi kesalahan internal pada server",
			errDetail, // Dalam produksi sebaiknya tidak expose detail panic. Di sini untuk debug.
		))
	})
}

// NoRouteHandler dipanggil Gin saat tidak ada route yang cocok dengan path request.
// Return 404 dengan ErrorResponse standar supaya konsisten dengan error lain
// (daripada default Gin yang return plain text '404 page not found').
func NoRouteHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := logging.GetLogger(c)
		logger.Warn().
			Str("path", c.Request.URL.Path).
			Str("method", c.Request.Method).
			Msg("NoRoute: path tidak dikenal")

		response.AbortJSON(c, http.StatusNotFound,
			response.ErrNotFound,
			"Endpoint tidak ditemukan",
			c.Request.Method+" "+c.Request.URL.Path)
	}
}

// NoMethodHandler dipanggil Gin saat path match tapi HTTP method tidak sesuai
// (misal GET ke /transactions yang cuma register POST). Return 405.
// Aktif jika HandleMethodNotAllowed = true di engine.
func NoMethodHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := logging.GetLogger(c)
		logger.Warn().
			Str("path", c.Request.URL.Path).
			Str("method", c.Request.Method).
			Msg("NoMethod: HTTP method tidak diizinkan untuk path ini")

		response.AbortJSON(c, http.StatusMethodNotAllowed,
			response.ErrMethodNotAllowed,
			"HTTP method tidak diizinkan untuk endpoint ini",
			c.Request.Method+" "+c.Request.URL.Path)
	}
}

// GlobalErrorHandler adalah middleware post-processor yang berjalan setelah
// handler (via c.Next()). Jika handler menaruh error lewat c.Error(err) tapi
// BELUM menulis response body (c.Writer.Written() == false), middleware ini
// akan menulis response ErrorResponse standar 500 agar client tidak menerima
// body kosong.
//
// Kegunaan:
//   - Handler yang pakai pattern 'if err != nil { c.Error(err); return }'
//     tanpa explicit c.JSON(status, ...) tetap dapat response konsisten.
//   - Tidak menggantikan handler yang SUDAH menulis response sendiri
//     (tidak double-write).
func GlobalErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) == 0 {
			return
		}
		if c.Writer.Written() {
			// Handler sudah menulis response, cukup log errors tambahan
			// agar bisa diinvestigasi (tidak overwrite body client).
			logger := logging.GetLogger(c)
			logger.Warn().
				Int("error_count", len(c.Errors)).
				Str("errors", c.Errors.String()).
				Msg("GlobalErrorHandler: errors ada tapi response sudah ditulis, skip")
			return
		}

		// Response belum ditulis — kirim fallback 500
		last := c.Errors.Last()
		logger := logging.GetLogger(c)
		logger.Error().
			Err(last.Err).
			Str("path", c.FullPath()).
			Int("error_count", len(c.Errors)).
			Msg("GlobalErrorHandler: menulis fallback 500 karena handler tidak kirim response")

		c.JSON(http.StatusInternalServerError, response.ErrorJSON(
			response.ErrInternalError,
			"Terjadi kesalahan di server",
			last.Err.Error(),
		))
	}
}

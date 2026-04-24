package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

type SuccessResponse struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Global Error Codes
const (
	ErrInvalidInput       = "ERR_INVALID_INPUT"       // 400
	ErrUnauthorized       = "ERR_UNAUTHORIZED"        // 401
	ErrForbidden          = "ERR_FORBIDDEN"           // 403
	ErrNotFound           = "ERR_NOT_FOUND"           // 404
	ErrConflict           = "ERR_CONFLICT"            // 409
	ErrRateLimited        = "ERR_RATE_LIMITED"        // 429
	ErrInternalError      = "ERR_INTERNAL_ERROR"      // 500
	ErrServiceUnavailable = "ERR_SERVICE_UNAVAILABLE" // 503
)

// Global Success Codes
const (
	SuccessOK       = "SUCCESS_OK"
	SuccessCreated  = "SUCCESS_CREATED"
	SuccessAccepted = "SUCCESS_ACCEPTED"
)

// SuccessJSON — wrapper konsisten untuk response sukses.
func SuccessJSON(message string, data interface{}) SuccessResponse {
	return SuccessResponse{
		Message: message,
		Data:    data,
	}
}

// ErrorJSON — wrapper konsisten untuk response error.
func ErrorJSON(code string, message string, detail string) ErrorResponse {
	return ErrorResponse{
		Code:    code,
		Message: message,
		Detail:  detail,
	}
}

// StatusJSON menulis response error ke c.JSON dengan wrapper ErrorResponse.
// Tidak memanggil c.Abort — kalau butuh stop pipeline middleware, pakai AbortJSON.
func StatusJSON(c *gin.Context, status int, code, message, detail string) {
	c.JSON(status, ErrorJSON(code, message, detail))
}

// AbortJSON menulis response error + menghentikan pipeline middleware (c.Abort).
// Dipakai di middleware (rate limit, auth, shield) supaya handler downstream
// tidak ikut dieksekusi.
func AbortJSON(c *gin.Context, status int, code, message, detail string) {
	c.AbortWithStatusJSON(status, ErrorJSON(code, message, detail))
}

// GetCodeForStatus — map HTTP Status ke Error Code default.
// Menjamin konsistensi antara HTTP semantik dan application-level code
// saat caller tidak eksplisit menentukan code.
func GetCodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return ErrInvalidInput
	case http.StatusUnauthorized:
		return ErrUnauthorized
	case http.StatusForbidden:
		return ErrForbidden
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		return ErrConflict
	case http.StatusTooManyRequests:
		return ErrRateLimited
	case http.StatusServiceUnavailable:
		return ErrServiceUnavailable
	default:
		return ErrInternalError
	}
}

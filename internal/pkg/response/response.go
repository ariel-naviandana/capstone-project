package response

import "net/http"

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
	ErrInvalidInput       = "ERR_INVALID_INPUT"
	ErrNotFound           = "ERR_NOT_FOUND"
	ErrServiceUnavailable = "ERR_SERVICE_UNAVAILABLE"
	ErrInternalError      = "ERR_INTERNAL_ERROR"
	ErrUnauthorized       = "ERR_UNAUTHORIZED"
	ErrConflict           = "ERR_CONFLICT"
)

// Global Success Codes
const (
	SuccessOK       = "SUCCESS_OK"
	SuccessCreated  = "SUCCESS_CREATED"
	SuccessAccepted = "SUCCESS_ACCEPTED"
)

// Helper function untuk generate success response
func SuccessJSON(message string, data interface{}) SuccessResponse {
	return SuccessResponse{
		Message: message,
		Data:    data,
	}
}

// Helper function untuk generate error response
func ErrorJSON(code string, message string, detail string) ErrorResponse {
	return ErrorResponse{
		Code:    code,
		Message: message,
		Detail:  detail,
	}
}

// Map HTTP Status ke Error Code default
func GetCodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return ErrInvalidInput
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusServiceUnavailable:
		return ErrServiceUnavailable
	case http.StatusUnauthorized:
		return ErrUnauthorized
	case http.StatusConflict:
		return ErrConflict
	default:
		return ErrInternalError
	}
}

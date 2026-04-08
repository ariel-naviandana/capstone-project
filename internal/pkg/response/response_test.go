package response

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSuccessJSON(t *testing.T) {
	t.Run("With data", func(t *testing.T) {
		data := map[string]string{"key": "value"}
		resp := SuccessJSON("Operation successful", data)

		assert.Equal(t, "Operation successful", resp.Message)
		assert.Equal(t, data, resp.Data)
	})

	t.Run("With nil data", func(t *testing.T) {
		resp := SuccessJSON("No data", nil)

		assert.Equal(t, "No data", resp.Message)
		assert.Nil(t, resp.Data)
	})
}

func TestErrorJSON(t *testing.T) {
	t.Run("With detail", func(t *testing.T) {
		resp := ErrorJSON(ErrInvalidInput, "Invalid input", "field 'name' is required")

		assert.Equal(t, ErrInvalidInput, resp.Code)
		assert.Equal(t, "Invalid input", resp.Message)
		assert.Equal(t, "field 'name' is required", resp.Detail)
	})

	t.Run("Without detail", func(t *testing.T) {
		resp := ErrorJSON(ErrNotFound, "Not found", "")

		assert.Equal(t, ErrNotFound, resp.Code)
		assert.Equal(t, "Not found", resp.Message)
		assert.Empty(t, resp.Detail)
	})
}

func TestGetCodeForStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		expected string
	}{
		{"Bad Request", http.StatusBadRequest, ErrInvalidInput},
		{"Not Found", http.StatusNotFound, ErrNotFound},
		{"Service Unavailable", http.StatusServiceUnavailable, ErrServiceUnavailable},
		{"Unauthorized", http.StatusUnauthorized, ErrUnauthorized},
		{"Conflict", http.StatusConflict, ErrConflict},
		{"Internal Server Error", http.StatusInternalServerError, ErrInternalError},
		{"Unknown Status", http.StatusTeapot, ErrInternalError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code := GetCodeForStatus(tc.status)
			assert.Equal(t, tc.expected, code)
		})
	}
}

func TestErrorCodes(t *testing.T) {
	assert.Equal(t, "ERR_INVALID_INPUT", ErrInvalidInput)
	assert.Equal(t, "ERR_NOT_FOUND", ErrNotFound)
	assert.Equal(t, "ERR_SERVICE_UNAVAILABLE", ErrServiceUnavailable)
	assert.Equal(t, "ERR_INTERNAL_ERROR", ErrInternalError)
	assert.Equal(t, "ERR_UNAUTHORIZED", ErrUnauthorized)
	assert.Equal(t, "ERR_CONFLICT", ErrConflict)
}

func TestSuccessCodes(t *testing.T) {
	assert.Equal(t, "SUCCESS_OK", SuccessOK)
	assert.Equal(t, "SUCCESS_CREATED", SuccessCreated)
	assert.Equal(t, "SUCCESS_ACCEPTED", SuccessAccepted)
}

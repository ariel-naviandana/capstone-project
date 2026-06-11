package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSuccessJSON(t *testing.T) {
	res := SuccessJSON("ok", gin.H{"n": 1})
	assert.Equal(t, "ok", res.Message)
	assert.Equal(t, gin.H{"n": 1}, res.Data)
}

func TestErrorJSON(t *testing.T) {
	res := ErrorJSON(ErrInvalidInput, "bad", "detail here")
	assert.Equal(t, ErrInvalidInput, res.Code)
	assert.Equal(t, "bad", res.Message)
	assert.Equal(t, "detail here", res.Detail)
}

func TestErrorJSON_SerializationShape(t *testing.T) {
	// Kontrak JSON: field 'code', 'message', 'detail' (detail omitempty)
	raw, err := json.Marshal(ErrorJSON(ErrNotFound, "missing", ""))
	assert.NoError(t, err)
	var got map[string]any
	assert.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, ErrNotFound, got["code"])
	assert.Equal(t, "missing", got["message"])
	_, hasDetail := got["detail"]
	assert.False(t, hasDetail, "empty detail harus di-omit (omitempty)")
}

func TestGetCodeForStatus(t *testing.T) {
	cases := []struct {
		status int
		want   string
	}{
		{http.StatusBadRequest, ErrInvalidInput},
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusNotFound, ErrNotFound},
		{http.StatusConflict, ErrConflict},
		{http.StatusTooManyRequests, ErrRateLimited},
		{http.StatusServiceUnavailable, ErrServiceUnavailable},
		{http.StatusInternalServerError, ErrInternalError},
		{999, ErrInternalError}, // unknown status → default ErrInternalError
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, GetCodeForStatus(tc.status),
			"status=%d should map to %s", tc.status, tc.want)
	}
}

func TestStatusJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	StatusJSON(c, http.StatusBadRequest, ErrInvalidInput, "bad req", "field X")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var got ErrorResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, ErrInvalidInput, got.Code)
	assert.Equal(t, "bad req", got.Message)
	assert.Equal(t, "field X", got.Detail)
	// StatusJSON tidak meng-abort
	assert.False(t, c.IsAborted())
}

func TestAbortJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	AbortJSON(c, http.StatusUnauthorized, ErrUnauthorized, "need auth", "")

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	var got ErrorResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, ErrUnauthorized, got.Code)
	assert.Equal(t, "need auth", got.Message)
	// AbortJSON meng-abort (middleware downstream dilewati)
	assert.True(t, c.IsAborted())
}

func TestAbortJSON_StopsPipeline(t *testing.T) {
	// Verifikasi handler downstream TIDAK dieksekusi setelah AbortJSON.
	gin.SetMode(gin.TestMode)
	r := gin.New()
	reached := false
	r.GET("/x",
		func(c *gin.Context) {
			AbortJSON(c, http.StatusTooManyRequests, ErrRateLimited, "too many", "")
		},
		func(c *gin.Context) {
			reached = true
			c.JSON(http.StatusOK, gin.H{"ok": true})
		},
	)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.False(t, reached, "handler downstream tidak boleh dieksekusi setelah AbortJSON")
}

func TestErrorCodeConstants_StableContract(t *testing.T) {
	// Snapshot test: kode error adalah API public yang tidak boleh berubah diam2
	// (client mungkin switch-case di kode ini). Kalau ada perubahan di sini,
	// dokumentasikan sebagai breaking change.
	assert.Equal(t, "ERR_INVALID_INPUT", ErrInvalidInput)
	assert.Equal(t, "ERR_UNAUTHORIZED", ErrUnauthorized)
	assert.Equal(t, "ERR_FORBIDDEN", ErrForbidden)
	assert.Equal(t, "ERR_NOT_FOUND", ErrNotFound)
	assert.Equal(t, "ERR_CONFLICT", ErrConflict)
	assert.Equal(t, "ERR_RATE_LIMITED", ErrRateLimited)
	assert.Equal(t, "ERR_INTERNAL_ERROR", ErrInternalError)
	assert.Equal(t, "ERR_SERVICE_UNAVAILABLE", ErrServiceUnavailable)
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

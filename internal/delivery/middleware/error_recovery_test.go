package middleware

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/capstone-b4/capstone-go/internal/pkg/response"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

const testAnyPath = "/x"

func unmarshalErr(t *testing.T, body []byte) response.ErrorResponse {
	t.Helper()
	var er response.ErrorResponse
	assert.NoError(t, json.Unmarshal(body, &er))
	return er
}

// ─── CustomRecovery ─────────────────────────────────────────────────────

func TestCustomRecovery_CatchesPanic_WithErrorResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CustomRecovery())
	r.GET(testAnyPath, func(c *gin.Context) {
		panic("boom for test")
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testAnyPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	er := unmarshalErr(t, w.Body.Bytes())
	assert.Equal(t, response.ErrInternalError, er.Code)
	assert.Contains(t, er.Detail, "boom for test")
}

func TestCustomRecovery_NoPanic_Passthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CustomRecovery())
	r.GET(testAnyPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testAnyPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// ─── NoRouteHandler ─────────────────────────────────────────────────────

func TestNoRouteHandler_UnknownPath_Returns404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.NoRoute(NoRouteHandler())
	r.GET("/known", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/this-path-does-not-exist", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	er := unmarshalErr(t, w.Body.Bytes())
	assert.Equal(t, response.ErrNotFound, er.Code)
	assert.Contains(t, er.Detail, "GET /this-path-does-not-exist")
}

// ─── NoMethodHandler ────────────────────────────────────────────────────

func TestNoMethodHandler_WrongMethod_Returns405(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.NoMethod(NoMethodHandler())
	r.POST("/only-post", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/only-post", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	er := unmarshalErr(t, w.Body.Bytes())
	assert.Equal(t, response.ErrMethodNotAllowed, er.Code)
	assert.Contains(t, er.Detail, "GET /only-post")
}

// ─── GlobalErrorHandler ────────────────────────────────────────────────

func TestGlobalErrorHandler_HandlerErrorNoResponse_Writes500(t *testing.T) {
	// Skenario: handler c.Error(err) tapi tidak c.JSON → middleware harus
	// menulis fallback 500.
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GlobalErrorHandler())
	r.GET(testAnyPath, func(c *gin.Context) {
		_ = c.Error(errors.New("silent handler failure"))
		// NOTE: no c.JSON — middleware should write fallback
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testAnyPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	er := unmarshalErr(t, w.Body.Bytes())
	assert.Equal(t, response.ErrInternalError, er.Code)
	assert.Contains(t, er.Detail, "silent handler failure")
}

func TestGlobalErrorHandler_HandlerErrorButResponseAlreadyWritten_Passthrough(t *testing.T) {
	// Skenario: handler c.Error() DAN sudah c.JSON → middleware TIDAK overwrite.
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GlobalErrorHandler())
	r.GET(testAnyPath, func(c *gin.Context) {
		_ = c.Error(errors.New("logged error"))
		c.JSON(http.StatusBadRequest, response.ErrorJSON(
			response.ErrInvalidInput, "bad input", ""))
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testAnyPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	er := unmarshalErr(t, w.Body.Bytes())
	assert.Equal(t, response.ErrInvalidInput, er.Code)
	assert.Equal(t, "bad input", er.Message)
}

func TestGlobalErrorHandler_NoErrors_Passthrough(t *testing.T) {
	// Skenario: handler sukses, tidak ada c.Error → response normal.
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GlobalErrorHandler())
	r.GET(testAnyPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, response.SuccessJSON("ok", gin.H{"n": 1}))
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testAnyPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"message":"ok"`)
}

// ─── Integrasi lengkap: semua handler terdaftar ─────────────────────────

func TestErrorHandlers_FullWiring(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CustomRecovery())
	r.Use(GlobalErrorHandler())
	r.HandleMethodNotAllowed = true
	r.NoRoute(NoRouteHandler())
	r.NoMethod(NoMethodHandler())

	r.GET("/panic", func(c *gin.Context) { panic("full-wire panic") })
	r.GET("/silent", func(c *gin.Context) {
		_ = c.Error(errors.New("full-wire silent"))
	})
	r.POST("/post-only", func(c *gin.Context) { c.Status(http.StatusOK) })

	cases := []struct {
		name, method, path string
		status             int
		code               string
	}{
		{"Panic→500", "GET", "/panic", http.StatusInternalServerError, response.ErrInternalError},
		{"SilentErr→500", "GET", "/silent", http.StatusInternalServerError, response.ErrInternalError},
		{"UnknownPath→404", "GET", "/zzz", http.StatusNotFound, response.ErrNotFound},
		{"WrongMethod→405", "GET", "/post-only", http.StatusMethodNotAllowed, response.ErrMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(tc.method, tc.path, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.status, w.Code)
			er := unmarshalErr(t, w.Body.Bytes())
			assert.Equal(t, tc.code, er.Code, "code mismatch for %s", tc.name)
			assert.NotEmpty(t, er.Message)
		})
	}
}

package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/capstone-b4/capstone-go/internal/pkg/response"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestDDosShield_AllowsUnderCapacity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	InitDDosShield(2)
	r := gin.New()
	r.Use(DDosShield())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDDosShield_RejectsOverCapacity_WithErrorResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Capacity 1: isi manual supaya semaphore penuh, request berikutnya ditolak.
	InitDDosShield(1)
	ddosSemaphore <- struct{}{} // simulate 1 request sedang running
	defer func() { <-ddosSemaphore }()

	r := gin.New()
	r.Use(DDosShield())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	// Verifikasi bentuk ErrorResponse
	var got response.ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &got)
	assert.NoError(t, err)
	assert.Equal(t, response.ErrServiceUnavailable, got.Code)
	assert.NotEmpty(t, got.Message)
}

func TestDDosShield_NotInitialized_Passthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ddosSemaphore = nil // simulate not-initialized state

	r := gin.New()
	r.Use(DDosShield())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/x", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

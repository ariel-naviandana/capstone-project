package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestDDosShield_AllowsRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	InitDDosShield(10)

	r := gin.New()
	r.Use(DDosShield())
	r.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestDDosShield_NilSemaphore(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Reset semaphore to nil
	ddosSemaphore = nil

	r := gin.New()
	r.Use(DDosShield())
	r.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDDosShield_RejectsWhenFull(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create a semaphore with capacity 1 and fill it
	InitDDosShield(1)
	ddosSemaphore <- struct{}{} // Fill the only slot

	r := gin.New()
	r.Use(DDosShield())
	r.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "Service Unavailable")

	// Drain the semaphore for cleanup
	<-ddosSemaphore
}

func TestCustomRecovery_HandlesPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(CustomRecovery())
	r.GET("/panic", func(c *gin.Context) {
		panic("test panic!")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "ERR_INTERNAL_ERROR")
	assert.Contains(t, w.Body.String(), "test panic!")
}

func TestCustomRecovery_NormalRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(CustomRecovery())
	r.GET("/ok", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestInitDDosShield(t *testing.T) {
	InitDDosShield(50)
	assert.NotNil(t, ddosSemaphore)
	assert.Equal(t, 50, cap(ddosSemaphore))
}

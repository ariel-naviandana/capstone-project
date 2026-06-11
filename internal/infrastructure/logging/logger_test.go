package logging

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestGetLogger_NilContext(t *testing.T) {
	logger := GetLogger(nil)
	// Should return the global logger when context is nil
	assert.IsType(t, zerolog.Logger{}, logger)
}

func TestGetLogger_NoLoggerInContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	logger := GetLogger(c)
	// Should return global logger when no "logger" key exists in context
	assert.IsType(t, zerolog.Logger{}, logger)
}

func TestGetLogger_WithLoggerInContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	customLogger := log.With().Str("trace_id", "test-trace-123").Logger()
	c.Set("logger", customLogger)

	logger := GetLogger(c)
	assert.IsType(t, zerolog.Logger{}, logger)
}

func TestGetLogger_WrongTypeInContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

	// Set a non-logger value
	c.Set("logger", "not-a-logger")

	logger := GetLogger(c)
	// Should fall back to global logger
	assert.IsType(t, zerolog.Logger{}, logger)
}

func TestInitLogger(t *testing.T) {
	// Should not panic
	assert.NotPanics(t, func() {
		InitLogger()
	})
}

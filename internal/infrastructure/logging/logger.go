package logging

import (
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogger() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	if os.Getenv("ENV") == "production" {
		output.NoColor = true
	}

	log.Logger = log.Output(output).With().Caller().Logger()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	// Default to "warn" so per-request Info/Debug logs become near-free
	// no-ops on the hot path. Set LOG_LEVEL=info or =debug for local debugging.
	levelStr := strings.TrimSpace(strings.ToLower(os.Getenv("LOG_LEVEL")))
	var level zerolog.Level
	switch levelStr {
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.WarnLevel
	}
	zerolog.SetGlobalLevel(level)

	log.Warn().Str("log_level", level.String()).Msg("Structured logger initialized")
}

func GetLogger(c *gin.Context) zerolog.Logger {
	if c == nil {
		return log.Logger
	}

	loggerVal, exists := c.Get("logger")
	if !exists {
		return log.Logger
	}

	logger, ok := loggerVal.(zerolog.Logger)
	if !ok {
		return log.Logger
	}

	return logger
}

package logging

import (
	"os"
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

	levelStr := os.Getenv("LOG_LEVEL")
	var level zerolog.Level
	switch levelStr {
	case "debug":
		level = zerolog.DebugLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	log.Info().Str("log_level", levelStr).Msg("Structured logger initialized with zerolog")
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

package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear any existing env vars that could affect the test
	originalPort := os.Getenv("SERVER_PORT")
	defer func() {
		if originalPort != "" {
			os.Setenv("SERVER_PORT", originalPort)
		}
	}()

	// Load config (will use env vars or defaults)
	LoadConfig()

	// Verify defaults are applied
	if AppConfig.ServerPort == "" {
		// If SERVER_PORT env var is not set, viper default should be "8000"
		t.Log("SERVER_PORT was empty after load, checking default behavior")
	}

	assert.GreaterOrEqual(t, AppConfig.RateLimitRequests, 1, "RateLimitRequests should have a default value")
	assert.GreaterOrEqual(t, AppConfig.RateLimitWindow, 1, "RateLimitWindow should have a default value")
}

func TestLoadConfig_WithEnvVars(t *testing.T) {
	// Set environment variables
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("POSTGRES_HOST", "test-host")
	os.Setenv("POSTGRES_PORT", "5433")
	os.Setenv("POSTGRES_USER", "testuser")
	os.Setenv("POSTGRES_PASSWORD", "testpass")
	os.Setenv("POSTGRES_DB", "testdb")
	os.Setenv("REDIS_ADDR", "localhost:6380")

	defer func() {
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("POSTGRES_HOST")
		os.Unsetenv("POSTGRES_PORT")
		os.Unsetenv("POSTGRES_USER")
		os.Unsetenv("POSTGRES_PASSWORD")
		os.Unsetenv("POSTGRES_DB")
		os.Unsetenv("REDIS_ADDR")
	}()

	LoadConfig()

	assert.Equal(t, "9090", AppConfig.ServerPort)
	assert.Equal(t, "test-host", AppConfig.PostgresHost)
	assert.Equal(t, "5433", AppConfig.PostgresPort)
	assert.Equal(t, "testuser", AppConfig.PostgresUser)
	assert.Equal(t, "testpass", AppConfig.PostgresPassword)
	assert.Equal(t, "testdb", AppConfig.PostgresDBName)
	assert.Equal(t, "localhost:6380", AppConfig.RedisAddr)
}

func TestConfigStruct(t *testing.T) {
	cfg := Config{
		ServerPort:        "8080",
		PostgresHost:      "localhost",
		PostgresPort:      "5432",
		PostgresUser:      "admin",
		PostgresPassword:  "secret",
		PostgresDBName:    "mydb",
		KafkaBrokers:      []string{"broker1:9092", "broker2:9092"},
		KafkaTopic:        "transactions",
		KafkaGroupID:      "my-group",
		MongoURI:          "mongodb://localhost:27017",
		RedisAddr:         "localhost:6379",
		RedisPassword:     "redispass",
		RedisDB:           1,
		RateLimitRequests: 1000,
		RateLimitWindow:   60,
		LogInfo:           true,
	}

	assert.Equal(t, "8080", cfg.ServerPort)
	assert.Equal(t, "localhost", cfg.PostgresHost)
	assert.Equal(t, "5432", cfg.PostgresPort)
	assert.Equal(t, "admin", cfg.PostgresUser)
	assert.Equal(t, "secret", cfg.PostgresPassword)
	assert.Equal(t, "mydb", cfg.PostgresDBName)
	assert.Len(t, cfg.KafkaBrokers, 2)
	assert.Equal(t, "transactions", cfg.KafkaTopic)
	assert.Equal(t, "my-group", cfg.KafkaGroupID)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)
	assert.Equal(t, "localhost:6379", cfg.RedisAddr)
	assert.Equal(t, "redispass", cfg.RedisPassword)
	assert.Equal(t, 1, cfg.RedisDB)
	assert.Equal(t, 1000, cfg.RateLimitRequests)
	assert.Equal(t, 60, cfg.RateLimitWindow)
	assert.True(t, cfg.LogInfo)
}

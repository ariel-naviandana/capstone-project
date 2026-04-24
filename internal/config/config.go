package config

import (
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type Config struct {
	ServerPort string `mapstructure:"SERVER_PORT"`

	PostgresHost     string `mapstructure:"POSTGRES_HOST"`
	PostgresPort     string `mapstructure:"POSTGRES_PORT"`
	PostgresUser     string `mapstructure:"POSTGRES_USER"`
	PostgresPassword string `mapstructure:"POSTGRES_PASSWORD"`
	PostgresDBName   string `mapstructure:"POSTGRES_DB"`

	KafkaBrokers []string `mapstructure:"KAFKA_BROKERS"`
	KafkaTopic   string   `mapstructure:"KAFKA_TOPIC"`
	KafkaGroupID string   `mapstructure:"KAFKA_GROUP_ID"`

	MongoURI string `mapstructure:"MONGO_URI"`

	RedisAddr     string `mapstructure:"REDIS_ADDR"`
	RedisPassword string `mapstructure:"REDIS_PASSWORD"`
	RedisDB       int    `mapstructure:"REDIS_DB"`

	RateLimitRequests int `mapstructure:"RATE_LIMIT_REQUESTS"`
	RateLimitWindow   int `mapstructure:"RATE_LIMIT_WINDOW"`

	JWTSecret     string `mapstructure:"JWT_SECRET"`
	JWTExpiryHour int    `mapstructure:"JWT_EXPIRY_HOUR"`

	LogInfo bool `mapstructure:"LOG_INFO"`
}

var AppConfig Config

func LoadConfig() {
	if err := godotenv.Load(".env"); err != nil {
		log.Warn().Err(err).Msg("Warning: godotenv.Load failed (lanjut tanpa .env file)")
	} else {
		log.Info().Msg("godotenv successfully loaded .env")
	}

	viper.AutomaticEnv()

	// Set Default Values
	viper.SetDefault("SERVER_PORT", "8000")
	viper.SetDefault("RATE_LIMIT_REQUESTS", 25000)
	viper.SetDefault("RATE_LIMIT_WINDOW", 60)
	viper.SetDefault("JWT_EXPIRY_HOUR", 24)

	// Bind Environment Variables for Viper Unmarshal
	_ = viper.BindEnv("POSTGRES_HOST")
	_ = viper.BindEnv("POSTGRES_PORT")
	_ = viper.BindEnv("POSTGRES_USER")
	_ = viper.BindEnv("POSTGRES_PASSWORD")
	_ = viper.BindEnv("POSTGRES_DB")
	_ = viper.BindEnv("KAFKA_BROKERS")
	_ = viper.BindEnv("KAFKA_TOPIC")
	_ = viper.BindEnv("KAFKA_GROUP_ID")
	_ = viper.BindEnv("MONGO_URI")
	_ = viper.BindEnv("REDIS_ADDR")
	_ = viper.BindEnv("REDIS_PASSWORD")
	_ = viper.BindEnv("REDIS_DB")
	_ = viper.BindEnv("JWT_SECRET")
	_ = viper.BindEnv("JWT_EXPIRY_HOUR")
	_ = viper.BindEnv("LOG_INFO")

	if err := viper.Unmarshal(&AppConfig); err != nil {
		log.Fatal().Err(err).Msg("Config unmarshal error")
	}

	if AppConfig.LogInfo {
		log.Info().
			Str("server_port", AppConfig.ServerPort).
			Str("postgres_host", AppConfig.PostgresHost).
			Str("postgres_db", AppConfig.PostgresDBName).
			Strs("kafka_brokers", AppConfig.KafkaBrokers).
			Str("kafka_topic", AppConfig.KafkaTopic).
			Str("mongo_uri", AppConfig.MongoURI).
			Str("redis_addr", AppConfig.RedisAddr).
			Msg("Config loaded successfully with details")
	} else {
		log.Info().Msg("Config loaded successfully (detail skipped)")
	}
}

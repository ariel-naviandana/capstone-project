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

	viper.SetDefault("SERVER_PORT", "8000")
	viper.BindEnv("SERVER_PORT")

	viper.BindEnv("POSTGRES_HOST")
	viper.BindEnv("POSTGRES_PORT")
	viper.BindEnv("POSTGRES_USER")
	viper.BindEnv("POSTGRES_PASSWORD")
	viper.BindEnv("POSTGRES_DB")

	viper.BindEnv("KAFKA_BROKERS")
	viper.BindEnv("KAFKA_TOPIC")
	viper.BindEnv("KAFKA_GROUP_ID")
	viper.BindEnv("MONGO_URI")

	viper.BindEnv("REDIS_ADDR")
	viper.BindEnv("REDIS_PASSWORD")
	viper.BindEnv("REDIS_DB")

	viper.SetDefault("RATE_LIMIT_REQUESTS", 100)
	viper.SetDefault("RATE_LIMIT_WINDOW", 60)
	viper.BindEnv("RATE_LIMIT_REQUESTS")
	viper.BindEnv("RATE_LIMIT_WINDOW")

	viper.BindEnv("LOG_INFO")

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

package config

import (
	"log"

	"github.com/joho/godotenv"
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
}

var AppConfig Config

func LoadConfig() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("Warning: godotenv.Load failed: %v", err)
	} else {
		log.Println("godotenv successfully loaded .env")
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

	if err := viper.Unmarshal(&AppConfig); err != nil {
		log.Fatalf("Unmarshal error: %v", err)
	}

	log.Printf("Loaded SERVER_PORT: [%s]", AppConfig.ServerPort)
	log.Printf("Loaded POSTGRES_HOST: [%s]", AppConfig.PostgresHost)
	log.Printf("Loaded POSTGRES_DB: [%s]", AppConfig.PostgresDBName)
	log.Printf("Loaded KAFKA_BROKERS: %v", AppConfig.KafkaBrokers)
	log.Printf("Loaded KAFKA_TOPIC: [%s]", AppConfig.KafkaTopic)
	log.Printf("Loaded MONGO_URI: [%s]", AppConfig.MongoURI)
	log.Printf("Loaded REDIS_ADDR: [%s]", AppConfig.RedisAddr)

	log.Println("Config loaded successfully")
}

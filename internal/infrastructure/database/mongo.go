package database

import (
	"context"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoClient *mongo.Client
var TransactionLogCollection *mongo.Collection

func ConnectMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.AppConfig.MongoURI
	if uri == "" {
		log.Fatal().Msg("MONGO_URI tidak ditemukan di config")
	}

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("Gagal connect Mongo")
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("Mongo ping gagal")
	}

	MongoClient = client
	TransactionLogCollection = client.Database("capstone").Collection("transaction_logs")

	ensureIndexes()

	log.Info().Msg("Connected to MongoDB & indexes ensured")
}

func ensureIndexes() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "tx_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "timestamp", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "timestamp", Value: -1}},
		},
	}

	_, err := TransactionLogCollection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("Gagal create index di Mongo (lanjut tanpa index baru)")
		return
	}

	log.Info().
		Msg("Mongo indexes berhasil dibuat/diperiksa (tx_id, timestamp, status, user_id)")
}

func CloseMongo() {
	if MongoClient != nil {
		if err := MongoClient.Disconnect(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Error disconnecting from MongoDB")
		}
		log.Info().Msg("MongoDB disconnected")
	}
}

func LogToMongo(event domain.KafkaTransactionEvent, status string, details string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := bson.M{
		"tx_id":     event.TxID,
		"user_id":   event.UserID,
		"type":      event.Type,
		"amount":    event.Amount,
		"status":    status,
		"details":   details,
		"timestamp": time.Now().UTC(),
	}

	err := resilience.RetryWithBackoff(ctx, func() error {
		_, err := TransactionLogCollection.InsertOne(ctx, doc)
		return err
	}, 3, 1*time.Second)

	if err != nil {
		log.Warn().
			Err(err).
			Str("tx_id", event.TxID).
			Str("status", status).
			Msg("Gagal log ke Mongo (breaker)")
	} else {
		log.Info().
			Str("tx_id", event.TxID).
			Str("status", status).
			Msg("Logged to Mongo")
	}
}

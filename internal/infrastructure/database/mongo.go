package database

import (
	"context"
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoClient *mongo.Client
var TransactionLogCollection *mongo.Collection

func ConnectMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(config.AppConfig.MongoURI) // tambah di config nanti
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("Gagal connect Mongo: ", err)
	}

	// Ping untuk test koneksi
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Mongo ping gagal: ", err)
	}

	MongoClient = client
	TransactionLogCollection = client.Database("capstone").Collection("transaction_logs")

	log.Println("Connected to MongoDB")
}

func CloseMongo() {
	if MongoClient != nil {
		MongoClient.Disconnect(context.Background())
		log.Println("MongoDB disconnected")
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

	_, err := TransactionLogCollection.InsertOne(ctx, doc)
	if err != nil {
		log.Printf("Gagal log ke Mongo: %v", err)
	}
}

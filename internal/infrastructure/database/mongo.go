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

	uri := config.AppConfig.MongoURI
	if uri == "" {
		log.Fatal("MONGO_URI tidak ditemukan di config")
	}

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Gagal connect Mongo: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Mongo ping gagal: %v", err)
	}

	MongoClient = client
	TransactionLogCollection = client.Database("capstone").Collection("transaction_logs")

	// Otomatis buat index saat connect
	ensureIndexes()

	log.Println("Connected to MongoDB & indexes ensured")
}

// Fungsi auto-create index (idempotent)
func ensureIndexes() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Index untuk query cepat
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "tx_id", Value: 1}},
			Options: options.Index().SetUnique(true), // optional unique kalau tx_id unik
		},
		{
			Keys: bson.D{{Key: "timestamp", Value: -1}}, // recent first
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "timestamp", Value: -1}}, // compound untuk filter per user + recent
		},
	}

	_, err := TransactionLogCollection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Printf("Gagal create index di Mongo: %v (lanjut tanpa index baru)", err)
		return
	}

	log.Println("Mongo indexes berhasil dibuat/diperiksa (tx_id, timestamp, status, user_id)")
}

func CloseMongo() {
	if MongoClient != nil {
		MongoClient.Disconnect(context.Background())
		log.Println("MongoDB disconnected")
	}
}

// Fungsi LogToMongo tetap sama seperti sebelumnya
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
	} else {
		log.Printf("Logged to Mongo: tx_id=%d, status=%s", event.TxID, status)
	}
}

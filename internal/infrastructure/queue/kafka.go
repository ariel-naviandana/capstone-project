package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

func InitKafkaProducer() {
	brokers := config.AppConfig.KafkaBrokers
	if len(brokers) == 0 {
		log.Fatal("KAFKA_BROKERS tidak ditemukan di config")
	}

	// Auto-create topic kalau belum ada
	createTopicIfNotExist(brokers[0], config.AppConfig.KafkaTopic)

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    config.AppConfig.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}

	log.Printf("Kafka producer initialized dengan brokers: %v, topic: %s", brokers, config.AppConfig.KafkaTopic)
}

// Fungsi auto-create topic kalau belum ada (dipanggil sekali saat init)
func createTopicIfNotExist(broker string, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("Gagal connect ke broker untuk check/create topic: %v", err)
		return // tidak fatal, biar app tetap jalan
	}
	defer conn.Close()

	// Cek apakah topic sudah ada
	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		log.Printf("Topic %s sudah ada", topic)
		return
	}

	// Buat topic kalau belum ada
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("Gagal auto-create topic %s: %v (lanjut tanpa topic)", topic, err)
		return
	}

	log.Printf("Topic %s berhasil dibuat otomatis", topic)
}

func PublishTransactionEvent(txID int64, userID int64, recipientID int64, amount float64, txType string) error {
	if kafkaWriter == nil {
		return fmt.Errorf("kafka writer belum di-init")
	}

	message := map[string]interface{}{
		"tx_id":        txID,
		"user_id":      userID,
		"recipient_id": recipientID,
		"amount":       amount,
		"type":         txType,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("gagal marshal message: %v", err)
	}

	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", txID)),
			Value: data,
		},
	)
	if err != nil {
		return fmt.Errorf("gagal publish ke Kafka: %v", err)
	}

	log.Printf("Berhasil publish event transaksi ke Kafka: tx_id=%d, type=%s", txID, txType)
	return nil
}

func CloseKafkaProducer() {
	if kafkaWriter != nil {
		kafkaWriter.Close()
		log.Println("Kafka producer ditutup")
	}
}

// ... kode producer tetap

var kafkaReader *kafka.Reader

func StartKafkaConsumer() {
	brokers := config.AppConfig.KafkaBrokers
	if len(brokers) == 0 {
		log.Fatal("KAFKA_BROKERS tidak ditemukan")
	}

	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "transaction-worker-group",
		Topic:    config.AppConfig.KafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Printf("Kafka consumer started, group: transaction-worker-group, topic: %s", config.AppConfig.KafkaTopic)

	for {
		msg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error read message: %v", err)
			time.Sleep(1 * time.Second) // backoff
			continue
		}

		var event domain.KafkaTransactionEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshal: %v", err)
			continue
		}

		log.Printf("Received event: tx_id=%d, type=%s", event.TxID, event.Type)

		// Proses real
		processTransactionEvent(&event)

		// Commit offset
		kafkaReader.CommitMessages(context.Background(), msg)
	}
}

func processTransactionEvent(event *domain.KafkaTransactionEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Ambil transaction & user dari DB
	var tx domain.Transaction
	err := database.PostgresPool.QueryRow(ctx,
		"SELECT id, user_id, recipient_id, amount, type, status FROM transactions WHERE id = $1",
		event.TxID).Scan(&tx.ID, &tx.UserID, &tx.RecipientID, &tx.Amount, &tx.Type, &tx.Status)
	if err != nil {
		log.Printf("Gagal ambil tx %d: %v", event.TxID, err)
		database.LogToMongo(*event, "failed", "Transaction not found")
		return
	}

	var userBalance float64
	err = database.PostgresPool.QueryRow(ctx,
		"SELECT balance FROM users WHERE id = $1 FOR UPDATE", // lock row untuk atomic
		event.UserID).Scan(&userBalance)
	if err != nil {
		log.Printf("Gagal ambil saldo user %d: %v", event.UserID, err)
		database.LogToMongo(*event, "failed", "User not found")
		return
	}

	var recipientBalance float64
	if event.Type == "transfer" && event.RecipientID != 0 {
		err = database.PostgresPool.QueryRow(ctx,
			"SELECT balance FROM users WHERE id = $1 FOR UPDATE",
			event.RecipientID).Scan(&recipientBalance)
		if err != nil {
			log.Printf("Gagal ambil saldo recipient %d: %v", event.RecipientID, err)
			database.LogToMongo(*event, "failed", "Recipient not found")
			return
		}
	}

	// 2. Proses berdasarkan type
	var newStatus string = "success"
	var details string

	switch event.Type {
	case "deposit":
		userBalance += event.Amount
		details = fmt.Sprintf("Deposit +%.2f", event.Amount)

	case "withdraw":
		if userBalance < event.Amount {
			newStatus = "failed"
			details = "Saldo tidak cukup"
		} else {
			userBalance -= event.Amount
			details = fmt.Sprintf("Withdraw -%.2f", event.Amount)
		}

	case "transfer":
		if userBalance < event.Amount {
			newStatus = "failed"
			details = "Saldo pengirim tidak cukup"
		} else {
			userBalance -= event.Amount
			recipientBalance += event.Amount
			details = fmt.Sprintf("Transfer %.2f ke user %d", event.Amount, event.RecipientID)
		}

	default:
		newStatus = "failed"
		details = "Type tidak dikenal"
	}

	// 3. Update DB atomic (dalam transaction)
	txDB, err := database.PostgresPool.Begin(ctx)
	if err != nil {
		log.Printf("Gagal begin tx DB: %v", err)
		return
	}
	defer txDB.Rollback(ctx)

	// Update balance user
	_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", userBalance, event.UserID)
	if err != nil {
		log.Printf("Gagal update balance user: %v", err)
		return
	}

	// Update balance recipient kalau transfer
	if event.Type == "transfer" && event.RecipientID != 0 {
		_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", recipientBalance, event.RecipientID)
		if err != nil {
			log.Printf("Gagal update balance recipient: %v", err)
			return
		}
	}

	// Update status transaction
	_, err = txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE id = $2", newStatus, event.TxID)
	if err != nil {
		log.Printf("Gagal update status tx: %v", err)
		return
	}

	if err := txDB.Commit(ctx); err != nil {
		log.Printf("Gagal commit tx DB: %v", err)
		return
	}

	// 4. Log ke Mongo
	database.LogToMongo(*event, newStatus, details)

	log.Printf("Processed tx_id=%d: status=%s, details=%s", event.TxID, newStatus, details)
}

func CloseKafkaConsumer() {
	if kafkaReader != nil {
		kafkaReader.Close()
		log.Println("Kafka consumer closed")
	}
}

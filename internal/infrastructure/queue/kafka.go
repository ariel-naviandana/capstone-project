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

func createTopicIfNotExist(broker string, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("Gagal connect ke broker untuk check/create topic: %v", err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		log.Printf("Topic %s sudah ada", topic)
		return
	}

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

func PublishTransactionEvent(txID string, userID int64, recipientID int64, amount float64, txType string) error {
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
			Key:   []byte(txID),
			Value: data,
		},
	)
	if err != nil {
		return fmt.Errorf("gagal publish ke Kafka: %v", err)
	}

	log.Printf("Berhasil publish event transaksi ke Kafka: tx_id=%s, type=%s", txID, txType)
	return nil
}

func CloseKafkaProducer() {
	if kafkaWriter != nil {
		kafkaWriter.Close()
		log.Println("Kafka producer ditutup")
	}
}

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
			time.Sleep(1 * time.Second)
			continue
		}

		var event domain.KafkaTransactionEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshal: %v", err)
			continue
		}

		log.Printf("Received event: tx_id=%s, type=%s", event.TxID, event.Type)

		processTransactionEvent(&event)

		kafkaReader.CommitMessages(context.Background(), msg)
	}
}

func processTransactionEvent(event *domain.KafkaTransactionEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Cek apakah tx sudah diproses (idempotent)
	var existingTxID string
	err := database.PostgresPool.QueryRow(ctx,
		"SELECT tx_id FROM transactions WHERE tx_id = $1", event.TxID).Scan(&existingTxID)
	if err == nil {
		log.Printf("Tx %s sudah diproses sebelumnya, skip", event.TxID)
		return
	}

	// 2. Insert transaction dengan status pending
	_, err = database.PostgresPool.Exec(ctx,
		`INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, 'pending', NOW(), NOW())`,
		event.TxID, event.UserID, event.RecipientID, event.Amount, event.Type)
	if err != nil {
		log.Printf("Gagal insert tx pending %s: %v", event.TxID, err)
		database.LogToMongo(*event, "failed", "Insert pending gagal")
		return
	}

	// 3. Cek saldo user
	var userBalance float64
	err = database.PostgresPool.QueryRow(ctx,
		"SELECT balance FROM users WHERE id = $1 FOR UPDATE",
		event.UserID).Scan(&userBalance)
	if err != nil {
		log.Printf("Gagal ambil saldo user %d: %v", event.UserID, err)
		database.LogToMongo(*event, "failed", "User not found")
		return
	}

	// 4. Cek saldo recipient kalau transfer
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

	// 5. Proses berdasarkan type
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

	// 6. Update DB atomic
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
	_, err = txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE tx_id = $2", newStatus, event.TxID)
	if err != nil {
		log.Printf("Gagal update status tx: %v", err)
		return
	}

	if err := txDB.Commit(ctx); err != nil {
		log.Printf("Gagal commit tx DB: %v", err)
		return
	}

	// 7. Log ke Mongo
	database.LogToMongo(*event, newStatus, details)

	log.Printf("Processed tx_id=%s: status=%s, details=%s", event.TxID, newStatus, details)
}

func CloseKafkaConsumer() {
	if kafkaReader != nil {
		kafkaReader.Close()
		log.Println("Kafka consumer closed")
	}
}

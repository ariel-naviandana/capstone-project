package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

func InitKafkaProducer() {
	brokers := config.AppConfig.KafkaBrokers
	if len(brokers) == 0 {
		log.Fatal("KAFKA_BROKERS tidak ditemukan di config")
	}

	createTopicIfNotExist(brokers[0], config.AppConfig.KafkaTopic)

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    config.AppConfig.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	}

	log.Printf("Kafka producer initialized dengan brokers: %v, topic: %s", brokers, config.AppConfig.KafkaTopic)
}

func createTopicIfNotExist(broker string, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("Gagal connect ke broker: %v", err)
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
		log.Printf("Gagal auto-create topic %s: %v", topic, err)
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
		return fmt.Errorf("gagal marshal: %v", err)
	}

	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(txID),
			Value: data,
		},
	)
	if err != nil {
		log.Printf("Gagal publish ke Kafka: %v", err)
		return fmt.Errorf("gagal publish ke Kafka: %w", err)
	}

	log.Printf("Berhasil publish: tx_id=%s, type=%s", txID, txType)
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
		msg, err := resilience.ExecuteWithBreaker[kafka.Message](
			context.Background(),
			resilience.KafkaConsumerBreaker,
			"KafkaRead",
			func() (kafka.Message, error) {
				return kafkaReader.ReadMessage(context.Background())
			},
		)
		if err != nil {
			log.Printf("Kafka read ditolak breaker: %v", err)
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

	cacheKey := "tx:" + event.TxID

	pendingDetail := domain.TransactionDetail{
		TxID:        event.TxID,
		UserID:      event.UserID,
		RecipientID: event.RecipientID,
		Amount:      event.Amount,
		Type:        event.Type,
		Status:      "pending",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	cache.SetCache(ctx, cacheKey, pendingDetail, 5*time.Minute)

	var existing string
	err := database.PostgresPool.QueryRow(ctx,
		"SELECT tx_id FROM transactions WHERE tx_id = $1", event.TxID).Scan(&existing)
	if err == nil {
		log.Printf("Tx %s sudah diproses sebelumnya", event.TxID)
		return
	}

	_, err = database.PostgresPool.Exec(ctx,
		`INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, 'pending', NOW(), NOW())`,
		event.TxID, event.UserID, event.RecipientID, event.Amount, event.Type)
	if err != nil {
		log.Printf("Gagal insert pending %s: %v", event.TxID, err)
		database.LogToMongo(*event, "failed", "Insert pending gagal")
		return
	}

	var userBalance float64
	err = database.PostgresPool.QueryRow(ctx,
		"SELECT balance FROM users WHERE id = $1 FOR UPDATE", event.UserID).Scan(&userBalance)
	if err != nil {
		log.Printf("Gagal ambil saldo user %d: %v", event.UserID, err)
		database.LogToMongo(*event, "failed", "User not found")
		return
	}

	var recipientBalance float64
	if event.Type == "transfer" && event.RecipientID != 0 {
		err = database.PostgresPool.QueryRow(ctx,
			"SELECT balance FROM users WHERE id = $1 FOR UPDATE", event.RecipientID).Scan(&recipientBalance)
		if err != nil {
			log.Printf("Gagal ambil saldo recipient %d: %v", event.RecipientID, err)
			database.LogToMongo(*event, "failed", "Recipient not found")
			return
		}
	}

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

	txDB, err := database.PostgresPool.Begin(ctx)
	if err != nil {
		log.Printf("Gagal begin tx: %v", err)
		return
	}
	defer txDB.Rollback(ctx)

	_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", userBalance, event.UserID)
	if err != nil {
		log.Printf("Gagal update balance user: %v", err)
		return
	}

	if event.Type == "transfer" && event.RecipientID != 0 {
		_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", recipientBalance, event.RecipientID)
		if err != nil {
			log.Printf("Gagal update balance recipient: %v", err)
			return
		}
	}

	_, err = txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE tx_id = $2", newStatus, event.TxID)
	if err != nil {
		log.Printf("Gagal update status: %v", err)
		return
	}

	if err := txDB.Commit(ctx); err != nil {
		log.Printf("Gagal commit: %v", err)
		return
	}

	finalDetail := domain.TransactionDetail{
		TxID:        event.TxID,
		UserID:      event.UserID,
		RecipientID: event.RecipientID,
		Amount:      event.Amount,
		Type:        event.Type,
		Status:      newStatus,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	cache.SetCache(ctx, cacheKey, finalDetail, 5*time.Minute)

	database.LogToMongo(*event, newStatus, details)

	log.Printf("Processed tx_id=%s: status=%s, details=%s", event.TxID, newStatus, details)
}

func CloseKafkaConsumer() {
	if kafkaReader != nil {
		kafkaReader.Close()
		log.Println("Kafka consumer closed")
	}
}

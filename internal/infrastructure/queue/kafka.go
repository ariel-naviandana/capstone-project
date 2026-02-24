package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

func InitKafkaProducer() {
	brokers := config.AppConfig.KafkaBrokers
	if len(brokers) == 0 {
		log.Fatal().Msg("KAFKA_BROKERS tidak ditemukan di config")
	}

	createTopicIfNotExist(brokers[0], config.AppConfig.KafkaTopic)

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    config.AppConfig.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	}

	log.Info().
		Strs("brokers", brokers).
		Str("topic", config.AppConfig.KafkaTopic).
		Msg("Kafka producer initialized")
}

func createTopicIfNotExist(broker string, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Warn().Err(err).Str("broker", broker).Msg("Gagal connect ke broker untuk create topic")
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		log.Info().Str("topic", topic).Msg("Topic sudah ada")
		return
	}

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Warn().Err(err).Str("topic", topic).Msg("Gagal auto-create topic")
		return
	}

	log.Info().Str("topic", topic).Msg("Topic berhasil dibuat otomatis")
}

func PublishTransactionEvent(txID string, userID int64, recipientID int64, amount float64, txType string, traceID string) error {
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
		log.Error().Err(err).Msg("Gagal marshal message untuk Kafka")
		return fmt.Errorf("gagal marshal: %w", err)
	}

	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(txID),
			Value: data,
			Headers: []kafka.Header{
				{Key: "trace_id", Value: []byte(traceID)},
			},
		},
	)
	if err != nil {
		log.Warn().
			Err(err).
			Str("tx_id", txID).
			Str("type", txType).
			Msg("Gagal publish ke Kafka")
		return fmt.Errorf("gagal publish ke Kafka: %w", err)
	}

	log.Debug().
		Str("tx_id", txID).
		Str("trace_id_sent", traceID).
		Msg("Headers dikirim ke Kafka (trace_id)")

	log.Info().
		Str("tx_id", txID).
		Str("type", txType).
		Str("trace_id", traceID).
		Msg("Berhasil publish ke Kafka")

	return nil
}

func CloseKafkaProducer() {
	if kafkaWriter != nil {
		kafkaWriter.Close()
		log.Info().Msg("Kafka producer ditutup")
	}
}

var kafkaReader *kafka.Reader

const maxConcurrent = 10

var processSemaphore = make(chan struct{}, maxConcurrent)

func StartKafkaConsumer() {
	brokers := config.AppConfig.KafkaBrokers
	if len(brokers) == 0 {
		log.Fatal().Msg("KAFKA_BROKERS tidak ditemukan")
	}

	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "transaction-worker-group",
		Topic:    config.AppConfig.KafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Info().
		Str("group_id", "transaction-worker-group").
		Str("topic", config.AppConfig.KafkaTopic).
		Int("max_concurrent", maxConcurrent).
		Msg("Kafka consumer started")

	for {
		var msg kafka.Message

		_, breakerErr := resilience.ExecuteWithBreaker[kafka.Message](
			context.Background(),
			resilience.KafkaConsumerBreaker,
			"KafkaRead",
			func() (kafka.Message, error) {
				retryErr := resilience.RetryWithBackoff(context.Background(), func() error {
					var innerErr error
					msg, innerErr = kafkaReader.ReadMessage(context.Background())
					return innerErr
				}, 3, 500*time.Millisecond)

				return msg, retryErr
			},
		)

		if breakerErr != nil {
			log.Warn().
				Err(breakerErr).
				Msg("Kafka read ditolak breaker, sleep 1s")
			time.Sleep(1 * time.Second)
			continue
		}

		processSemaphore <- struct{}{}

		go func(msg kafka.Message) {
			defer func() { <-processSemaphore }()

			headersLog := make(map[string]string)
			for _, h := range msg.Headers {
				headersLog[string(h.Key)] = string(h.Value)
			}

			eventLogger := log.With().
				Interface("kafka_headers_received", headersLog).
				Logger()

			eventLogger.Debug().Msg("All Kafka headers received")

			var traceID string
			for _, h := range msg.Headers {
				if string(h.Key) == "trace_id" {
					traceID = string(h.Value)
					break
				}
			}

			if traceID == "" {
				traceID = "unknown-" + time.Now().Format("20060102-150405")
			}

			eventLogger = eventLogger.With().Str("trace_id", traceID).Logger()

			var event domain.KafkaTransactionEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				eventLogger.Error().
					Err(err).
					Msg("Error unmarshal Kafka message")
				return
			}

			eventLogger.Info().
				Str("tx_id", event.TxID).
				Str("type", event.Type).
				Int("concurrent", maxConcurrent-len(processSemaphore)).
				Msg("Received event")

			processTransactionEvent(&event, eventLogger)

			if err := kafkaReader.CommitMessages(context.Background(), msg); err != nil {
				eventLogger.Warn().
					Err(err).
					Msg("Gagal commit offset")
			}
		}(msg)
	}
}

func processTransactionEvent(event *domain.KafkaTransactionEvent, logger zerolog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	var newStatus string = "success"
	var details string = "Processed successfully"

	_, breakerErr := resilience.ExecuteWithBreaker[struct{}](
		ctx,
		resilience.PostgresBreaker,
		"PostgresProcessTx",
		func() (struct{}, error) {
			var existing string
			err := database.PostgresPool.QueryRow(ctx,
				"SELECT tx_id FROM transactions WHERE tx_id = $1", event.TxID).Scan(&existing)
			if err == nil {
				logger.Info().
					Str("tx_id", event.TxID).
					Msg("Tx sudah diproses sebelumnya")
				return struct{}{}, nil
			}

			err = resilience.RetryWithBackoff(ctx, func() error {
				_, err := database.PostgresPool.Exec(ctx,
					`INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, created_at, updated_at)
					 VALUES ($1, $2, $3, $4, $5, 'pending', NOW(), NOW())`,
					event.TxID, event.UserID, event.RecipientID, event.Amount, event.Type)
				return err
			}, 3, 500*time.Millisecond)

			if err != nil {
				logger.Warn().
					Err(err).
					Str("tx_id", event.TxID).
					Msg("Gagal insert pending setelah retry")
				database.LogToMongo(*event, "failed", "Insert pending gagal setelah retry")
				return struct{}{}, err
			}

			var userBalance float64
			err = resilience.RetryWithBackoff(ctx, func() error {
				return database.PostgresPool.QueryRow(ctx,
					"SELECT balance FROM users WHERE id = $1 FOR UPDATE", event.UserID).Scan(&userBalance)
			}, 3, 500*time.Millisecond)

			if err != nil {
				logger.Warn().
					Err(err).
					Int64("user_id", event.UserID).
					Msg("Gagal ambil saldo user setelah retry")
				database.LogToMongo(*event, "failed", "User not found")
				return struct{}{}, err
			}

			var recipientBalance float64
			if event.Type == "transfer" && event.RecipientID != 0 {
				err = resilience.RetryWithBackoff(ctx, func() error {
					return database.PostgresPool.QueryRow(ctx,
						"SELECT balance FROM users WHERE id = $1 FOR UPDATE", event.RecipientID).Scan(&recipientBalance)
				}, 3, 500*time.Millisecond)

				if err != nil {
					logger.Warn().
						Err(err).
						Int64("recipient_id", event.RecipientID).
						Msg("Gagal ambil saldo recipient setelah retry")
					database.LogToMongo(*event, "failed", "Recipient not found")
					return struct{}{}, err
				}
			}

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
				logger.Warn().
					Err(err).
					Msg("Gagal begin tx")
				return struct{}{}, err
			}
			defer txDB.Rollback(ctx)

			err = resilience.RetryWithBackoff(ctx, func() error {
				_, err := txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2",
					userBalance, event.UserID)
				return err
			}, 3, 500*time.Millisecond)
			if err != nil {
				logger.Warn().
					Err(err).
					Int64("user_id", event.UserID).
					Msg("Gagal update balance user setelah retry")
				return struct{}{}, err
			}

			if event.Type == "transfer" && event.RecipientID != 0 {
				err = resilience.RetryWithBackoff(ctx, func() error {
					_, err := txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2",
						recipientBalance, event.RecipientID)
					return err
				}, 3, 500*time.Millisecond)
				if err != nil {
					logger.Warn().
						Err(err).
						Int64("recipient_id", event.RecipientID).
						Msg("Gagal update balance recipient setelah retry")
					return struct{}{}, err
				}
			}

			err = resilience.RetryWithBackoff(ctx, func() error {
				_, err := txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE tx_id = $2",
					newStatus, event.TxID)
				return err
			}, 3, 500*time.Millisecond)
			if err != nil {
				logger.Warn().
					Err(err).
					Str("tx_id", event.TxID).
					Msg("Gagal update status setelah retry")
				return struct{}{}, err
			}

			err = resilience.RetryWithBackoff(ctx, func() error {
				return txDB.Commit(ctx)
			}, 3, 500*time.Millisecond)
			if err != nil {
				logger.Warn().
					Err(err).
					Msg("Gagal commit setelah retry")
				return struct{}{}, err
			}

			return struct{}{}, nil
		},
	)

	if breakerErr != nil {
		logger.Warn().
			Err(breakerErr).
			Str("tx_id", event.TxID).
			Msg("Postgres process ditolak breaker")
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

	logger.Info().
		Str("tx_id", event.TxID).
		Str("status", newStatus).
		Str("details", details).
		Msg("Processed tx")
}

func CloseKafkaConsumer() {
	if kafkaReader != nil {
		kafkaReader.Close()
		log.Info().Msg("Kafka consumer closed")
	}
}

func IsKafkaProducerReady() bool {
	return kafkaWriter != nil
}

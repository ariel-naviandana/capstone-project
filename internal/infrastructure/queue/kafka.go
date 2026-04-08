package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
		Addr:         kafka.TCP(brokers...),
		Topic:        config.AppConfig.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 10 * time.Millisecond,
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

const maxConcurrent = 100

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
					msg, innerErr = kafkaReader.FetchMessage(context.Background())
					return innerErr
				}, 3, 500*time.Millisecond)

				return msg, retryErr
			},
		)

		if breakerErr != nil {
			log.Warn().
				Err(breakerErr).
				Msg("Kafka fetch initial message ditolak breaker, sleep 1s")
			time.Sleep(1 * time.Second)
			continue
		}

		batch := []kafka.Message{msg}

		// Try to fetch more messages concurrently up to maxConcurrent to build a batch
		for i := 1; i < maxConcurrent; i++ {
			fetchCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			m, fetchErr := kafkaReader.FetchMessage(fetchCtx)
			cancel()
			if fetchErr != nil {
				break
			}
			batch = append(batch, m)
		}

		var wg sync.WaitGroup
		for _, batchMsg := range batch {
			wg.Add(1)
			go func(m kafka.Message) {
				defer wg.Done()

				headersLog := make(map[string]string)
				for _, h := range m.Headers {
					headersLog[string(h.Key)] = string(h.Value)
				}

				eventLogger := log.With().
					Interface("kafka_headers_received", headersLog).
					Logger()

				var traceID string
				for _, h := range m.Headers {
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
				if err := json.Unmarshal(m.Value, &event); err != nil {
					eventLogger.Error().
						Err(err).
						Msg("Error unmarshal Kafka message")
					return
				}

				processTransactionEvent(&event, eventLogger)
			}(batchMsg)
		}

		// Wait for all messages in the batch to process completely
		wg.Wait()

		// ONLY commit offsets synchronously after the entire batch processes successfully
		if err := kafkaReader.CommitMessages(context.Background(), batch...); err != nil {
			log.Warn().
				Err(err).
				Msg("Gagal sinkronisasi commit offset kafka batch")
		}
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
	if err := cache.SetCache(ctx, cacheKey, pendingDetail, 5*time.Minute); err != nil {
		logger.Warn().Err(err).Str("tx_id", event.TxID).Msg("Failed to set pending cache")
	}

	var newStatus string = "success"
	var details string = "Processed successfully"

	_, breakerErr := resilience.ExecuteWithBreaker[struct{}](
		ctx,
		resilience.PostgresBreaker,
		"PostgresProcessTx",
		func() (struct{}, error) {
			return struct{}{}, resilience.RetryWithBackoff(ctx, func() error {
				txDB, err := database.WritePool.Begin(ctx)
				if err != nil {
					return err
				}
				defer func() {
					if rbErr := txDB.Rollback(ctx); rbErr != nil {
						log.Warn().Err(rbErr).Msg("Error rolling back transaction")
					}
				}()

				// 1. Deduplication using ON CONFLICT DO NOTHING
				var insertedTxID string
				err = txDB.QueryRow(ctx,
					`INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, created_at, updated_at)
					 VALUES ($1, $2, $3, $4, $5, 'pending', NOW(), NOW())
					 ON CONFLICT (tx_id) DO NOTHING RETURNING tx_id`,
					event.TxID, event.UserID, event.RecipientID, event.Amount, event.Type).Scan(&insertedTxID)

				if err != nil && err.Error() != "no rows in result set" { // Duplicate tx will return no rows
					return err
				}

				if insertedTxID == "" {
					logger.Info().Str("tx_id", event.TxID).Msg("Tx sudah diproses sebelumnya (duplicate)")
					return nil // Already processed
				}

				// 2. Lock Users & Calculate Balances safely
				var userAccount domain.UserBalance
				var recipientAccount domain.UserBalance

				if event.Type == "transfer" && event.RecipientID != 0 {
					rows, err := txDB.Query(ctx, "SELECT id, username, balance FROM users WHERE id IN ($1, $2) FOR UPDATE", event.UserID, event.RecipientID)
					if err != nil {
						return err
					}
					defer rows.Close()

					var foundUser, foundRecipient bool
					for rows.Next() {
						var account domain.UserBalance
						if err := rows.Scan(&account.ID, &account.Username, &account.Balance); err != nil {
							continue
						}
						if account.ID == event.UserID {
							userAccount = account
							foundUser = true
						} else if account.ID == event.RecipientID {
							recipientAccount = account
							foundRecipient = true
						}
					}
					rows.Close()

					if !foundUser || !foundRecipient {
						newStatus = "failed"
						details = "Salah satu user tidak ditemukan"
					}
				} else {
					err = txDB.QueryRow(ctx, "SELECT id, username, balance FROM users WHERE id = $1 FOR UPDATE", event.UserID).
						Scan(&userAccount.ID, &userAccount.Username, &userAccount.Balance)
					if err != nil {
						newStatus = "failed"
						details = "Pengirim tidak ditemukan"
					}
				}

				userBalance := userAccount.Balance
				recipientBalance := recipientAccount.Balance

				// 3. Process Logic
				if newStatus != "failed" {
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
				}

				// 4. Update balances if success
				if newStatus == "success" {
					_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", userBalance, event.UserID)
					if err != nil {
						return err
					}

					if event.Type == "transfer" && event.RecipientID != 0 {
						_, err = txDB.Exec(ctx, "UPDATE users SET balance = $1, updated_at = NOW() WHERE id = $2", recipientBalance, event.RecipientID)
						if err != nil {
							return err
						}
					}

					// Update caching immediately (Write-Through rather than Invalidate)
					userAccount.Balance = userBalance
					if err := cache.SetCache(ctx, fmt.Sprintf("user_balance:%d", event.UserID), userAccount, 1*time.Minute); err != nil {
						log.Warn().Err(err).Int64("user_id", event.UserID).Msg("Failed to set user balance cache")
					}

					if event.Type == "transfer" && event.RecipientID != 0 {
						recipientAccount.Balance = recipientBalance
						if err := cache.SetCache(ctx, fmt.Sprintf("user_balance:%d", event.RecipientID), recipientAccount, 1*time.Minute); err != nil {
							log.Warn().Err(err).Int64("recipient_id", event.RecipientID).Msg("Failed to set recipient balance cache")
						}
					}
				} else {
					// Fallback for failed transactions
					database.LogToMongo(*event, newStatus, details)
				}

				// 5. Update transaction status
				_, err = txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE tx_id = $2", newStatus, event.TxID)
				if err != nil {
					return err
				}

				return txDB.Commit(ctx)
			}, 3, 500*time.Millisecond)
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
	if err := cache.SetCache(ctx, cacheKey, finalDetail, 5*time.Minute); err != nil {
		logger.Warn().Err(err).Str("tx_id", event.TxID).Msg("Failed to set final cache")
	}

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

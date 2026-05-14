package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	defer func() { _ = conn.Close() }()

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

func PublishTransactionEvent(txID string, accountNo string, recipientNo string, amount float64, txType string, refNo string, traceID string) error {
	if kafkaWriter == nil {
		return fmt.Errorf("kafka writer belum di-init")
	}

	event := domain.KafkaTransactionEvent{
		TrxID:       txID,
		AccountNo:   accountNo,
		RecipientNo: recipientNo,
		Amount:      amount,
		Type:        txType,
		RefNo:       refNo,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(event)
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
		if err := kafkaWriter.Close(); err != nil {
			log.Warn().Err(err).Msg("Gagal menutup Kafka producer")
		}
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

		_, breakerErr := resilience.ExecuteWithBreaker(
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

				// The previous event payload was different. Now it has tx_id, account_no, recipient_no, amount, type, timestamp.
				// In our new schema, event.TxID is now event.TrxID if we matched domain models.
				// Wait, we defined KafkaTransactionEvent with TrxID, AccountNo, RecipientNo, Amount, Type, Timestamp.
				// But we published with tx_id etc in json keys.
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

	if event.TrxID == "" {
		logger.Warn().Interface("event", event).Msg("Skip: empty trx_id in event")
		return
	}

	// Use the producer-stamped timestamp so retries/redeliveries hit the same
	// PK (trx_id, created_at) and ON CONFLICT DO NOTHING actually dedups.
	eventTime, err := time.Parse(time.RFC3339Nano, event.Timestamp)
	if err != nil {
		eventTime, err = time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			eventTime = time.Now().UTC()
		}
	}
	eventTime = eventTime.UTC()

	pendingDetail := &domain.TransactionDetail{
		TrxID:       event.TrxID,
		AccountNo:   event.AccountNo,
		RecipientNo: event.RecipientNo,
		Amount:      event.Amount,
		Type:        event.Type,
		Status:      "pending",
		RefNo:       event.RefNo,
		CreatedAt:   eventTime,
		UpdatedAt:   eventTime,
	}

	newStatus := "success"
	details := "Processed successfully"
	// duplicate stays true when ON CONFLICT short-circuits; we then skip
	// rewriting the cache so the canonical post-commit state from the first
	// delivery is not clobbered by this redelivery.
	duplicate := false

	_, breakerErr := resilience.ExecuteWithBreaker(
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
					if rbErr := txDB.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
						log.Warn().Err(rbErr).Msg("Error rolling back transaction")
					}
				}()

				// 1. Deduplication using ON CONFLICT DO NOTHING.
				// Partitioned tables require the partition key inside the PK,
				// so the conflict target is (trx_id, created_at). Pass the
				// producer timestamp so a redelivered Kafka message carries
				// the same PK and the conflict actually fires.
				var refNoArg interface{}
				if event.RefNo != "" {
					refNoArg = event.RefNo
				}
				var insertedTxID string
				err = txDB.QueryRow(ctx,
					`INSERT INTO transactions (trx_id, account_no, amount, type, status, ref_no, created_at, updated_at)
					 VALUES ($1, $2, $3, $4, 'pending', $5, $6, $6)
					 ON CONFLICT (trx_id, created_at) DO NOTHING RETURNING trx_id`,
					event.TrxID, event.AccountNo, event.Amount, event.Type, refNoArg, eventTime).Scan(&insertedTxID)

				if err != nil && !errors.Is(err, pgx.ErrNoRows) { // Duplicate tx will return no rows
					return err
				}

				if insertedTxID == "" {
					duplicate = true
					logger.Info().Str("tx_id", event.TrxID).Msg("Tx sudah diproses sebelumnya (duplicate)")
					return nil // Already processed
				}

				// Cache pending only after the row is ours, so a redelivered
				// duplicate cannot overwrite the previously-committed final
				// state with "pending".
				cache.TxLayer.WriteThrough(ctx, event.TrxID, pendingDetail)

				// 2. Lock Accounts & Calculate Balances safely
				var userAccount domain.AccountBalance
				var recipientAccount domain.AccountBalance

				if event.Type == "transfer" && event.RecipientNo != "" {
					rows, err := txDB.Query(ctx, "SELECT account_no, balance FROM accounts WHERE account_no IN ($1, $2) FOR UPDATE", event.AccountNo, event.RecipientNo)
					if err != nil {
						return err
					}

					var foundUser, foundRecipient bool
					for rows.Next() {
						var account domain.AccountBalance
						if err := rows.Scan(&account.AccountNo, &account.Balance); err != nil {
							continue
						}
						switch account.AccountNo {
						case event.AccountNo:
							userAccount = account
							foundUser = true
						case event.RecipientNo:
							recipientAccount = account
							foundRecipient = true
						}
					}
					rows.Close()

					if !foundUser || !foundRecipient {
						newStatus = "failed"
						details = "Salah satu account tidak ditemukan"
					}
				} else {
					err = txDB.QueryRow(ctx, "SELECT account_no, balance FROM accounts WHERE account_no = $1 FOR UPDATE", event.AccountNo).
						Scan(&userAccount.AccountNo, &userAccount.Balance)
					if err != nil {
						newStatus = "failed"
						details = "Rekening pengirim tidak ditemukan"
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
							details = fmt.Sprintf("Transfer %.2f ke rekening %s", event.Amount, event.RecipientNo)
						}

					default:
						newStatus = "failed"
						details = "Type tidak dikenal"
					}
				}

				// 4. Update balances if success
				if newStatus == "success" {
					_, err = txDB.Exec(ctx, "UPDATE accounts SET balance = $1, updated_at = NOW() WHERE account_no = $2", userBalance, event.AccountNo)
					if err != nil {
						return err
					}

					if event.Type == "transfer" && event.RecipientNo != "" {
						_, err = txDB.Exec(ctx, "UPDATE accounts SET balance = $1, updated_at = NOW() WHERE account_no = $2", recipientBalance, event.RecipientNo)
						if err != nil {
							return err
						}
					}

					// Update caching immediately (Write-Through rather than Invalidate).
					// BalanceLayer populates both L1 + Redis so reads after a commit
					// hit memory directly without going to Postgres.
					userAccount.Balance = userBalance
					cache.BalanceLayer.WriteThrough(ctx, event.AccountNo, &userAccount)

					if event.Type == "transfer" && event.RecipientNo != "" {
						recipientAccount.Balance = recipientBalance
						cache.BalanceLayer.WriteThrough(ctx, event.RecipientNo, &recipientAccount)
					}
				} else {
					// Fallback for failed transactions
					database.LogToMongo(*event, newStatus, details)
				}

				// 5. Update transaction status
				_, err = txDB.Exec(ctx, "UPDATE transactions SET status = $1, updated_at = NOW() WHERE trx_id = $2", newStatus, event.TrxID)
				if err != nil {
					return err
				}

				// 6. Generate Notification
				notifID := "NTF-" + uuid.NewString()[:8]
				_, err = txDB.Exec(ctx, "INSERT INTO notifications (notif_id, account_no, channel, title, trx_ref, created_at, updated_at) VALUES ($1, $2, 'in-app', $3, $4, NOW(), NOW())",
					notifID, event.AccountNo, details, event.TrxID)
				if err != nil {
					log.Warn().Err(err).Msg("Gagal membuat notifikasi")
					// Not a hard failure for the transaction.
				}

				return txDB.Commit(ctx)
			}, 3, 500*time.Millisecond)
		},
	)

	if breakerErr != nil {
		logger.Warn().
			Err(breakerErr).
			Str("tx_id", event.TrxID).
			Msg("Postgres process ditolak breaker")
		return
	}

	// Skip the final cache write for duplicates: the first delivery already
	// wrote the canonical state and this event's payload may differ.
	if duplicate {
		return
	}

	finalDetail := &domain.TransactionDetail{
		TrxID:       event.TrxID,
		AccountNo:   event.AccountNo,
		RecipientNo: event.RecipientNo,
		Amount:      event.Amount,
		Type:        event.Type,
		Status:      newStatus,
		RefNo:       event.RefNo,
		CreatedAt:   eventTime,
		UpdatedAt:   time.Now().UTC(),
	}
	cache.TxLayer.WriteThrough(ctx, event.TrxID, finalDetail)
}

func CloseKafkaConsumer() {
	if kafkaReader != nil {
		if err := kafkaReader.Close(); err != nil {
			log.Warn().Err(err).Msg("Gagal menutup Kafka consumer")
		}
		log.Info().Msg("Kafka consumer closed")
	}
}

func IsKafkaProducerReady() bool {
	return kafkaWriter != nil
}
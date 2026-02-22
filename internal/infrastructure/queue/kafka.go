package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
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

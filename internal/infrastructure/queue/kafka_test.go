package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsKafkaProducerReady_NotInitialized(t *testing.T) {
	// Reset kafkaWriter to nil
	originalWriter := kafkaWriter
	kafkaWriter = nil
	defer func() { kafkaWriter = originalWriter }()

	assert.False(t, IsKafkaProducerReady())
}

func TestPublishTransactionEvent_NilWriter(t *testing.T) {
	// Reset kafkaWriter to nil
	originalWriter := kafkaWriter
	kafkaWriter = nil
	defer func() { kafkaWriter = originalWriter }()

	err := PublishTransactionEvent("tx-123", 1, 2, 50000, "transfer", "trace-abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "belum di-init")
}

func TestCloseKafkaProducer_NilWriter(t *testing.T) {
	// Should not panic when kafkaWriter is nil
	originalWriter := kafkaWriter
	kafkaWriter = nil
	defer func() { kafkaWriter = originalWriter }()

	assert.NotPanics(t, func() {
		CloseKafkaProducer()
	})
}

func TestCloseKafkaConsumer_NilReader(t *testing.T) {
	// Should not panic when kafkaReader is nil
	originalReader := kafkaReader
	kafkaReader = nil
	defer func() { kafkaReader = originalReader }()

	assert.NotPanics(t, func() {
		CloseKafkaConsumer()
	})
}

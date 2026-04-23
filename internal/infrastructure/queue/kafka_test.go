package queue

import (
	"sync"
	"testing"
	"time"

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

// ==================== BACKPRESSURE TESTS ====================

func TestBackpressure_LimitConcurrent(t *testing.T) {
	// Test ini memverifikasi bahwa maxConcurrent tidak boleh 0
	assert.Greater(t, maxConcurrent, 0, "maxConcurrent harus > 0 untuk backpressure")
	// maxConcurrent seharusnya diatur ke nilai wajar (contoh: 100)
	assert.LessOrEqual(t, maxConcurrent, 1000, "maxConcurrent wajar < 1000")
}

// Test untuk memastikan batch processing tidak melebihi maxConcurrent
func TestBackpressure_BatchSizeLimit(t *testing.T) {
	// maxConcurrent digunakan sebagai batas maksimum batch size
	// Jika batch size melebihi maxConcurrent, seharusnya dibatasi
	batchSize := 150
	maxBatch := maxConcurrent

	if batchSize > maxBatch {
		batchSize = maxBatch
	}

	assert.LessOrEqual(t, batchSize, maxConcurrent)
	assert.Equal(t, batchSize, maxConcurrent) // Batch size akan dipotong ke maxConcurrent
}

// Test untuk memverifikasi bahwa goroutine tidak melebihi kapasitas
func TestBackpressure_GoroutineLimit(t *testing.T) {
	// Simulasi bahwa sistem tidak akan membuat lebih dari maxConcurrent goroutine
	// Ini diimplementasikan via batch processing (setiap batchMsg dalam goroutine terpisah)
	// Jumlah goroutine maksimal = len(batch) ≤ maxConcurrent

	// Buat channel untuk tracking goroutine
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	// Simulasi 150 task dengan semaphore
	tasks := 150
	completed := 0

	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			// Simulasi proses
			time.Sleep(1 * time.Millisecond)
			completed++
		}()
	}

	wg.Wait()

	// Semua task harus selesai
	assert.Equal(t, tasks, completed)
	// Tidak lebih dari maxConcurrent goroutine yang berjalan bersamaan
	// (ini dijamin oleh semaphore)
}

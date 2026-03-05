# Capstone B.4 : Exploding User Data Scalabilities

Prototype sistem transaksi user yang scalable, low-latency, dan reliable menggunakan Go. Fokus pada resilience, observability, dan handling overload.

## Tech Stack
- **Backend**: Go 1.24 + Gin Gonic
- **Database**: PostgreSQL (transaksi utama) + MongoDB (logging & data fleksibel)
- **Caching & Rate Limiting**: Redis
- **Message Queue**: Kafka (Confluent)
- **Resilience**: Circuit Breaker (gobreaker), Retry with Backoff, Backpressure (semaphore)
- **Logging & Observability**: Zerolog (structured JSON) + Trace ID propagation
- **Deployment**: Docker + Docker Compose (monorepo: API + Worker)

## Fitur Utama & Resilience
- Rate limiting per IP/user (Redis)
- Async processing transaksi via Kafka (producer di API, consumer di Worker)
- Connection pooling (pgxpool untuk Postgres) dengan Read/Write Separation (Master/Replica)
- Circuit Breaker di semua external call (Kafka producer/consumer, Postgres, Mongo)
- Retry with exponential backoff untuk transient error
- Backpressure di Kafka consumer (max 10 concurrent proses event)
- Structured logging (zerolog JSON) di seluruh flow
- Trace ID propagation: dari API request → Kafka header → Worker proses event
- Caching balance & transaction status di Redis

## Struktur Proyek
```
capstone-go/
├── cmd/
│   ├── api/          # HTTP server (Gin)
│   └── worker/       # Kafka consumer worker
├── internal/
│   ├── application/  # Business logic / service
│   ├── config/       # Load .env + viper
│   ├── delivery/     # Handler & middleware Gin
│   ├── domain/       # Models & entities
│   ├── infrastructure/
│   │   ├── cache/    # Redis client
│   │   ├── database/ # Postgres & Mongo
│   │   ├── logging/  # Zerolog init + helper
│   │   ├── queue/    # Kafka producer & consumer
│   │   └── resilience/ # Circuit breaker + retry
├── .env              # Config environment
├── docker-compose.yml
└── README.md
```

## Cara Jalankan Lokal (Dev)
1. Install dependencies:
   ```
   go mod tidy
   ```

2. Copy `.env.example` ke `.env` dan isi (password DB, dll).

3. Jalankan dengan hot-reload (pakai Air):
   ```
   air
   ```

4. Akses API:
   - Health check: http://localhost:8000/health
   - POST transaction: http://localhost:8000/transactions
   - GET balance: http://localhost:8000/users/1/balance
   - GET transaction: http://localhost:8000/transactions/{txId}

## Cara Jalankan dengan Docker
```
docker compose up --build -d
```

- API: http://localhost:8000
- Worker: berjalan di background, consume Kafka
- Postgres: localhost:5432
- Mongo: localhost:27017
- Redis: localhost:6379
- Kafka: localhost:9092

## Cara Tes Resilience & Observability
1. **Normal flow**:
   - POST /transactions → cek log API (publish success) & log worker (processed success)
   - Lihat trace_id sama di log API & worker

2. **Simulasi failure**:
   - Stop Postgres: `docker stop tx-postgres`
   - Spam POST → API return 503 cepat (breaker trip), worker retry lalu skip proses
   - Start lagi: `docker start tx-postgres` → recovery otomatis

3. **Simulasi overload**:
   - Gunakan k6 load test (lihat bagian Load Test di bawah)

## Load Test dengan k6
Script untuk load testing (Peak Load & Failure Simulation) sudah tersedia di `performance_test.js`.
Jalankan menggunakan k6:
```bash
k6 run performance_test.js
```

## SLO Target (Target Capaian)
- p95 latency < 500ms di normal load
- Error rate < 1% saat overload/failure
- Breaker aktif proteksi sistem (fast fail 503)
- Trace ID konsisten end-to-end

## Catatan Pengembangan
- Logging sekarang full zerolog JSON + trace ID propagation
- Semua external call dilindungi breaker + retry
- Backpressure batasi concurrent proses di worker (max 10)

## Next Step (Ongoing)
- Unit Test (handler, repo, resilience)
- Autentikasi JWT (login + protect endpoint)
- Tambah endpoint GET /users/:id/transactions (history tx)
- CI/CD GitHub Actions (test otomatis)
- Custom Error Response standar
- Prometheus Metrics + Grafana Basic
- Load Testing k6 (peak load + failure sim)
- Capacity Planning & SLO Report (dari k6)
- Partitioning/Sharding DB
- Kubernetes Minikube + HPA lokal
- Cloud Deployment (AWS/GCP)

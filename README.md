# Capstone B.4 : Exploding User Data Scalabilities

Prototype sistem transaksi user yang scalable, low-latency, dan reliable menggunakan Go. Fokus pada resilience, observability, dan handling overload.

## Tech Stack
- **Backend**: Go 1.24 + Gin Gonic
- **Database**: PostgreSQL (transaksi utama) + MongoDB (logging & data fleksibel)
- **Caching & Rate Limiting**: Redis
- **Message Queue**: Kafka (Confluent)
- **Resilience**: Circuit Breaker (gobreaker), Retry with Backoff, Batch Processing
- **Logging & Observability**: Zerolog (structured JSON) + Trace ID propagation + Prometheus metrics + Grafana dashboard
- **Deployment**: Docker + Docker Compose (monorepo: API + Worker)
- **Load & Chaos Testing**: k6 (performance_test.js & chaos_test.js di folder tests/k6/)

## Arsitektur Sistem Flow
<img width="2459" height="1135" alt="Screenshot 2026-03-08 070253" src="https://github.com/user-attachments/assets/662fb9da-0eff-4ee0-bd13-4a5983fe3695" />
<img width="2407" height="1051" alt="Screenshot 2026-03-08 072751" src="https://github.com/user-attachments/assets/8fdef6b8-9d5e-4433-8ebd-2289beb985d4" />
<img width="2414" height="1063" alt="Screenshot 2026-03-08 073817" src="https://github.com/user-attachments/assets/61c639ff-dfd0-4bc5-93ad-79f34b22cd9b" />


## Fitur Utama & Resilience
- Rate limiting per IP/user (Redis)
- Async processing transaksi via Kafka (producer di API, consumer di Worker)
- Connection pooling (pgxpool untuk Postgres) dengan Read/Write Separation (Master/Replica)
- Circuit Breaker di semua external call (Kafka producer/consumer, Postgres, Mongo)
- Retry with exponential backoff untuk transient error
- Batch processing di Kafka consumer (max 100 concurrent proses event per batch)
- Structured logging (zerolog JSON) di seluruh flow
- Trace ID propagation: dari API request → Kafka header → Worker proses event
- Caching balance & transaction status di Redis
- Observability via Prometheus metrics (Gin requests, custom breaker/cache) + Grafana dashboard real-time (RPS, p95 latency dengan threshold SLO, error rate, breaker state/trips, cache hit rate)

## Arsitektur Sistem

```mermaid
flowchart TB
    Client([Client User/Service]) -->|HTTP REST/JSON| API

    subgraph "API Node (Gin Gonic)"
        API[API Server]
        MiddlewareLayer[Middleware Layer<br/>RateLimit, TraceID]
        TxHandler[Transaction Handler]
        
        API --> MiddlewareLayer
        MiddlewareLayer --> TxHandler
    end

    subgraph "Caching & Rate Limiting"
        Redis[(Redis 7)]
        MiddlewareLayer -->|1. Check/Set IP Limit<br/>CircuitBreaker| Redis
        TxHandler -.->|"Cache Read/Miss<br/>(Get Tx/Balance)"| Redis
    end

    subgraph "Message Queue Layer (Confluent)"
        KafkaBroker[(Kafka 7.6.1<br/>Topic: transactions)]
        TxHandler -->|2. Async Publish Event<br/>+ TraceID Header<br/>CircuitBreaker| KafkaBroker
    end

    subgraph "Worker Node (Background Processor)"
        Worker[Kafka Consumer Worker]
        BatchProcessor[Batch Processor<br/>Max 100/batch]
        PostgresTxDB{DB Tx Manager}
        
        KafkaBroker -->|3. Consume Batch<br/>CircuitBreaker| Worker
        Worker --> BatchProcessor
        BatchProcessor --> PostgresTxDB
    end

    subgraph "Database Layer"
        PG_Primary[(PostgreSQL 18 Master<br/>Write Pool)]
        PG_Replica[(PostgreSQL 18 Replica<br/>Read Pool)]
        Mongo[(MongoDB 7<br/>Fallback Logs)]
        
        PG_Primary -.->|Wal Replication| PG_Replica
    end

    %% Worker to Data Layer interactions
    PostgresTxDB -->|4. Update Balance/Status<br/>ON CONFLICT Deduplication<br/>CircuitBreaker| PG_Primary
    PostgresTxDB -.->|Write-Through Cache| Redis
    PostgresTxDB -->|5. Log Failed Tx<br/>CircuitBreaker| Mongo

    %% Read Operations from API
    TxHandler -->|Read Balance/Get Tx<br/>CircuitBreaker| PG_Replica

    %% Observability noting
    classDef observer fill:#e6f2ff,stroke:#3388ff,stroke-dasharray: 5 5;
    classDef primary fill:#ffe6e6,stroke:#ff3333;
    classDef cache fill:#e6ffe6,stroke:#33cc33;
    classDef worker fill:#fff2e6,stroke:#ff9933;
    
    class MiddlewareLayer observer;
    class PG_Primary primary;
    class Worker,BatchProcessor worker;
    class Redis cache;
    class KafkaBroker cache;
```

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
```bash
# Gunakan flag --compatibility jika limit resource (CPU/Memory) tidak teraplikasi pada environment non-Swarm
docker compose up --build -d --compatibility
```

- API: http://localhost:8000
- Worker: berjalan di background, consume Kafka
- Postgres: http://localhost:5432
- Mongo: http://localhost:27017
- Redis: http://localhost:6379
- Kafka: http://localhost:9092
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

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

## Load & Chaos Testing dengan k6

Script testing berada di folder `tests/k6/`:

- `performance_test.js`: Load test normal (smoke, load, stress, spike, soak)
- `chaos_test.js`: Simulasi failure (Postgres/Redis/Kafka down)

Cara jalankan (dari root proyek):

```bash
# Performance test
k6 run tests/k6/performance_test.js
```
```bash
# Chaos test postgre primary down
docker stop tx-postgre-primary
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=postgres
docker start tx-postgre-primary
```
```bash
# Chaos test redis down
docker stop tx-redis
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=redis
docker start tx-redis
```
```bash
# Chaos test kafka down
docker stop tx-kafka
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=kafka
docker start tx-kafka
```
K6 Testing Result

https://docs.google.com/spreadsheets/d/1MeOlugkW6gw524ed3eTZDOFPl-spz2XyKEeltw5GJrA/edit?usp=sharing

## Observability & SLO Dashboard (Grafana)
- Prometheus scrape metrics dari endpoint `/metrics` di API
- Grafana tampilkan real-time:
  - Requests per Second (RPS)
  - Latency p95 (threshold red >500ms untuk SLO breach)
  - Error Rate (%)
  - Circuit Breaker State & Trips Count
  - Cache Hit Rate (%)

Cara akses:
1. Buka http://localhost:3000
2. Login: capstone / admin123 (ganti password setelah login)
3. Dashboards → New → Import → upload `grafana-dashboard.json` di root proyek
4. Pilih datasource Prometheus (URL: http://prometheus:9090)
5. Import → dashboard langsung muncul
6. Refresh dashboard saat test k6 → lihat RPS naik, latency spike, cache hit rate tinggi

Dashboard di-export ke `grafana-dashboard.json` supaya bisa di-import ulang di mesin lain.

## SLO Target (Target Capaian)
- p95 latency < 500ms di normal load
- Error rate < 1% saat overload/failure
- Breaker aktif proteksi sistem (fast fail 503)
- Trace ID konsisten end-to-end
- Cache hit rate >80% pada read-heavy workload

## Catatan Pengembangan
- Logging sekarang full zerolog JSON + trace ID propagation
- Semua external call dilindungi breaker + retry
- Batch processing batasi concurrent proses di worker (max 100 per batch)
- Observability menggunakan Prometheus + Grafana untuk monitor RPS, latency, error, breaker, cache secara real-time

## Next Step (Ongoing)
- Unit Test (handler, repo, resilience)
- Autentikasi JWT (login + protect endpoint)
- Tambah endpoint GET /users/:id/transactions (history tx)
- CI/CD GitHub Actions (test otomatis)
- Custom Error Response standar
- Capacity Planning & SLO Report (dari k6)
- Partitioning/Sharding DB
- Kubernetes Minikube + HPA lokal
- Cloud Deployment (AWS/GCP)
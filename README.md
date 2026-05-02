# Capstone B.4 : Exploding User Data Scalabilities

Prototype sistem transaksi user yang scalable, low-latency, dan reliable menggunakan Go. Fokus pada resilience, observability, dan handling overload.

## Tech Stack
- **Backend**: Go 1.24 + Gin Gonic
- **Database**: PostgreSQL (transaksi utama) + MongoDB (logging & data fleksibel)
- **Caching & Rate Limiting**: Redis
- **Message Queue**: Kafka (Confluent)
- **Connection Pooler**: PgBouncer (transaction pooling, port 6432)
- **Resilience**: Circuit Breaker (gobreaker) dengan EOF-aware filtering, Retry with Backoff, Batch Processing
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
- Connection pooling berlapis: PgBouncer (transaction mode, max 10.000 client conn) di depan pgxpool (Read/Write Separation Master/Replica)
- Circuit Breaker di semua external call (Kafka producer/consumer, Postgres, Mongo) dengan filter `io.EOF` agar idle connection re-sync tidak mentrigger breaker trip
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

## Unit Testing

Unit test mencakup handler, repository, resilience (circuit breaker & retry), dan backpressure.

### Menjalankan Test

```bash
# Semua unit test
go test ./...

# Dengan coverage
go test -cover ./...
```

## Cara Tes Resilience & Observability
1. **Normal flow**:
   - POST /transactions → cek log API (publish success) & log worker (processed success)
   - Lihat trace_id sama di log API & worker

2. **Simulasi failure — Database**:
   - Stop Postgres: `docker stop tx-postgres-primary`
   - Spam POST → API return 503 cepat (breaker trip), worker retry lalu skip proses
   - Start lagi: `docker start tx-postgres-primary` → recovery otomatis

3. **Simulasi failure — PgBouncer (Connection Pooler)**:
   - Stop PgBouncer: `docker stop tx-pgbouncer`
   - API akan return 503 dari breaker; log akan menampilkan EOF error
   - Start lagi: `docker start tx-pgbouncer`
   - **Catatan**: Sejak optimasi Phase 2, error `io.EOF` yang muncul saat PgBouncer restart **tidak** mentrigger circuit breaker trip. Sistem membedakan antara "Database Down" (trip) dan "Idle Connection Re-sync / Pooler Restart" (no trip). Observasi di Grafana: `breaker_trips_total` tidak naik saat PgBouncer di-restart.

4. **Simulasi overload**:
   - Gunakan k6 load test (lihat bagian Load Test di bawah)

## Load & Chaos Testing dengan k6

Script testing berada di folder `tests/k6/`:

- `performance_test.js`: Load test normal (smoke, load, stress, spike, soak)
- `chaos_test.js`: Simulasi failure (Postgres/Redis/Kafka down)

Cara jalankan (dari root proyek):

```bash
# Performance test — pilih profil dengan TEST_PROFILE
k6 run -e TEST_PROFILE=smoke  tests/k6/performance_test.js --summary-export=smoke_summary.json
k6 run -e TEST_PROFILE=load   tests/k6/performance_test.js --summary-export=load_summary.json
k6 run -e TEST_PROFILE=stress tests/k6/performance_test.js --summary-export=stress_summary.json
k6 run -e TEST_PROFILE=spike  tests/k6/performance_test.js --summary-export=spike_summary.json
k6 run -e TEST_PROFILE=soak   tests/k6/performance_test.js --summary-export=soak_summary.json
```
```bash
# Chaos test — postgres primary down
docker stop tx-postgres-primary
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=postgres --summary-export=chaos_postgres.json
docker start tx-postgres-primary
```
```bash
# Chaos test — redis down
docker stop tx-redis
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=redis --summary-export=chaos_redis.json
docker start tx-redis
```
```bash
# Chaos test — kafka down
docker stop tx-kafka
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=kafka --summary-export=chaos_kafka.json
docker start tx-kafka
```
K6 Testing Result

[Link Google Sheet Hasil Testing K6](https://docs.google.com/spreadsheets/d/1MeOlugkW6gw524ed3eTZDOFPl-spz2XyKEeltw5GJrA/edit?usp=sharing)

## Observability & SLO Dashboard (Grafana)
- Prometheus scrape metrics dari endpoint `/metrics` di API
- Grafana tampilkan real-time:
  - Requests per Second (RPS)
  - Latency p95 (threshold alert merah >500ms untuk SLO breach)
  - Error Rate (%)
  - Circuit Breaker State & Trips Count per komponen (Postgres, Kafka, Mongo)
  - Cache Hit Rate (%) — Redis cache-aside untuk `GetUserBalance` & `GetByTxID`
  - **PgBouncer Pool Utilization**: active connections, idle connections, waiting connections per database (`capstone` / `capstone_read`)
  - pgxpool stats: acquired, idle, total connections per pool (write/read)

Cara akses:
1. Buka http://localhost:3000
2. Login: capstone / admin123 (ganti password setelah login)
3. Dashboards → New → Import → upload `grafana-dashboard.json` di root proyek
4. Pilih datasource Prometheus (URL: http://prometheus:9090)
5. Import → dashboard langsung muncul
6. Refresh dashboard saat test k6 → lihat RPS naik, latency spike, cache hit rate tinggi

Dashboard di-export ke `grafana-dashboard.json` supaya bisa di-import ulang di mesin lain.

## SLO Target (Target Capaian)
- **p95 latency < 500ms** di normal load 
- Error rate < 1% saat normal load; toleransi < 10% saat chaos (failure injection)
- Breaker aktif proteksi sistem (fast fail 503); EOF dari PgBouncer **tidak** mentrigger trip
- Trace ID konsisten end-to-end (API → Kafka header → Worker)
- Cache hit rate >80% pada read-heavy workload (balance & transaction lookup)
- PgBouncer pool utilization < 80% dari `default_pool_size` saat peak load

## Catatan Pengembangan
- Logging sekarang full zerolog JSON + trace ID propagation
- Semua external call dilindungi breaker + retry
- Batch processing batasi concurrent proses di worker (max 100 per batch)
- Observability menggunakan Prometheus + Grafana untuk monitor RPS, latency, error, breaker, cache secara real-time

## Next Step (Ongoing)
- Autentikasi JWT (login + protect endpoint)
- Tambah endpoint GET /users/:id/transactions (history tx)
- CI/CD GitHub Actions (test otomatis)
- Custom Error Response standar
- Capacity Planning & SLO Report (dari k6)
- Partitioning/Sharding DB
- Kubernetes Minikube + HPA lokal
- Cloud Deployment (AWS/GCP)

## Kubernetes Deployment & Testing
### Prerequisites
- Docker Desktop atau Docker Engine
- kubectl (Kubernetes CLI)
- Minikube (untuk local K8s cluster)

### 1. Install Minikube & kubectl
```bash
# Install kubectl (jika belum ada)
# Windows (via Chocolatey)
choco install kubernetes-cli

# Atau download manual dari https://kubernetes.io/docs/tasks/tools/

# Install Minikube
# Windows (via Chocolatey)
choco install minikube

# Atau download dari https://minikube.sigs.k8s.io/docs/start/
```

### 2. Start Minikube Cluster
```bash
# Start Minikube dengan Docker driver
minikube start --driver=docker

# Enable ingress addon (untuk expose service)
minikube addons enable ingress

# Cek status
minikube status
kubectl get nodes
```

### 3. Build Docker Images
```bash
# Build images untuk API dan Worker
docker build -f Dockerfile.api -t capstone-project-api:latest .
docker build -f Dockerfile.worker -t capstone-project-worker:latest .
```

### 4. Load Images ke Minikube
```bash
# Load images ke Minikube registry
minikube image load capstone-project-api:latest
minikube image load capstone-project-worker:latest
```

### 5. Deploy ke Kubernetes
```bash
# Apply semua K8s manifests
kubectl apply -f k8s/

# Cek deployment status
kubectl get deployments
kubectl get pods
kubectl get services
kubectl get configmaps
kubectl get secrets
kubectl get hpa
```

### 6. Test Aplikasi
```bash
# Forward port untuk akses API (karena service type ClusterIP)
kubectl port-forward svc/bankx-api 8000:8000

# Test health check
curl http://localhost:8000/health

# Test transaction
curl -X POST http://localhost:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "amount": 100.0, "type": "credit"}'

# Test balance
curl http://localhost:8000/users/1/balance

# Cek logs
kubectl logs -f deployment/bankx-api
kubectl logs -f deployment/bankx-worker
```

### 7. Monitoring dengan Grafana & Prometheus
```bash
# Forward ports untuk akses web UI
kubectl port-forward svc/prometheus 9090:9090
kubectl port-forward svc/grafana 3000:3000

# Akses:
# - Prometheus: http://localhost:9090
# - Grafana: http://localhost:3000 (user: capstone, pass: admin123)

# Import dashboard dari grafana-dashboard.json
```

### 8. Load Testing dengan k6
```bash
# Jalankan k6 dari lokal (karena Minikube cluster)
k6 run tests/k6/performance_test.js

# Atau untuk chaos test
# Stop salah satu service di K8s
kubectl scale deployment bankx-postgres-primary --replicas=0
k6 run tests/k6/chaos_test.js --env CHAOS_TARGET=postgres
kubectl scale deployment bankx-postgres-primary --replicas=1
```

### 9. Test Scaling & HPA
```bash
# Generate load untuk trigger HPA
k6 run tests/k6/performance_test.js --vus 50 --duration 5m

# Monitor scaling
kubectl get hpa
kubectl get pods -l app=bankx-api
```

### 10. Cleanup
```bash
# Stop Minikube
minikube stop

# Delete cluster (optional)
minikube delete
```

### Troubleshooting
- Jika pod CrashLoopBackOff: `kubectl describe pod <pod-name>` → cek logs & events
- Jika image tidak ditemukan: Pastikan `minikube image load` berhasil
- Jika service tidak accessible: Cek `kubectl get endpoints`
- Untuk debug lebih lanjut: `kubectl exec -it <pod-name> -- /bin/sh`
# Capstone Go: Exploding User Data Scalabilities

Prototype sistem transaksi user yang scalable, low-latency, dan reliable menggunakan Go.

## Tech Stack
- **Backend**: Go 1.23 + Gin
- **Database**: PostgreSQL (core transactional) + MongoDB (logs & flexible data)
- **Caching**: Redis
- **Message Queue**: Kafka
- **Deployment**: Docker + Docker Compose (monorepo: API + Worker)

## Struktur Proyek
capstone-go/
├── cmd/
│   ├── api/          # API server (Gin)
│   └── worker/       # Kafka consumer worker
├── internal/
│   ├── config/
│   ├── domain/
│   ├── application/
│   ├── infrastructure/
│   └── delivery/
├── pkg/
├── .env
├── Dockerfile.api
├── Dockerfile.worker
├── docker-compose.yml
├── .gitignore
└── README.md


## Cara Jalankan Lokal (Dev)
1. Install dependencies:
go mod tidy

2. Copy `.env.example` ke `.env` dan isi password/DB.

3. Jalankan dengan hot-reload (Air):
air

4. Akses API:
- Health: http://localhost:8000/health
- POST transaction: http://localhost:8000/transactions

## Cara Jalankan dengan Docker
docker compose up --build -d


- API: http://localhost:8000
- Worker: jalan di background, consume Kafka

## Fitur Utama
- Rate limiting (Redis)
- Async processing (Kafka + Worker)
- Connection pooling (pgxpool)
- Structured logging (logrus)
- Hybrid DB (Postgres + MongoDB)

## Rencana Selanjutnya
- Implementasi GET /transactions/:id + Redis cache
- Distributed tracing (Jaeger)
- DB migration (golang-migrate)
- Load testing (k6)
- Migrasi ke AWS (EKS/ECS + managed services)

Dibuat untuk capstone dengan fokus scalability & low latency.
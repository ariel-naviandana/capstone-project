# Capstone Go: Exploding User Data Scalabilities

Prototype sistem transaksi user yang scalable, low-latency, dan reliable menggunakan Go.

## Tech Stack
- **Backend**: Go 1.24 + Gin
- **Database**: PostgreSQL (core transactional) + MongoDB (logs & flexible data)
- **Caching**: Redis
- **Message Queue**: Kafka
- **Deployment**: Docker + Docker Compose (monorepo: API + Worker)


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

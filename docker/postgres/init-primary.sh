#!/bin/bash
set -e

echo "host replication repuser 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"

cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER repuser REPLICATION LOGIN CONNECTION LIMIT 10 ENCRYPTED PASSWORD 'reppassword';
EOSQL

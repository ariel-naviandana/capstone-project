#!/bin/bash
set -e

echo "Starting replica sync..."
rm -rf ${PGDATA}/*

pg_basebackup -h ${master_host} -D ${PGDATA} -U ${master_user} -vP -W

echo "Replica sync complete."
chown postgres:postgres ${PGDATA} -R
chmod 700 ${PGDATA} -R

cat >> ${PGDATA}/postgresql.conf <<EOF
listen_addresses = '*'
EOF

cat > ${PGDATA}/standby.signal <<EOF
EOF

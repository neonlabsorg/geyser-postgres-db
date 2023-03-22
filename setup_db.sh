#!/bin/bash

set -e

if [ -z "$PGDATA" ]; then
  echo "PGDATA is not defined!"
  exit 1
fi

echo "Enable pg_cron extension..."
cat <<EOT >> ${PGDATA}/postgresql.conf
shared_preload_libraries = 'pg_cron'
EOT

cat <<EOT >> ${PGDATA}/postgresql.conf
cron.database_name='${POSTGRES_DB:-solana}'
EOT

cat <<EOT >> ${PGDATA}/postgresql.conf
max_wal_size=8GB
EOT

cat <<EOT >> ${PGDATA}/postgresql.conf
shared_buffers=1GB
EOT

echo "Restarting PostgreSQL server..."
pg_ctl restart


#!/bin/bash

set -e

if [ -z "$PGPASSWORD" ]; then
  PGPASSWORD=solana-pass
fi

if [ -z "$PGDATA" ]; then
  echo "PGDATA is not defined!"
  exit 1
fi

if [ -z "$ACCOUNT_AUDIT_PART_SLOT_COUNT" ]; then
  echo "ACCOUNT_AUDIT_PART_SLOT_COUNT is not defined!"
  exit 1
fi

if [ -z "$ACCOUNT_AUDIT_START_SLOT" ]; then
  echo "ACCOUNT_AUDIT_START_SLOT is not defined!"
  exit 1
fi

if [ -z "$ACCOUNT_AUDIT_RETENTION_SLOTS" ]; then
  echo "ACCOUNT_AUDIT_RETENTION_SLOTS is not defined!"
  exit 1
fi

if [ -z "$MAINTENANCE_SCHEDULE" ]; then
  echo "MAINTENANCE_SCHEDULE is not defined!"
  exit 1
fi

if [ -z "$TEMP_ACCOUNT_PART_SLOT_COUNT" ]; then
  echo "TEMP_ACCOUNT_PART_SLOT_COUNT is not defined!"
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

echo "Deploying DB schema..."
psql \
  --dbname=solana \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_schema.sql
  
echo "Deploying DB functions..."
psql \
  --dbname=solana \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_functions.sql

echo "Finalizing DB partitions and maintenance schedule..."
envsubst < /opt/scripts/partitions.sql.template | psql \
  --dbname=solana \
  --username=$POSTGRES_USER

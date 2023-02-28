#!/bin/bash

set -e

if [ -z "$PGPASSWORD" ]; then
   PGPASSWORD=solana-pass
fi

echo "Enable pg_cron extension..."
cat <<EOT >> ${PGDATA}/postgresql.conf
shared_preload_libraries = 'pg_cron'
EOT

cat <<EOT >> ${PGDATA}/postgresql.conf
cron.database_name='${POSTGRES_DB:-solana}'
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

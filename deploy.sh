#!/bin/bash

set -e

if [ -z "$PGPASSWORD" ]; then
   PGPASSWORD=solana-pass
fi

cat <<EOT >> ${PGDATA}/postgresql.conf
shared_preload_libraries = 'pg_cron'
EOT

cat <<EOT >> ${PGDATA}/postgresql.conf
cron.database_name='${POSTGRES_DB:-solana}'
EOT

pg_ctl restart

psql \
  --dbname=solana \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_schema.sql
  
psql \
  --dbname=solana \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_functions.sql

envsubst < /opt/scripts/partitions.sql.template | psql \
  --dbname=solana \
  --username=$POSTGRES_USER

#!/bin/sh

set -e

if [ -z "$POSTGRES_HOST" ]; then
  echo "POSTGRES_HOST is not defined!"
  exit 1
fi

if [ -z "$POSTGRES_DB" ]; then
  echo "POSTGRES_DB is not defined!"
  exit 1
fi

if [ -z "$PGPASSWORD" ]; then
  echo "PGPASSWORD is not defined!"
  exit 1
fi

if [ -z "$HISTORY_PART_SLOT_COUNT" ]; then
  echo "HISTORY_PART_SLOT_COUNT is not defined!"
  exit 1
fi

if [ -z "$HISTORY_START_SLOT" ]; then
  echo "HISTORY_START_SLOT is not defined!"
  exit 1
fi

if [ -z "$HISTORY_RETENTION_SLOTS" ]; then
  echo "HISTORY_RETENTION_SLOTS is not defined!"
  exit 1
fi

if [ -z "$MAINTENANCE_SCHEDULE" ]; then
  echo "MAINTENANCE_SCHEDULE is not defined!"
  exit 1
fi

echo "Deploying DB schema..."
psql \
  --host=$POSTGRES_HOST \
  --dbname=$POSTGRES_DB \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_schema.sql
  
echo "Deploying DB functions..."
psql \
  --host=$POSTGRES_HOST \
  --dbname=$POSTGRES_DB \
  --username=$POSTGRES_USER \
  --file=/opt/scripts/create_functions.sql

echo "Finalizing DB partitions and maintenance schedule..."
envsubst < /opt/scripts/partitions.sql.template | psql \
  --host=$POSTGRES_HOST \
  --dbname=$POSTGRES_DB \
  --username=$POSTGRES_USER

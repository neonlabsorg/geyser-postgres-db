#!/bin/bash

set -e

PGPASSWORD=solana-pass

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

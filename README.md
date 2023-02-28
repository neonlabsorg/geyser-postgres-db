# Postgres DB for Tracer API

## Contents

- Postgres confirugation file (postgres.conf)
- DB schema - create_schema.sql, create_functions.sql, partitions.sql.template, drop_schema.sql
- Deployment scipt - deploy.sh
- CI dockerfile - Dockerfile

## Deployment

1. Install pg_partman and pg_cron extensions (please, refer to original docs https://github.com/pgpartman/pg_partman https://github.com/citusdata/pg_cron)
2. Place postgres.conf file into postgresdb configuration directory (usually */etc/postgresql/postgres.conf*)
3. Start database engine
4. run deployment script on the same machine with DB and with next env variables set up:
   - POSTGRES_USER - name of the deployer-user
   - PGPASSWORD - password of deployer-user
   - ACCOUNT_AUDIT_PART_SLOT_COUNT - number of solana slots which will be stored in a single partition of account_audit table
   - ACCOUNT_AUDIT_START_SLOT - number of first slot
   - ACCOUNT_AUDIT_RETENTION_SLOTS - number of slots to store in account_audit table (account's data from older slots will be merged into older_account table) and corresponding partitions will be deleted. 
   Example of the command:
   
   ```bash
   POSTGRES_USER=solana-user PGPASSWORD=solana-pass ACCOUNT_AUDIT_PART_SLOT_COUNT=216000 ACCOUNT_AUDIT_START_SLOT=0 ACCOUNT_AUDIT_RETENTION_SLOTS=6480000 ./deploy.sh
   ```
   This command will create DB schema with account_audit table splitted by days and retention interval of 30 days


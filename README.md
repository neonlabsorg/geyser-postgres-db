# Postgres DB for Tracer API

## Contents

- DB schema - create_schema.sql, create_functions.sql, partitions.sql.template, drop_schema.sql
- Deployment scipt - deploy.sh
- CI dockerfile - Dockerfile

## Deployment

1. Install pg_partman and pg_cron extensions (please, refer to original docs https://github.com/pgpartman/pg_partman https://github.com/citusdata/pg_cron)
2. Start database engine
3. run deployment script on the same machine with DB and with next env variables:
   - PGDATA - path to postgres configuration files (e. g. /var/lib/postgresql/data)
   - POSTGRES_USER - name of the deployer-user
   - PGPASSWORD - password of deployer-user
   - ACCOUNT_AUDIT_PART_SLOT_COUNT - number of solana slots which will be stored in a single partition of account_audit table
   - ACCOUNT_AUDIT_START_SLOT - number of first slot
   - ACCOUNT_AUDIT_RETENTION_SLOTS - number of slots to store in account_audit table (account's data from older slots will be merged into older_account table) and corresponding partitions will be deleted.
   - ACCOUNT_AUDIT_MAINTENANCE_SCHEDULE - schedule of maintenance procedure. Determines how often will table creation|retention performed. This variable should store string in cron-compatible format (e. g. */5 * * * * - to run maintenance every 5 minutes). Make sure the schedule will be synchronized with number of slots stored in a single partition (considering single slot takes ~0.4 seconds)
   Example of the command:
   
   ```bash
   PGDATA=/var/lib/postgresql/data POSTGRES_USER=solana-user PGPASSWORD=solana-pass ACCOUNT_AUDIT_PART_SLOT_COUNT=216000 ACCOUNT_AUDIT_START_SLOT=0 ACCOUNT_AUDIT_RETENTION_SLOTS=6480000 ACCOUNT_AUDIT_MAINTENANCE_SCHEDULE="30 0 * * */1"./deploy.sh
   ```
   This command will create DB schema with account_audit table splitted by days, retention interval of 30 days and maintenance happened every day at 00:30 AM 


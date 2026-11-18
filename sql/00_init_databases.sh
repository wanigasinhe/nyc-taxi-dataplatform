#!/usr/bin/env bash
# ==============================================================================
# Initialize multiple databases in PostgreSQL container
# ==============================================================================
set -e

echo "Creating 'airflow' and 'superset' databases if not already present..."
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "${POSTGRES_DB:-nyc_taxi}" <<-EOSQL
    SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    GRANT ALL PRIVILEGES ON DATABASE airflow TO ${POSTGRES_USER:-postgres};
    SELECT 'CREATE DATABASE superset' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
    GRANT ALL PRIVILEGES ON DATABASE superset TO ${POSTGRES_USER:-postgres};
EOSQL
echo "'airflow' and 'superset' databases ready."

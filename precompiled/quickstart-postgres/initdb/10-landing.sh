#!/bin/sh
# First start of the bundled Postgres only: create the landing database (the migrate role also checks it).
set -e
db="${ALELT_LANDING_DB:-alelt_landing}"
if ! psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc \
     "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE \"$db\""
fi

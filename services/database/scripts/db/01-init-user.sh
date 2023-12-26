#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    CREATE USER $DB_ADMIN_USER WITH PASSWORD '$DB_ADMIN_PASSWORD';
    CREATE USER $DB_APP_USER WITH PASSWORD '$DB_APP_PASSWORD';

    --GRANT pg_read_server_files TO $DB_ADMIN_USER;
    --GRANT pg_write_server_files TO $DB_ADMIN_USER;
EOSQL

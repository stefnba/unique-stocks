#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    CREATE USER $DB_ADMIN_USER WITH PASSWORD '$DB_ADMIN_PASSWORD';
    DROP DATABASE IF EXISTS $DB_NAME;
    CREATE database $DB_NAME
        WITH
        OWNER = $DB_ADMIN_USER
        ENCODING = 'UTF8'
        LC_COLLATE = 'en_US.utf8'
        LC_CTYPE = 'en_US.utf8'
        TABLESPACE = pg_default
        CONNECTION LIMIT = -1
        IS_TEMPLATE = False;
        
    ALTER DATABASE $DB_NAME SET timezone TO 'UTC';

    REVOKE CONNECT ON DATABASE $DB_NAME FROM PUBLIC;
    GRANT  CONNECT ON DATABASE $DB_NAME  TO $DB_ADMIN_USER;
EOSQL
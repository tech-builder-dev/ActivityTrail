-- Create the default Activity Trail database.
--
-- Run this while connected to the PostgreSQL maintenance database:
--
--   psql -d postgres -v ON_ERROR_STOP=1 -f db/postgres/001_create_database.sql
--
-- Then apply the schema:
--
--   psql -d activitytrail -1 -v ON_ERROR_STOP=1 -f db/postgres/V1__schema.sql
--
-- Activity Trail does not create the database at startup. Point DATABASE_URL
-- at this database, or at another database whose schema matches V1__schema.sql.
-- This statement fails if a database named activitytrail already exists.

CREATE DATABASE activitytrail
    WITH ENCODING 'UTF8'
         TEMPLATE template0;

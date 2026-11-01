-- Default Activity Trail schema for PostgreSQL 16 or newer.
--
-- Apply it to the activitytrail database:
--
--   psql -d activitytrail -1 -v ON_ERROR_STOP=1 -f db/postgres/V1__schema.sql
--
-- The shipped table is unpartitioned and includes the default indexes.
-- Correctness rules live in the primary key and the check constraints.
-- Secondary indexes are the default physical design. A host may add, replace,
-- or drop those secondary indexes, and may partition field_activity_trail, by adding
-- SQL under db/postgres/custom. Leave event_id unique and leave the check
-- constraints in place.
--
-- retention_days NULL means this application is kept until a host deletes rows
-- outside the retention job. A number is the retention window in 24-hour days.

SET TIME ZONE 'UTC';

CREATE TABLE application (
    application_id text PRIMARY KEY,
    display_name text NOT NULL,
    retention_days integer NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT application_id_pattern CHECK (
        application_id ~ '^[A-Za-z0-9._:-]{1,128}$'
    ),
    CONSTRAINT application_display_name_present CHECK (
        char_length(display_name) BETWEEN 1 AND 200
        AND btrim(display_name) <> ''
    ),
    CONSTRAINT application_retention_days_range CHECK (
        retention_days IS NULL
        OR retention_days BETWEEN 1 AND 3650
    )
);

COMMENT ON COLUMN application.retention_days IS
    'Days to keep field changes, measured as 24-hour UTC days from updated_at. NULL disables deletion for this application.';

CREATE TABLE field_activity_trail (
    event_id uuid PRIMARY KEY,
    application_id text NOT NULL REFERENCES application (application_id) ON DELETE RESTRICT,
    object_name text NOT NULL,
    object_id text NOT NULL,
    object_field text NOT NULL,
    change_type text NOT NULL,
    old_value text NULL,
    new_value text NULL,
    updated_by text NOT NULL,
    updated_at timestamptz NOT NULL,
    correlation_id text NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    body_hash bytea NOT NULL,
    CONSTRAINT field_activity_trail_object_name_pattern CHECK (
        object_name ~ '^[A-Za-z0-9._:-]{1,128}$'
    ),
    CONSTRAINT field_activity_trail_object_id_present CHECK (
        char_length(object_id) BETWEEN 1 AND 256
        AND btrim(object_id) <> ''
        AND position('/' IN object_id) = 0
    ),
    CONSTRAINT field_activity_trail_object_field_pattern CHECK (
        object_field ~ '^[A-Za-z0-9._:-]{1,128}$'
    ),
    CONSTRAINT field_activity_trail_updated_by_pattern CHECK (
        updated_by ~ '^[A-Za-z0-9._:-]{1,128}$'
    ),
    CONSTRAINT field_activity_trail_correlation_id_pattern CHECK (
        correlation_id IS NULL
        OR correlation_id ~ '^[A-Za-z0-9._:-]{1,128}$'
    ),
    CONSTRAINT field_activity_trail_value_length CHECK (
        (old_value IS NULL OR char_length(old_value) <= 8192)
        AND (new_value IS NULL OR char_length(new_value) <= 8192)
    ),
    CONSTRAINT field_activity_trail_body_hash_length CHECK (
        octet_length(body_hash) = 32
    ),
    CONSTRAINT field_activity_trail_change_shape CHECK (
        (
            change_type = 'set'
            AND old_value IS NULL
            AND new_value IS NOT NULL
        )
        OR (
            change_type = 'update'
            AND old_value IS NOT NULL
            AND new_value IS NOT NULL
            AND old_value <> new_value
        )
        OR (
            change_type = 'clear'
            AND old_value IS NOT NULL
            AND new_value IS NULL
        )
    )
);

CREATE INDEX field_activity_trail_object_idx
    ON field_activity_trail (
        application_id,
        object_name,
        object_id,
        updated_at DESC,
        recorded_at DESC,
        event_id DESC
    );

CREATE INDEX field_activity_trail_field_idx
    ON field_activity_trail (
        application_id,
        object_name,
        object_id,
        object_field,
        updated_at DESC,
        recorded_at DESC,
        event_id DESC
    );

CREATE INDEX field_activity_trail_retention_idx
    ON field_activity_trail (application_id, updated_at);

COMMENT ON TABLE field_activity_trail IS
    'One stored field activity. event_id is unique. Secondary indexes are the default physical design and may be replaced by a host.';

COMMENT ON COLUMN field_activity_trail.change_type IS
    'set, update, or clear. Names the shape of the change so a reader can tell a first value, a replacement, and a removal apart, and so the change-shape check can reject a row that does not match.';

COMMENT ON COLUMN field_activity_trail.correlation_id IS
    'Optional client id shared by every field published from one save. Not unique. A read returns it so support can see which fields were committed together.';

COMMENT ON COLUMN field_activity_trail.body_hash IS
    'SHA-256 of the canonical event body. A retry with the same event_id and the same hash is stored once. The same event_id with a different hash is rejected and does not overwrite the row.';

CREATE TABLE retention_run (
    retention_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NULL,
    rows_deleted bigint NOT NULL DEFAULT 0,
    status text NOT NULL,
    CONSTRAINT retention_run_status_known CHECK (
        status IN ('running', 'succeeded', 'failed')
    )
);

COMMENT ON TABLE retention_run IS
    'One execution of the retention pass. Contains no field values.';

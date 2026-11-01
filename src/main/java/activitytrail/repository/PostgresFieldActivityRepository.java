package activitytrail.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class PostgresFieldActivityRepository implements FieldActivityRepository {

    private static final int LOCK = 8142001;
    private static final int BATCH = 5000;

    private static final String INSERT = """
            WITH inserted AS (
                INSERT INTO field_activity_trail (
                    event_id, application_id, object_name, object_id, object_field,
                    change_type, old_value, new_value, updated_by, updated_at,
                    correlation_id, body_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            )
            SELECT
                EXISTS (SELECT 1 FROM inserted) AS inserted,
                (SELECT body_hash FROM field_activity_trail WHERE event_id = ?) AS stored_hash
            """;

    private static final String READ = """
            SELECT event_id, object_field, change_type, old_value, new_value,
                   updated_by, updated_at, recorded_at, correlation_id
            FROM field_activity_trail
            WHERE application_id = :application_id
              AND object_name = :object_name
              AND object_id = :object_id
              AND (CAST(:field AS text) IS NULL OR object_field = :field)
              AND (CAST(:from_at AS timestamptz) IS NULL OR updated_at >= :from_at)
              AND (CAST(:to_at AS timestamptz) IS NULL OR updated_at < :to_at)
              AND (
                    CAST(:cursor_updated_at AS timestamptz) IS NULL
                    OR (updated_at, recorded_at, event_id) < (
                        CAST(:cursor_updated_at AS timestamptz),
                        CAST(:cursor_recorded_at AS timestamptz),
                        CAST(:cursor_event_id AS uuid)
                    )
                  )
            ORDER BY updated_at DESC, recorded_at DESC, event_id DESC
            LIMIT :limit
            """;

    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate named;
    private final TransactionTemplate transactions;

    PostgresFieldActivityRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbc = new JdbcTemplate(dataSource);
        this.named = new NamedParameterJdbcTemplate(jdbc);
        this.transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public boolean databaseIsUp() {
        try {
            Integer one = jdbc.queryForObject("SELECT 1", Integer.class);
            return one != null && one == 1;
        } catch (DataAccessException ex) {
            return false;
        }
    }

    @Override
    public Instant currentDatabaseTime() {
        OffsetDateTime clock = jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class);
        if (clock == null) {
            throw new IllegalStateException("Database clock is unavailable");
        }
        return clock.toInstant();
    }

    @Override
    public Optional<RegisteredApplication> findApplication(String applicationId) {
        List<RegisteredApplication> rows = jdbc.query(SELECT_APPLICATION + " WHERE application_id = ?", registeredApplicationMapper(), applicationId);
        return rows.stream().findFirst();
    }

    @Override
    public RegisteredApplication registerApplication(String applicationId, String displayName, Integer retentionDays) {
        try {
            return jdbc.queryForObject(
                    """
                    INSERT INTO application (application_id, display_name, retention_days)
                    VALUES (?, ?, ?)
                    """ + RETURNING,
                    registeredApplicationMapper(),
                    applicationId,
                    displayName,
                    retentionDays
            );
        } catch (DuplicateKeyException ex) {
            throw new ApplicationAlreadyRegistered(applicationId);
        }
    }

    @Override
    public FieldChangeStoreResult storeFieldChange(FieldChange change) {
        try {
            return transactions.execute(status -> {
                StoredFieldChangeRow row = jdbc.queryForObject(
                        INSERT,
                        (rs, rowNum) -> new StoredFieldChangeRow(rs.getBoolean("inserted"), rs.getBytes("stored_hash")),
                        change.eventId(),
                        change.applicationId(),
                        change.objectName(),
                        change.objectId(),
                        change.objectField(),
                        change.changeType(),
                        change.oldValue(),
                        change.newValue(),
                        change.updatedBy(),
                        Timestamp.from(change.updatedAt()),
                        change.correlationId(),
                        change.bodyHash(),
                        change.eventId()
                );
                if (row == null || (!row.inserted && row.storedHash == null)) {
                    throw new IllegalStateException("The field change disappeared during save");
                }
                if (row.inserted) {
                    return FieldChangeStoreResult.NEW_ROW_STORED;
                }
                if (MessageDigest.isEqual(row.storedHash, change.bodyHash())) {
                    return FieldChangeStoreResult.DUPLICATE_EVENT;
                }
                status.setRollbackOnly();
                return FieldChangeStoreResult.CONFLICTING_EVENT;
            });
        } catch (DataAccessException ex) {
            throw new IllegalStateException("Could not store the field change", ex);
        }
    }

    @Override
    public List<FieldActivity> findFieldActivity(
            String applicationId,
            String objectName,
            String objectId,
            String field,
            Instant from,
            Instant to,
            FieldActivityPageMarker after,
            int limit
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("application_id", applicationId)
                .addValue("object_name", objectName)
                .addValue("object_id", objectId)
                .addValue("field", field, Types.VARCHAR)
                .addValue("from_at", toUtcTimestamp(from), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("to_at", toUtcTimestamp(to), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("cursor_updated_at", toUtcTimestamp(after == null ? null : after.updatedAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("cursor_recorded_at", toUtcTimestamp(after == null ? null : after.recordedAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("cursor_event_id", after == null ? null : after.eventId(), Types.OTHER)
                .addValue("limit", limit);
        return named.query(READ, params, (rs, rowNum) -> new FieldActivity(
                rs.getObject("event_id", UUID.class),
                rs.getString("object_field"),
                rs.getString("change_type"),
                rs.getString("old_value"),
                rs.getString("new_value"),
                rs.getString("updated_by"),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant(),
                rs.getObject("recorded_at", OffsetDateTime.class).toInstant(),
                rs.getString("correlation_id")
        ));
    }

    @Override
    public int deleteExpiredFieldActivity() {
        try (Connection connection = dataSource.getConnection()) {
            if (!tryRetentionLock(connection)) {
                return 0;
            }
            try {
                int deleted = 0;
                for (RetentionWindow window : retentionWindows(connection)) {
                    Instant cutoff = retentionCutoff(connection, window.days);
                    int batch;
                    do {
                        batch = deleteExpiredBatch(connection, window.applicationId, cutoff);
                        deleted += batch;
                    } while (batch > 0);
                }
                return deleted;
            } finally {
                releaseRetentionLock(connection);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Could not delete expired activity", ex);
        }
    }

    private static final String SELECT_APPLICATION = """
            SELECT application_id, display_name, retention_days, enabled, created_at, updated_at
            FROM application
            """;

    private static final String RETURNING = """
            RETURNING application_id, display_name, retention_days, enabled, created_at, updated_at
            """;

    private static RowMapper<RegisteredApplication> registeredApplicationMapper() {
        return (rs, rowNum) -> new RegisteredApplication(
                rs.getString("application_id"),
                rs.getString("display_name"),
                (Integer) rs.getObject("retention_days"),
                rs.getBoolean("enabled"),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant()
        );
    }

    private static OffsetDateTime toUtcTimestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static boolean tryRetentionLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setInt(1, LOCK);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static void releaseRetentionLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            statement.setInt(1, LOCK);
            statement.execute();
        }
    }

    private static List<RetentionWindow> retentionWindows(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT application_id, retention_days FROM application WHERE retention_days IS NOT NULL ORDER BY application_id"
        ); ResultSet rs = statement.executeQuery()) {
            List<RetentionWindow> retentionWindows = new ArrayList<>();
            while (rs.next()) {
                retentionWindows.add(new RetentionWindow(rs.getString(1), rs.getInt(2)));
            }
            return retentionWindows;
        }
    }

    private static Instant retentionCutoff(Connection connection, int days) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT clock_timestamp() - make_interval(days => ?)")) {
            statement.setInt(1, days);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        }
    }

    private static int deleteExpiredBatch(Connection connection, String applicationId, Instant cutoff) throws SQLException {
        boolean previous = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                """
                DELETE FROM field_activity_trail
                WHERE event_id IN (
                    SELECT event_id FROM field_activity_trail
                    WHERE application_id = ? AND updated_at < ?
                    ORDER BY updated_at
                    LIMIT ?
                )
                """
        )) {
            statement.setString(1, applicationId);
            statement.setTimestamp(2, Timestamp.from(cutoff));
            statement.setInt(3, BATCH);
            int deleted = statement.executeUpdate();
            connection.commit();
            return deleted;
        } catch (SQLException ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.setAutoCommit(previous);
        }
    }

    private record StoredFieldChangeRow(boolean inserted, byte[] storedHash) {
    }

    private record RetentionWindow(String applicationId, int days) {
    }
}

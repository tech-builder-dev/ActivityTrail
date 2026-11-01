package activitytrail.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Database access for activity. {@link PostgresFieldActivityRepository} is the implementation that
 * ships with the project. A host replaces that class to use another database.
 */
public interface FieldActivityRepository {

    boolean databaseIsUp();

    Instant currentDatabaseTime();

    Optional<RegisteredApplication> findApplication(String applicationId);

    RegisteredApplication registerApplication(String applicationId, String displayName, Integer retentionDays);

    FieldChangeStoreResult storeFieldChange(FieldChange change);

    List<FieldActivity> findFieldActivity(
            String applicationId,
            String objectName,
            String objectId,
            String field,
            Instant from,
            Instant to,
            FieldActivityPageMarker after,
            int limit
    );

    int deleteExpiredFieldActivity();
}

package activitytrail.repository;

import java.time.Instant;

public record RegisteredApplication(
        String applicationId,
        String displayName,
        Integer retentionDays,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt
) {
}

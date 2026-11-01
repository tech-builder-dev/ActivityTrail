package activitytrail.repository;

import java.time.Instant;
import java.util.UUID;

public record FieldActivity(
        UUID eventId,
        String objectField,
        String changeType,
        String oldValue,
        String newValue,
        String updatedBy,
        Instant updatedAt,
        Instant recordedAt,
        String correlationId
) {
}

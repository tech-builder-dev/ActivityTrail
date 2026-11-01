package activitytrail.repository;

import java.time.Instant;
import java.util.UUID;

public record FieldChange(
        UUID eventId,
        String applicationId,
        String objectName,
        String objectId,
        String objectField,
        String changeType,
        String oldValue,
        String newValue,
        String updatedBy,
        Instant updatedAt,
        String correlationId,
        byte[] bodyHash
) {
    public String messageKey() {
        return applicationId + ":" + objectName + ":" + objectId;
    }
}

package activitytrail.repository;

import java.time.Instant;
import java.util.UUID;

/** Where the previous page of an object's history ended. The next page starts after this row. */
public record FieldActivityPageMarker(Instant updatedAt, Instant recordedAt, UUID eventId) {
}

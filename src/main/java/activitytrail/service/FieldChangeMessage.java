package activitytrail.service;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.UUID;

/**
 * One Kafka field-change body. Null, blank, and pattern checks are annotations.
 * {@link ValidFieldChangeShape} checks set, update, and clear.
 */
@ValidFieldChangeShape
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class FieldChangeMessage {

    public static final String IDENTIFIER = "^[A-Za-z0-9._:-]{1,128}$";

    @NotNull
    private UUID eventId;

    @NotBlank
    @Pattern(regexp = IDENTIFIER)
    private String applicationId;

    @NotBlank
    @Pattern(regexp = IDENTIFIER)
    private String objectName;

    @NotBlank
    private String objectId;

    @NotBlank
    @Pattern(regexp = IDENTIFIER)
    private String objectField;

    @NotBlank
    @Pattern(regexp = "set|update|clear")
    private String changeType;

    private String oldValue;

    private String newValue;

    @NotBlank
    @Pattern(regexp = IDENTIFIER)
    private String updatedBy;

    @NotNull
    private Instant updatedAt;

    @Pattern(regexp = IDENTIFIER)
    private String correlationId;

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getObjectField() {
        return objectField;
    }

    public void setObjectField(String objectField) {
        this.objectField = objectField;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
}

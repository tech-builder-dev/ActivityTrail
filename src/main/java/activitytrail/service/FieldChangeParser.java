package activitytrail.service;

import activitytrail.repository.FieldChange;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/** Turns a Kafka message into a field change, or a reason the message cannot be stored. */
@Component
public class FieldChangeParser {

    static final String INVALID_JSON = "invalid_json";
    static final String VALIDATION_FAILED = "validation_failed";

    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
            .withZone(ZoneOffset.UTC);

    private final JsonMapper json;
    private final Validator validator;

    public FieldChangeParser(JsonMapper json, Validator validator) {
        this.json = json;
        this.validator = validator;
    }

    FieldChangeParseResult parseFieldChangeMessage(byte[] body, byte[] key) {
        if (body == null) {
            return FieldChangeParseResult.rejected(INVALID_JSON);
        }
        JsonNode root;
        try {
            root = json.readTree(body);
        } catch (JacksonException ex) {
            return FieldChangeParseResult.rejected(INVALID_JSON);
        }
        if (root == null || !root.isObject()) {
            return root == null ? FieldChangeParseResult.rejected(INVALID_JSON) : FieldChangeParseResult.rejected(VALIDATION_FAILED);
        }
        FieldChangeMessage message;
        try {
            message = json.treeToValue(root, FieldChangeMessage.class);
        } catch (JacksonException ex) {
            return FieldChangeParseResult.rejected(VALIDATION_FAILED);
        }
        if (!validator.validate(message).isEmpty()) {
            return FieldChangeParseResult.rejected(VALIDATION_FAILED);
        }
        FieldChange change = toFieldChange(message, new byte[0]);
        String actualKey = key == null ? null : new String(key, StandardCharsets.UTF_8);
        if (!change.messageKey().equals(actualKey)) {
            return FieldChangeParseResult.rejected(VALIDATION_FAILED);
        }
        return FieldChangeParseResult.accepted(toFieldChange(message, canonicalBodyHash(change)));
    }

    private static FieldChange toFieldChange(FieldChangeMessage message, byte[] bodyHash) {
        Instant updatedAt = message.getUpdatedAt().truncatedTo(ChronoUnit.MICROS);
        return new FieldChange(
                message.getEventId(),
                message.getApplicationId(),
                message.getObjectName(),
                message.getObjectId(),
                message.getObjectField(),
                message.getChangeType(),
                message.getOldValue(),
                message.getNewValue(),
                message.getUpdatedBy(),
                updatedAt,
                message.getCorrelationId(),
                bodyHash
        );
    }

    private byte[] canonicalBodyHash(FieldChange change) {
        ObjectNode node = json.createObjectNode();
        node.put("event_id", change.eventId().toString());
        node.put("application_id", change.applicationId());
        node.put("object_name", change.objectName());
        node.put("object_id", change.objectId());
        node.put("object_field", change.objectField());
        node.put("change_type", change.changeType());
        putTextOrNull(node, "old_value", change.oldValue());
        putTextOrNull(node, "new_value", change.newValue());
        node.put("updated_by", change.updatedBy());
        node.put("updated_at", TIMESTAMP.format(change.updatedAt()));
        putTextOrNull(node, "correlation_id", change.correlationId());
        try {
            return MessageDigest.getInstance("SHA-256").digest(json.writeValueAsBytes(node));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required", ex);
        }
    }

    private static void putTextOrNull(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    record FieldChangeParseResult(FieldChange change, String reason) {
        static FieldChangeParseResult accepted(FieldChange change) {
            return new FieldChangeParseResult(change, null);
        }

        static FieldChangeParseResult rejected(String reason) {
            return new FieldChangeParseResult(null, reason);
        }
    }
}

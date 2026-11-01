package activitytrail.controller;

import activitytrail.repository.FieldActivity;
import activitytrail.repository.ApplicationAlreadyRegistered;
import activitytrail.repository.FieldActivityPageMarker;
import activitytrail.repository.RegisteredApplication;
import activitytrail.service.FieldActivityService;
import activitytrail.service.FieldChangeMessage;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@Validated
public class FieldActivityController {

    private static final MediaType JSON_UTF8 = MediaType.parseMediaType("application/json;charset=UTF-8");
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
            .withZone(ZoneOffset.UTC);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final FieldActivityService fieldActivityService;
    private final int defaultRetentionDays;

    public FieldActivityController(FieldActivityService fieldActivityService, org.springframework.core.env.Environment environment) {
        this.fieldActivityService = fieldActivityService;
        this.defaultRetentionDays = Integer.parseInt(environment.getProperty("RETENTION_DAYS_DEFAULT", "365"));
    }

    @PostMapping(value = "/v1/operator/applications", produces = "application/json;charset=UTF-8")
    public ResponseEntity<ObjectNode> registerApplication(@Valid @RequestBody RegisterApplicationRequest request) {
        Integer retentionDays = defaultRetentionDays;
        if (request.isRetentionDaysSpecified()) {
            retentionDays = request.getRetentionDays();
        }
        try {
            RegisteredApplication created = fieldActivityService.registerApplication(
                    request.getApplicationId(), request.getDisplayName(), retentionDays);
            return ResponseEntity.status(201).contentType(JSON_UTF8).body(registeredApplicationJson(created));
        } catch (ApplicationAlreadyRegistered ex) {
            return errorResponse(409, "application_exists", "The application is already registered.");
        }
    }

    @GetMapping(value = "/v1/applications/{applicationId}/objects/{objectName}/{objectId}", produces = "application/json;charset=UTF-8")
    public ResponseEntity<ObjectNode> getFieldActivity(
            @PathVariable String applicationId,
            @PathVariable @Pattern(regexp = FieldChangeMessage.IDENTIFIER) String objectName,
            @PathVariable @Pattern(regexp = "^[^/]{1,256}$") String objectId,
            @RequestParam(required = false) @Pattern(regexp = "^$|" + FieldChangeMessage.IDENTIFIER) String field,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) @Min(1) @Max(200) Integer limit,
            @RequestParam(name = "cursor", required = false) String cursor
    ) {
        Optional<RegisteredApplication> application = fieldActivityService.findApplication(applicationId);
        if (application.isEmpty()) {
            return errorResponse(404, "application_not_found", "The application is not registered.");
        }
        if (!application.get().enabled()) {
            return errorResponse(403, "application_disabled", "This application is disabled.");
        }

        String selectedField = blankToNull(field);
        Instant selectedFrom = from;
        Instant selectedTo = to;
        int pageSize = limit == null ? 50 : limit;
        FieldActivityQueryCursor position = null;
        if (cursor != null) {
            position = decodeFieldActivityCursor(cursor);
            if (position == null) {
                return errorResponse(400, "invalid_cursor", "The cursor is not valid.");
            }
            if (field != null && !sameText(selectedField, position.field)) {
                return errorResponse(400, "invalid_cursor", "The cursor is not valid.");
            }
            if (from != null && !sameInstant(from, position.from)) {
                return errorResponse(400, "invalid_cursor", "The cursor is not valid.");
            }
            if (to != null && !sameInstant(to, position.to)) {
                return errorResponse(400, "invalid_cursor", "The cursor is not valid.");
            }
            if (field == null) {
                selectedField = position.field;
            }
            if (from == null) {
                selectedFrom = position.from;
            }
            if (to == null) {
                selectedTo = position.to;
            }
        }
        if (selectedFrom != null && selectedTo != null && !selectedFrom.isBefore(selectedTo)) {
            return errorResponse(400, "invalid_query", "The query is not valid.");
        }

        List<FieldActivity> rows = fieldActivityService.findFieldActivity(
                applicationId, objectName, objectId, selectedField, selectedFrom, selectedTo,
                position == null ? null : position.page, pageSize + 1
        );
        boolean more = rows.size() > pageSize;
        List<FieldActivity> page = more ? rows.subList(0, pageSize) : rows;
        ObjectNode body = JSON.createObjectNode();
        body.put("application_id", applicationId);
        body.put("object_name", objectName);
        body.put("object_id", objectId);
        ArrayNode items = body.putArray("items");
        for (FieldActivity item : page) {
            ObjectNode row = items.addObject();
            row.put("event_id", item.eventId().toString());
            row.put("object_field", item.objectField());
            row.put("change_type", item.changeType());
            putTextOrNull(row, "old_value", item.oldValue());
            putTextOrNull(row, "new_value", item.newValue());
            row.put("updated_by", item.updatedBy());
            row.put("updated_at", TIMESTAMP.format(item.updatedAt()));
            row.put("recorded_at", TIMESTAMP.format(item.recordedAt()));
            putTextOrNull(row, "correlation_id", item.correlationId());
        }
        if (more) {
            body.put("next_cursor", encodeNextFieldActivityCursor(page.get(page.size() - 1), selectedField, selectedFrom, selectedTo));
        } else {
            body.putNull("next_cursor");
        }
        return ResponseEntity.ok().contentType(JSON_UTF8).body(body);
    }

    @GetMapping(value = "/health", produces = "application/json;charset=UTF-8")
    public ResponseEntity<ObjectNode> databaseHealth() {
        boolean up = fieldActivityService.databaseIsUp();
        ObjectNode body = JSON.createObjectNode();
        body.put("status", up ? "ok" : "not_ready");
        return (up ? ResponseEntity.ok() : ResponseEntity.status(503)).contentType(JSON_UTF8).body(body);
    }

    private ObjectNode registeredApplicationJson(RegisteredApplication application) {
        ObjectNode row = JSON.createObjectNode();
        row.put("application_id", application.applicationId());
        row.put("display_name", application.displayName());
        if (application.retentionDays() == null) {
            row.putNull("retention_days");
        } else {
            row.put("retention_days", application.retentionDays());
        }
        row.put("enabled", application.enabled());
        return row;
    }

    private String encodeNextFieldActivityCursor(FieldActivity item, String field, Instant from, Instant to) {
        ObjectNode node = JSON.createObjectNode();
        node.put("v", 1);
        node.put("u", TIMESTAMP.format(item.updatedAt()));
        node.put("r", TIMESTAMP.format(item.recordedAt()));
        node.put("e", item.eventId().toString());
        putTextOrNull(node, "field", field);
        putTextOrNull(node, "from", from == null ? null : TIMESTAMP.format(from));
        putTextOrNull(node, "to", to == null ? null : TIMESTAMP.format(to));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(JSON.writeValueAsBytes(node));
    }

    private FieldActivityQueryCursor decodeFieldActivityCursor(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(Base64.getUrlDecoder().decode(raw));
            if (node == null || !node.isObject() || !node.has("v") || node.get("v").intValue() != 1) {
                return null;
            }
            FieldActivityPageMarker page = new FieldActivityPageMarker(
                    Instant.parse(node.get("u").stringValue()),
                    Instant.parse(node.get("r").stringValue()),
                    UUID.fromString(node.get("e").stringValue())
            );
            return new FieldActivityQueryCursor(page, readOptionalText(node, "field"), readOptionalInstant(node, "from"), readOptionalInstant(node, "to"));
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static String readOptionalText(JsonNode node, String field) {
        if (!node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).isString() ? node.get(field).stringValue() : null;
    }

    private static Instant readOptionalInstant(JsonNode node, String field) {
        String value = readOptionalText(node, field);
        if (value == null) {
            return null;
        }
        return Instant.parse(value);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static boolean sameText(String left, String right) {
        return left == null ? right == null : left.equals(right);
    }

    private static boolean sameInstant(Instant left, Instant right) {
        return left == null ? right == null : left.equals(right);
    }

    private static void putTextOrNull(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private ResponseEntity<ObjectNode> errorResponse(int status, String code, String message) {
        ObjectNode error = JSON.createObjectNode();
        error.put("code", code);
        error.put("message", message);
        ObjectNode body = JSON.createObjectNode();
        body.set("error", error);
        return ResponseEntity.status(status).contentType(JSON_UTF8).body(body);
    }

    private record FieldActivityQueryCursor(FieldActivityPageMarker page, String field, Instant from, Instant to) {
    }
}

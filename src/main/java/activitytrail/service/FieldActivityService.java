package activitytrail.service;

import activitytrail.repository.FieldActivity;
import activitytrail.repository.FieldActivityRepository;
import activitytrail.repository.FieldChange;
import activitytrail.repository.FieldActivityPageMarker;
import activitytrail.repository.RegisteredApplication;
import activitytrail.repository.FieldChangeStoreResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Registers applications, stores field changes from Kafka, and reads an object's activity.
 * Database calls go through {@link FieldActivityRepository}.
 */
@Service
public class FieldActivityService {

    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
            .withZone(ZoneOffset.UTC);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final FieldActivityRepository fieldActivityRepository;
    private final FieldChangeParser fieldChangeParser;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final String deadLetterTopic;

    public FieldActivityService(FieldActivityRepository fieldActivityRepository, FieldChangeParser fieldChangeParser,
                           KafkaTemplate<byte[], byte[]> kafka, org.springframework.core.env.Environment environment) {
        this.fieldActivityRepository = fieldActivityRepository;
        this.fieldChangeParser = fieldChangeParser;
        this.kafka = kafka;
        this.deadLetterTopic = environment.getProperty("activitytrail.dead-letter-topic", "activitytrail.field-changes.dlq");
    }

    public RegisteredApplication registerApplication(String applicationId, String displayName, Integer retentionDays) {
        return fieldActivityRepository.registerApplication(applicationId, displayName, retentionDays);
    }

    public Optional<RegisteredApplication> findApplication(String applicationId) {
        return fieldActivityRepository.findApplication(applicationId);
    }

    public List<FieldActivity> findFieldActivity(String applicationId, String objectName, String objectId,
                                    String field, Instant from, Instant to, FieldActivityPageMarker after, int limit) {
        return fieldActivityRepository.findFieldActivity(applicationId, objectName, objectId, field, from, to, after, limit);
    }

    public boolean databaseIsUp() {
        return fieldActivityRepository.databaseIsUp();
    }

    public void storeFieldChangeFromMessage(byte[] key, byte[] value) {
        FieldChangeParser.FieldChangeParseResult parsed = fieldChangeParser.parseFieldChangeMessage(value, key);
        if (parsed.reason() != null) {
            publishDeadLetter(parsed.reason(), key, value);
            return;
        }
        FieldChange change = parsed.change();
        Optional<RegisteredApplication> application = fieldActivityRepository.findApplication(change.applicationId());
        if (application.isEmpty()) {
            publishDeadLetter("unknown_application", key, value);
            return;
        }
        if (!application.get().enabled()) {
            publishDeadLetter("application_disabled", key, value);
            return;
        }
        if (change.updatedAt().isAfter(fieldActivityRepository.currentDatabaseTime().plus(Duration.ofMinutes(5)))) {
            publishDeadLetter(FieldChangeParser.VALIDATION_FAILED, key, value);
            return;
        }
        if (fieldActivityRepository.storeFieldChange(change) == FieldChangeStoreResult.CONFLICTING_EVENT) {
            publishDeadLetter("event_conflict", key, value);
        }
    }

    private void publishDeadLetter(String reason, byte[] key, byte[] value) {
        Instant failedAt = fieldActivityRepository.currentDatabaseTime();
        var body = JSON.createObjectNode();
        body.put("reason", reason);
        body.put("failed_at", TIMESTAMP.format(failedAt));
        body.put("original", value == null ? "" : new String(value, StandardCharsets.UTF_8));
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(deadLetterTopic, key, JSON.writeValueAsBytes(body));
        record.headers().add("X-Activity-Trail-Reason", reason.getBytes(StandardCharsets.UTF_8));
        try {
            kafka.send(record).get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Could not publish the dead letter", ex);
        }
    }
}

package activitytrail.controller;

import activitytrail.TestDatabase;
import activitytrail.repository.FieldActivityRepository;
import activitytrail.service.FieldChangeConsumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Use cases UC-1 through UC-9. The listener does not join a broker.
 * Publishing calls {@link FieldChangeConsumer} the same way the listener does.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
class FieldActivityUseCaseTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String DOB_EVENT_ID = "6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1001";
    private static final String EMAIL_EVENT_ID = "6f1c2a10-7b44-4e2a-9c31-0a6d5e8b1002";
    private static final String ORDER_EVENT_ID = "91aa0c33-1d20-4f77-8b10-55c0e1d20009";
    private static final String DOB_HASH = "5b4744408d6dd37355a2c37ec4a2674e7f9cba59319c921fbf41ab153f82eacb";
    private static final String DOB_KEY = "customer-profile:userProfile:u-100";

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> TestDatabase.POSTGRES.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("KAFKA_BROKERS", () -> "localhost:9092");
    }

    @LocalServerPort
    int port;

    @Autowired
    FieldChangeConsumer consumer;

    @Autowired
    FieldActivityRepository fieldActivityRepository;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @MockitoBean
    KafkaTemplate<byte[], byte[]> kafka;

    private final RestTemplate http = new RestTemplate(new JdkClientHttpRequestFactory());

    {
        http.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) {
                return false;
            }
        });
    }

    @BeforeEach
    void emptyDatabase() {
        jdbc.update("TRUNCATE field_activity_trail, application, retention_run RESTART IDENTITY");
        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void registerReturnsTheApplicationAndRejectsADuplicate() {
        ResponseEntity<String> created = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\"}");
        assertEquals(201, created.getStatusCode().value());
        JsonNode body = JSON.readTree(created.getBody());
        assertEquals("customer-profile", body.get("application_id").stringValue());
        assertEquals("Customer Profile", body.get("display_name").stringValue());
        assertEquals(365, body.get("retention_days").intValue());
        assertTrue(body.get("enabled").booleanValue());
        assertFalse(body.has("old_value"));

        ResponseEntity<String> orders = post("{\"application_id\":\"orders\",\"display_name\":\"Orders\",\"retention_days\":30}");
        assertEquals(201, orders.getStatusCode().value());
        assertEquals(30, JSON.readTree(orders.getBody()).get("retention_days").intValue());

        ResponseEntity<String> duplicate = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\"}");
        assertEquals(409, duplicate.getStatusCode().value());
        assertEquals("application_exists", errorCode(duplicate));
    }

    @Test
    void omittedRetentionUsesTheDefaultAndAnExplicitNullKeepsTheRows() {
        ResponseEntity<String> omitted = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\"}");
        assertEquals(365, JSON.readTree(omitted.getBody()).get("retention_days").intValue());

        ResponseEntity<String> forever = post("{\"application_id\":\"orders\",\"display_name\":\"Orders\",\"retention_days\":null}");
        assertEquals(201, forever.getStatusCode().value());
        assertTrue(JSON.readTree(forever.getBody()).get("retention_days").isNull());
    }

    @Test
    void retentionDaysOutsideTheRangeIsRejected() {
        ResponseEntity<String> tooSmall = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":0}");
        assertEquals(400, tooSmall.getStatusCode().value());
        assertEquals("validation_failed", errorCode(tooSmall));

        ResponseEntity<String> tooLarge = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":3651}");
        assertEquals(400, tooLarge.getStatusCode().value());
        assertEquals("validation_failed", errorCode(tooLarge));

        ResponseEntity<String> longest = post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":3650}");
        assertEquals(201, longest.getStatusCode().value());
        assertEquals(3650, JSON.readTree(longest.getBody()).get("retention_days").intValue());
    }

    @Test
    void uc1SupportReadsWhoChangedTheDateOfBirth() {
        registerDefaults();
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        root.addAppender(logs);
        try {
            publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
            assertNotDeadLettered();

            ResponseEntity<String> history = get("/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth");
            assertEquals(200, history.getStatusCode().value());
            JsonNode body = JSON.readTree(history.getBody());
            assertEquals(1, body.get("items").size());
            JsonNode item = body.get("items").get(0);
            assertEquals(DOB_EVENT_ID, item.get("event_id").stringValue());
            assertEquals("update", item.get("change_type").stringValue());
            assertEquals("1990-04-12", item.get("old_value").stringValue());
            assertEquals("1990-04-21", item.get("new_value").stringValue());
            assertEquals("user:88421", item.get("updated_by").stringValue());
            assertEquals("2026-11-01T09:14:02.000000Z", item.get("updated_at").stringValue());
            assertTrue(Instant.parse(item.get("recorded_at").stringValue()).isAfter(Instant.parse("2026-11-01T09:14:02Z")));
            assertEquals("save-7c2e91", item.get("correlation_id").stringValue());
            assertFalse(item.has("body_hash"));
            assertTrue(body.get("next_cursor").isNull());
            assertEquals(DOB_HASH, storedHash(DOB_EVENT_ID));
            String written = logs.list.stream().map(ILoggingEvent::getFormattedMessage).reduce("", (left, right) -> left + "\n" + right);
            assertFalse(written.contains("1990-04-12"));
            assertFalse(written.contains("1990-04-21"));
            assertFalse(written.contains("user:88421"));
        } finally {
            root.detachAppender(logs);
        }
    }

    @Test
    void uc2OneSaveStoresTwoRowsThatShareACorrelationId() {
        registerDefaults();
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        publish(DOB_KEY, emailEvent());

        JsonNode all = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody());
        assertEquals(2, all.get("items").size());
        assertEquals("save-7c2e91", all.get("items").get(0).get("correlation_id").stringValue());
        assertEquals("save-7c2e91", all.get("items").get(1).get("correlation_id").stringValue());

        JsonNode dates = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth").getBody());
        assertEquals(1, dates.get("items").size());
        assertEquals("dateOfBirth", dates.get("items").get(0).get("object_field").stringValue());
    }

    @Test
    void uc3OrderStatusStaysOnTheOrdersPath() {
        registerDefaults();
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        publish("orders:order:o-9", orderEvent());

        JsonNode profile = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody());
        assertEquals(1, profile.get("items").size());
        assertEquals("dateOfBirth", profile.get("items").get(0).get("object_field").stringValue());

        JsonNode order = JSON.readTree(get("/v1/applications/orders/objects/order/o-9").getBody());
        assertEquals(1, order.get("items").size());
        assertEquals("shipped", order.get("items").get(0).get("new_value").stringValue());
        assertEquals("system:fulfillment", order.get("items").get(0).get("updated_by").stringValue());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM field_activity_trail", Integer.class));
    }

    @Test
    void uc5AReadStaysOnThatObjectAndAnEmptyTrailIsAnEmptyList() {
        registerDefaults();
        ResponseEntity<String> empty = get("/v1/applications/customer-profile/objects/userProfile/u-100");
        assertEquals(200, empty.getStatusCode().value());
        JsonNode emptyBody = JSON.readTree(empty.getBody());
        assertEquals(0, emptyBody.get("items").size());
        assertTrue(emptyBody.get("next_cursor").isNull());

        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        publish(DOB_KEY, emailEvent());
        publish("customer-profile:userProfile:u-200", event(
                "11111111-1111-4111-8111-111111111111", "customer-profile", "userProfile", "u-200",
                "email", "set", null, "c@example.com", "user:88421", "2026-11-01T10:00:00Z", null));

        JsonNode u100 = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody());
        assertEquals(2, u100.get("items").size());
        for (JsonNode item : u100.get("items")) {
            assertFalse(item.get("event_id").stringValue().equals("11111111-1111-4111-8111-111111111111"));
        }
    }

    @Test
    void uc4SetAndClearAreStoredAndAnUnchangedUpdateIsRejected() {
        registerDefaults();
        publish(DOB_KEY, """
                {"event_id":"c0ffee00-0000-4000-8000-0000000000a1","application_id":"customer-profile","object_name":"userProfile","object_id":"u-100","object_field":"email","change_type":"set","old_value":null,"new_value":"a@example.com","updated_by":"user:88421","updated_at":"2026-10-01T12:00:00Z"}
                """);
        publish(DOB_KEY, """
                {"event_id":"c0ffee00-0000-4000-8000-0000000000a2","application_id":"customer-profile","object_name":"userProfile","object_id":"u-100","object_field":"email","change_type":"clear","old_value":"a@example.com","new_value":null,"updated_by":"user:88421","updated_at":"2026-10-02T12:00:00Z"}
                """);
        assertEquals(2, count("c0ffee00-0000-4000-8000-0000000000a1") + count("c0ffee00-0000-4000-8000-0000000000a2"));

        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000b1", "customer-profile", "userProfile", "u-100",
                "status", "update", "placed", "placed", "user:88421", "2026-10-03T12:00:00Z", null));
        assertDeadLetter("validation_failed");
        assertEquals(0, count("c0ffee00-0000-4000-8000-0000000000b1"));
    }

    @Test
    void uc4EmptyStringIsAValueAndARepeatedEmptyUpdateIsRejected() {
        registerDefaults();
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000c1", "customer-profile", "userProfile", "u-100",
                "nickname", "set", null, "", "user:88421", "2026-10-01T12:00:00Z", null));
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000c2", "customer-profile", "userProfile", "u-100",
                "nickname", "update", "", "x", "user:88421", "2026-10-02T12:00:00Z", null));
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000c3", "customer-profile", "userProfile", "u-100",
                "nickname", "clear", "", null, "user:88421", "2026-10-03T12:00:00Z", null));
        assertNotDeadLettered();
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM field_activity_trail", Integer.class));

        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000c4", "customer-profile", "userProfile", "u-100",
                "nickname", "update", "", "", "user:88421", "2026-10-04T12:00:00Z", null));
        assertDeadLetter("validation_failed");
        assertEquals(0, count("c0ffee00-0000-4000-8000-0000000000c4"));
    }

    @Test
    void uc4TwoSetsInARowAreBothStored() {
        registerDefaults();
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000d1", "customer-profile", "userProfile", "u-100",
                "email", "set", null, "a@example.com", "user:88421", "2026-10-01T12:00:00Z", null));
        publish(DOB_KEY, event(
                "c0ffee00-0000-4000-8000-0000000000d2", "customer-profile", "userProfile", "u-100",
                "email", "set", null, "b@example.com", "user:88421", "2026-10-02T12:00:00Z", null));
        assertNotDeadLettered();
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM field_activity_trail", Integer.class));
    }

    @Test
    void uc5UnknownAndDisabledApplicationsAndInvalidQueries() {
        ResponseEntity<String> missing = get("/v1/applications/customer-profile/objects/userProfile/u-100");
        assertEquals(404, missing.getStatusCode().value());
        assertEquals("application_not_found", errorCode(missing));

        registerDefaults();
        jdbc.update("UPDATE application SET enabled = false WHERE application_id = 'customer-profile'");
        ResponseEntity<String> disabled = get("/v1/applications/customer-profile/objects/userProfile/u-100");
        assertEquals(403, disabled.getStatusCode().value());
        assertEquals("application_disabled", errorCode(disabled));

        jdbc.update("UPDATE application SET enabled = true WHERE application_id = 'customer-profile'");
        ResponseEntity<String> badRange = get("/v1/applications/customer-profile/objects/userProfile/u-100?from=2026-11-02T00:00:00Z&to=2026-11-01T00:00:00Z");
        assertEquals(400, badRange.getStatusCode().value());
        assertEquals("invalid_query", errorCode(badRange));

        ResponseEntity<String> badLimit = get("/v1/applications/customer-profile/objects/userProfile/u-100?limit=0");
        assertEquals(400, badLimit.getStatusCode().value());
        assertEquals("invalid_query", errorCode(badLimit));

        ResponseEntity<String> badName = get("/v1/applications/customer-profile/objects/bad%20name/u-100");
        assertEquals(400, badName.getStatusCode().value());
        assertEquals("invalid_query", errorCode(badName));

        ResponseEntity<String> badCursor = get("/v1/applications/customer-profile/objects/userProfile/u-100?cursor=not-a-cursor");
        assertEquals(400, badCursor.getStatusCode().value());
        assertEquals("invalid_cursor", errorCode(badCursor));
    }

    @Test
    void uc5PagesDoNotOverlap() {
        registerDefaults();
        publish(DOB_KEY, event("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", "customer-profile", "userProfile", "u-100",
                "city", "set", null, "a", "user:1", "2026-11-01T09:00:00Z", null));
        publish(DOB_KEY, event("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", "customer-profile", "userProfile", "u-100",
                "city", "update", "a", "b", "user:1", "2026-11-01T10:00:00Z", null));
        publish(DOB_KEY, event("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3", "customer-profile", "userProfile", "u-100",
                "city", "update", "b", "c", "user:1", "2026-11-01T11:00:00Z", null));

        List<String> seen = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < 4; page++) {
            String path = "/v1/applications/customer-profile/objects/userProfile/u-100?limit=1";
            if (cursor != null) {
                path = path + "&cursor=" + cursor;
            }
            JsonNode body = JSON.readTree(get(path).getBody());
            if (body.get("items").isEmpty()) {
                assertTrue(body.get("next_cursor").isNull());
                break;
            }
            seen.add(body.get("items").get(0).get("event_id").stringValue());
            cursor = body.get("next_cursor").isNull() ? null : body.get("next_cursor").stringValue();
            if (cursor == null) {
                break;
            }
        }
        assertEquals(List.of(
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3",
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
        ), seen);
    }

    @Test
    void uc6DisablingOneApplicationDoesNotExposeTheOther() {
        registerDefaults();
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        publish("orders:order:o-9", orderEvent());
        jdbc.update("UPDATE application SET enabled = false WHERE application_id = 'customer-profile'");

        publish(DOB_KEY, event(
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1", "customer-profile", "userProfile", "u-100",
                "email", "set", null, "a@example.com", "user:88421", "2026-11-01T12:00:00Z", null));
        assertDeadLetter("application_disabled");
        assertEquals(0, count("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"));

        ResponseEntity<String> profile = get("/v1/applications/customer-profile/objects/userProfile/u-100");
        assertEquals(403, profile.getStatusCode().value());
        JsonNode orders = JSON.readTree(get("/v1/applications/orders/objects/order/o-9").getBody());
        assertEquals("shipped", orders.get("items").get(0).get("new_value").stringValue());
    }

    @Test
    void uc7InvalidMessagesAreDeadLetteredAndNotStored() {
        registerDefaults();
        publish(DOB_KEY, "not json");
        assertDeadLetter("invalid_json");

        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish(DOB_KEY, "[1,2]");
        assertDeadLetter("validation_failed");

        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z").replace("\"updated_by\":\"user:88421\",", ""));
        assertDeadLetter("validation_failed");
        assertEquals(0, count(DOB_EVENT_ID));

        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish("orders:userProfile:u-100", dateOfBirthEvent("cccccccc-cccc-4ccc-8ccc-ccccccccccc1", "2026-11-01T09:14:02Z"));
        assertDeadLetter("validation_failed");
        assertEquals(0, count("cccccccc-cccc-4ccc-8ccc-ccccccccccc1"));

        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish("unknown-app:userProfile:u-100", event(
                "cccccccc-cccc-4ccc-8ccc-ccccccccccc2", "unknown-app", "userProfile", "u-100",
                "email", "set", null, "a@example.com", "user:1", "2026-11-01T09:14:02Z", null));
        assertDeadLetter("unknown_application");
        assertEquals(0, count("cccccccc-cccc-4ccc-8ccc-ccccccccccc2"));
    }

    @Test
    void uc7AClockSkewOfMoreThanFiveMinutesIsRejected() {
        registerDefaults();
        Instant ahead = fieldActivityRepository.currentDatabaseTime().plus(Duration.ofMinutes(6));
        publish(DOB_KEY, event(
                "dddddddd-dddd-4ddd-8ddd-ddddddddddd1", "customer-profile", "userProfile", "u-100",
                "email", "set", null, "a@example.com", "user:88421", ahead.toString(), null));
        assertDeadLetter("validation_failed");
        assertEquals(0, count("dddddddd-dddd-4ddd-8ddd-ddddddddddd1"));
    }

    @Test
    void uc7ADeadLetterPublishFailureIsNotAcknowledged() {
        registerDefaults();
        reset(kafka);
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(failed);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        assertThrows(IllegalStateException.class, () -> consumer.storeFieldChange(
                record(DOB_KEY, "not json"), acknowledgment));
        verify(acknowledgment, never()).acknowledge();
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM field_activity_trail", Integer.class));
    }

    @Test
    void uc8TheSameCanonicalEventIsStoredOnce() {
        registerDefaults();
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        String recordedAt = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody())
                .get("items").get(0).get("recorded_at").stringValue();

        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID.toUpperCase(), "2026-11-01T09:14:02+00:00"));
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z").replace("}", ",\"note\":\"ignore me\"}"));
        assertNotDeadLettered();
        assertEquals(1, count(DOB_EVENT_ID));
        JsonNode item = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody()).get("items").get(0);
        assertEquals(recordedAt, item.get("recorded_at").stringValue());
        assertEquals("1990-04-21", item.get("new_value").stringValue());
        assertEquals(DOB_HASH, storedHash(DOB_EVENT_ID));
    }

    @Test
    void uc8ADifferentBodyWithTheSameEventIdDoesNotOverwrite() {
        registerDefaults();
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        reset(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z").replace("1990-04-21", "1991-01-01"));
        assertDeadLetter("event_conflict");
        JsonNode item = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth").getBody())
                .get("items").get(0);
        assertEquals("1990-04-21", item.get("new_value").stringValue());
        assertEquals(1, count(DOB_EVENT_ID));
    }

    @Test
    void at11AStoppedListenerStillServesStoredHistory() {
        registerDefaults();
        assertTrue(listenerRegistry.getListenerContainers().stream().noneMatch(container -> container.isRunning()));
        publish(DOB_KEY, dateOfBirthEvent(DOB_EVENT_ID, "2026-11-01T09:14:02Z"));
        ResponseEntity<String> history = get("/v1/applications/customer-profile/objects/userProfile/u-100?field=dateOfBirth");
        assertEquals(200, history.getStatusCode().value());
        assertEquals("1990-04-21", JSON.readTree(history.getBody()).get("items").get(0).get("new_value").stringValue());
    }

    @Test
    void uc9RetentionDeletesAgedRowsAndKeepsRecentOnes() {
        post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":1}");
        Instant databaseNow = fieldActivityRepository.currentDatabaseTime();
        publish(DOB_KEY, event(
                "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1", "customer-profile", "userProfile", "u-100",
                "city", "set", null, "old", "user:1", databaseNow.minus(Duration.ofDays(2)).toString(), null));
        publish(DOB_KEY, event(
                "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee2", "customer-profile", "userProfile", "u-100",
                "city", "update", "old", "new", "user:1", databaseNow.minus(Duration.ofHours(1)).toString(), null));

        int deleted = fieldActivityRepository.deleteExpiredFieldActivity();
        assertTrue(deleted >= 1);
        assertEquals(0, count("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1"));
        assertEquals(1, count("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee2"));
        JsonNode items = JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody()).get("items");
        assertEquals(1, items.size());
        assertEquals("new", items.get(0).get("new_value").stringValue());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM retention_run", Integer.class));
    }

    @Test
    void uc9ANullWindowIsKeptAndAnotherApplicationsAgedRowIsDeleted() {
        post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":null}");
        post("{\"application_id\":\"orders\",\"display_name\":\"Orders\",\"retention_days\":1}");
        Instant databaseNow = fieldActivityRepository.currentDatabaseTime();
        publish(DOB_KEY, event(
                "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee3", "customer-profile", "userProfile", "u-100",
                "city", "set", null, "kept", "user:1", databaseNow.minus(Duration.ofDays(3)).toString(), null));
        publish("orders:order:o-9", event(
                ORDER_EVENT_ID, "orders", "order", "o-9",
                "status", "set", null, "placed", "system:fulfillment", databaseNow.minus(Duration.ofDays(3)).toString(), null));

        fieldActivityRepository.deleteExpiredFieldActivity();
        assertEquals(1, count("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee3"));
        assertEquals(0, count(ORDER_EVENT_ID));
        assertEquals("kept", JSON.readTree(get("/v1/applications/customer-profile/objects/userProfile/u-100").getBody())
                .get("items").get(0).get("new_value").stringValue());
    }

    @Test
    void uc9ADisabledApplicationWithAWindowIsStillDeleted() {
        post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":1}");
        Instant databaseNow = fieldActivityRepository.currentDatabaseTime();
        publish(DOB_KEY, event(
                "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee4", "customer-profile", "userProfile", "u-100",
                "city", "set", null, "old", "user:1", databaseNow.minus(Duration.ofDays(2)).toString(), null));
        jdbc.update("UPDATE application SET enabled = false WHERE application_id = 'customer-profile'");

        fieldActivityRepository.deleteExpiredFieldActivity();
        assertEquals(0, count("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee4"));
    }

    @Test
    void healthReportsTheDatabase() {
        ResponseEntity<String> health = get("/health");
        assertEquals(200, health.getStatusCode().value());
        assertEquals("ok", JSON.readTree(health.getBody()).get("status").stringValue());
    }

    private void registerDefaults() {
        assertEquals(201, post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\"}").getStatusCode().value());
        assertEquals(201, post("{\"application_id\":\"orders\",\"display_name\":\"Orders\"}").getStatusCode().value());
    }

    private void publish(String key, String json) {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        consumer.storeFieldChange(record(key, json), acknowledgment);
        verify(acknowledgment).acknowledge();
    }

    private static ConsumerRecord<byte[], byte[]> record(String key, String json) {
        return new ConsumerRecord<>(
                "activitytrail.field-changes",
                0,
                0L,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                json == null ? null : json.getBytes(StandardCharsets.UTF_8)
        );
    }

    private void assertDeadLetter(String reason) {
        org.mockito.ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captor.capture());
        ProducerRecord<byte[], byte[]> sent = captor.getValue();
        assertEquals("activitytrail.field-changes.dlq", sent.topic());
        assertEquals(reason, new String(sent.headers().lastHeader("X-Activity-Trail-Reason").value(), StandardCharsets.UTF_8));
        JsonNode body = JSON.readTree(sent.value());
        assertEquals(reason, body.get("reason").stringValue());
        assertFalse(body.get("failed_at").stringValue().isBlank());
        assertTrue(body.has("original"));
    }

    private void assertNotDeadLettered() {
        verify(kafka, never()).send(any(ProducerRecord.class));
    }

    private int count(String eventId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM field_activity_trail WHERE event_id::text = ?",
                Integer.class,
                eventId.toLowerCase()
        );
        return count == null ? 0 : count;
    }

    private String storedHash(String eventId) {
        return jdbc.queryForObject(
                "SELECT encode(body_hash, 'hex') FROM field_activity_trail WHERE event_id::text = ?",
                String.class,
                eventId.toLowerCase()
        );
    }

    private ResponseEntity<String> get(String pathAndQuery) {
        return http.getForEntity("http://localhost:" + port + pathAndQuery, String.class);
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<String> post(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return http.postForEntity("http://localhost:" + port + "/v1/operator/applications", new HttpEntity<>(body, headers), String.class);
    }

    private static String errorCode(ResponseEntity<String> response) {
        return JSON.readTree(response.getBody()).get("error").get("code").stringValue();
    }

    private static String dateOfBirthEvent(String eventId, String updatedAt) {
        return event(eventId, "customer-profile", "userProfile", "u-100", "dateOfBirth", "update",
                "1990-04-12", "1990-04-21", "user:88421", updatedAt, "save-7c2e91");
    }

    private static String emailEvent() {
        return event(EMAIL_EVENT_ID, "customer-profile", "userProfile", "u-100", "email", "update",
                "a@example.com", "b@example.com", "user:88421", "2026-11-01T09:14:02Z", "save-7c2e91");
    }

    private static String orderEvent() {
        return event(ORDER_EVENT_ID, "orders", "order", "o-9", "status", "update",
                "placed", "shipped", "system:fulfillment", "2026-11-01T09:15:00Z", null);
    }

    private static String event(
            String eventId,
            String applicationId,
            String objectName,
            String objectId,
            String field,
            String changeType,
            String oldValue,
            String newValue,
            String updatedBy,
            String updatedAt,
            String correlationId
    ) {
        ObjectNode node = JSON.createObjectNode();
        node.put("event_id", eventId);
        node.put("application_id", applicationId);
        node.put("object_name", objectName);
        node.put("object_id", objectId);
        node.put("object_field", field);
        node.put("change_type", changeType);
        putTextOrNull(node, "old_value", oldValue);
        putTextOrNull(node, "new_value", newValue);
        node.put("updated_by", updatedBy);
        node.put("updated_at", updatedAt);
        if (correlationId != null) {
            node.put("correlation_id", correlationId);
        }
        return JSON.writeValueAsString(node);
    }

    private static void putTextOrNull(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }
}

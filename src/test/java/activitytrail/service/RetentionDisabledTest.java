package activitytrail.service;

import activitytrail.TestDatabase;

import org.junit.jupiter.api.Test;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UC-9 when deletion is turned off. The cleanup bean is absent, so an aged row stays readable.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "RETENTION_ENABLED=false")
@ActiveProfiles("local")
class RetentionDisabledTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

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
    org.springframework.context.ApplicationContext applicationContext;

    @Autowired
    JdbcTemplate jdbc;

    private final RestTemplate http = new RestTemplate(new JdkClientHttpRequestFactory());

    {
        http.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) {
                return false;
            }
        });
    }

    @Test
    void uc9TurningRetentionOffLeavesAgedRowsInPlace() {
        assertTrue(applicationContext.getBeansOfType(ExpiredFieldActivityCleanup.class).isEmpty());
        jdbc.update("TRUNCATE field_activity_trail, application, retention_run RESTART IDENTITY");
        post("{\"application_id\":\"customer-profile\",\"display_name\":\"Customer Profile\",\"retention_days\":1}");
        String updatedAt = jdbc.queryForObject("SELECT clock_timestamp()", java.time.OffsetDateTime.class)
                .toInstant()
                .minus(Duration.ofDays(2))
                .toString();
        consumer.storeFieldChange(
                new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                        "activitytrail.field-changes",
                        0,
                        0L,
                        "customer-profile:userProfile:u-100".getBytes(StandardCharsets.UTF_8),
                        event(updatedAt).getBytes(StandardCharsets.UTF_8)
                ),
                () -> {
                }
        );

        JsonNode items = JSON.readTree(http.getForEntity(
                "http://localhost:" + port + "/v1/applications/customer-profile/objects/userProfile/u-100",
                String.class
        ).getBody()).get("items");
        assertEquals(1, items.size());
        assertEquals("kept", items.get(0).get("new_value").stringValue());
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<String> post(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return http.postForEntity("http://localhost:" + port + "/v1/operator/applications", new HttpEntity<>(body, headers), String.class);
    }

    private static String event(String updatedAt) {
        return """
                {"event_id":"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee9","application_id":"customer-profile","object_name":"userProfile","object_id":"u-100","object_field":"city","change_type":"set","old_value":null,"new_value":"kept","updated_by":"user:1","updated_at":"%s"}
                """.formatted(updatedAt);
    }
}

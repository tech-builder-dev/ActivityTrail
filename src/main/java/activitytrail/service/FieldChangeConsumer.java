package activitytrail.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Starts with the process and hands each Kafka record to {@link FieldActivityService}. */
@Component
public class FieldChangeConsumer {

    private final FieldActivityService fieldActivityService;

    public FieldChangeConsumer(FieldActivityService fieldActivityService) {
        this.fieldActivityService = fieldActivityService;
    }

    @KafkaListener(topics = "${activitytrail.topic:activitytrail.field-changes}", groupId = "${activitytrail.group:activitytrail}")
    public void storeFieldChange(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
        fieldActivityService.storeFieldChangeFromMessage(record.key(), record.value());
        acknowledgment.acknowledge();
    }
}

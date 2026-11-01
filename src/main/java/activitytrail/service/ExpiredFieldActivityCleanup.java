package activitytrail.service;

import activitytrail.repository.FieldActivityRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Deletes activity older than each application's retention window. Runs in this process at 03:00 UTC. */
@Component
@ConditionalOnProperty(name = "RETENTION_ENABLED", havingValue = "true", matchIfMissing = true)
class ExpiredFieldActivityCleanup {

    private final FieldActivityRepository fieldActivityRepository;

    ExpiredFieldActivityCleanup(FieldActivityRepository fieldActivityRepository) {
        this.fieldActivityRepository = fieldActivityRepository;
    }

    @Scheduled(cron = "0 0 3 * * *", zone = "UTC")
    void deleteExpiredFieldActivity() {
        fieldActivityRepository.deleteExpiredFieldActivity();
    }
}

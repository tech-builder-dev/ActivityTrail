package activitytrail;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Set;

@SpringBootApplication
@EnableScheduling
public class ActivityTrailApplication {

    private static final Set<String> ENVIRONMENTS = Set.of("local", "qal", "e2e", "prod");

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ActivityTrailApplication.class);
        application.addInitializers(ActivityTrailApplication::requireEnvironment);
        application.run(args);
    }

    static void requireEnvironment(ConfigurableApplicationContext context) {
        boolean selected = false;
        for (String profile : context.getEnvironment().getActiveProfiles()) {
            if (ENVIRONMENTS.contains(profile)) {
                selected = true;
                break;
            }
        }
        if (!selected) {
            throw new IllegalStateException("Set spring.profiles.active to local, qal, e2e, or prod");
        }
    }
}

package activitytrail.repository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class PostgresRepositoryConfiguration {

    @Bean
    FieldActivityRepository fieldActivityRepository(DataSource dataSource) {
        return new PostgresFieldActivityRepository(dataSource);
    }
}

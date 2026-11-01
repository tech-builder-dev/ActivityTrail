package activitytrail;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/** One PostgreSQL process shared by the application tests. */
public final class TestDatabase {

    public static final EmbeddedPostgres POSTGRES;

    static {
        try {
            POSTGRES = EmbeddedPostgres.start();
        } catch (Exception ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private TestDatabase() {
    }
}

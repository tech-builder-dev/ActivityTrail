package activitytrail.repository;

public class ApplicationAlreadyRegistered extends RuntimeException {

    public ApplicationAlreadyRegistered(String applicationId) {
        super(applicationId);
    }
}

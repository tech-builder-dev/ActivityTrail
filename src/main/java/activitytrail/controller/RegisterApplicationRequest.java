package activitytrail.controller;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Body of POST /v1/operator/applications. Null and blank checks are Bean Validation annotations.
 * An omitted retention_days uses the deployment default. A present null keeps the rows.
 */
public class RegisterApplicationRequest {

    static final String IDENTIFIER = "^[A-Za-z0-9._:-]{1,128}$";

    @NotBlank(message = "application_id and display_name are required.")
    @Pattern(regexp = IDENTIFIER, message = "application_id or display_name is not valid.")
    private String applicationId;

    @NotBlank(message = "application_id and display_name are required.")
    @Size(max = 200, message = "application_id or display_name is not valid.")
    private String displayName;

    @Min(value = 1, message = "retention_days must be an integer from 1 to 3650, or null.")
    @Max(value = 3650, message = "retention_days must be an integer from 1 to 3650, or null.")
    private Integer retentionDays;

    @JsonIgnore
    private boolean retentionDaysSpecified;

    @JsonProperty("application_id")
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty("application_id")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("retention_days")
    public Integer getRetentionDays() {
        return retentionDays;
    }

    @JsonProperty("retention_days")
    public void setRetentionDays(Integer retentionDays) {
        this.retentionDays = retentionDays;
        this.retentionDaysSpecified = true;
    }

    @JsonIgnore
    public boolean isRetentionDaysSpecified() {
        return retentionDaysSpecified;
    }
}

package activitytrail.service;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/** Cross-field rules that @NotNull and @NotBlank cannot express on their own. */
public class FieldChangeShapeValidator implements ConstraintValidator<ValidFieldChangeShape, FieldChangeMessage> {

    @Override
    public boolean isValid(FieldChangeMessage message, ConstraintValidatorContext context) {
        if (message == null) {
            return true;
        }
        String objectId = message.getObjectId();
        if (objectId != null && (objectId.indexOf('/') >= 0 || objectId.isBlank()
                || objectId.codePointCount(0, objectId.length()) > 256)) {
            return false;
        }
        if (exceedsValueLength(message.getOldValue()) || exceedsValueLength(message.getNewValue())) {
            return false;
        }
        return switch (message.getChangeType()) {
            case null -> true;
            case "set" -> message.getOldValue() == null && message.getNewValue() != null;
            case "update" -> message.getOldValue() != null && message.getNewValue() != null
                    && !message.getOldValue().equals(message.getNewValue());
            case "clear" -> message.getOldValue() != null && message.getNewValue() == null;
            default -> true;
        };
    }

    private static boolean exceedsValueLength(String value) {
        return value != null && value.codePointCount(0, value.length()) > 8192;
    }
}

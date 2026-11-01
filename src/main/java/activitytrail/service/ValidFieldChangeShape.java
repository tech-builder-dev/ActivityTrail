package activitytrail.service;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** set requires a new value, update requires two different values, clear requires an old value. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = FieldChangeShapeValidator.class)
public @interface ValidFieldChangeShape {

    String message() default "old_value and new_value do not match change_type";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}

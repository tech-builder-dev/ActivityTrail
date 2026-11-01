package activitytrail.controller;

import jakarta.validation.ConstraintViolationException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** Turns Bean Validation failures into the activity error envelope. */
@RestControllerAdvice
class ApiExceptionHandler {

    private static final MediaType JSON_UTF8 = MediaType.parseMediaType("application/json;charset=UTF-8");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ObjectNode> invalidRequestBody(MethodArgumentNotValidException exception) {
        String message = "The request is not valid.";
        if (exception.getBindingResult().getFieldError() != null
                && exception.getBindingResult().getFieldError().getDefaultMessage() != null) {
            message = exception.getBindingResult().getFieldError().getDefaultMessage();
        }
        return error(400, "validation_failed", message);
    }

    @ExceptionHandler({
            HandlerMethodValidationException.class,
            ConstraintViolationException.class,
            MethodArgumentTypeMismatchException.class
    })
    ResponseEntity<ObjectNode> invalidQuery() {
        return error(400, "invalid_query", "The query is not valid.");
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ObjectNode> unreadableBody() {
        return error(400, "validation_failed", "The request is not valid.");
    }

    private static ResponseEntity<ObjectNode> error(int status, String code, String message) {
        ObjectNode error = JSON.createObjectNode();
        error.put("code", code);
        error.put("message", message);
        ObjectNode body = JSON.createObjectNode();
        body.set("error", error);
        return ResponseEntity.status(status).contentType(JSON_UTF8).body(body);
    }
}

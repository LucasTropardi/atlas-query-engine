package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ConnectionNotFoundException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ExternalQueryConnectionException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.InactiveConnectionException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.JdbcConnectionConfigurationException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.UnsupportedDatabaseTypeException;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(DatasetNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleDatasetNotFound(
            DatasetNotFoundException exception,
            HttpServletRequest request
    ) {
        LOGGER.warn("Business error on path={}: {}", request.getRequestURI(), exception.getMessage());
        return buildError(HttpStatus.BAD_REQUEST, "dataset_not_found", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(InvalidQueryException.class)
    public ResponseEntity<ApiErrorResponse> handleInvalidQuery(
            InvalidQueryException exception,
            HttpServletRequest request
    ) {
        LOGGER.warn("Validation error on path={}: {}", request.getRequestURI(), exception.getMessage());
        return buildError(HttpStatus.BAD_REQUEST, "invalid_query", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler({
            ConnectionNotFoundException.class,
            InactiveConnectionException.class,
            UnsupportedDatabaseTypeException.class,
            JdbcConnectionConfigurationException.class
    })
    public ResponseEntity<ApiErrorResponse> handleConnectionConfigurationError(
            RuntimeException exception,
            HttpServletRequest request
    ) {
        LOGGER.warn("Connection resolution error on path={}: {}", request.getRequestURI(), exception.getMessage());
        return buildError(HttpStatus.BAD_REQUEST, "connection_resolution_error", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(ExternalQueryConnectionException.class)
    public ResponseEntity<ApiErrorResponse> handleExternalQueryConnectionError(
            ExternalQueryConnectionException exception,
            HttpServletRequest request
    ) {
        LOGGER.error("External connection error on path={}: {}", request.getRequestURI(), exception.getMessage(), exception);
        return buildError(HttpStatus.BAD_GATEWAY, "external_connection_error", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidation(
            MethodArgumentNotValidException exception,
            HttpServletRequest request
    ) {
        String message = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + " " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));
        LOGGER.warn("Request body validation error on path={}: {}", request.getRequestURI(), message);
        return buildError(HttpStatus.BAD_REQUEST, "request_validation_error", message, request.getRequestURI());
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiErrorResponse> handleDataAccessException(
            DataAccessException exception,
            HttpServletRequest request
    ) {
        LOGGER.error("Unexpected database error on path={}", request.getRequestURI(), exception);
        return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "database_error",
                "Unexpected database error while executing the query", request.getRequestURI());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleUnexpectedException(
            Exception exception,
            HttpServletRequest request
    ) {
        LOGGER.error("Unexpected error on path={}", request.getRequestURI(), exception);
        return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "internal_server_error",
                "Unexpected internal error", request.getRequestURI());
    }

    private ResponseEntity<ApiErrorResponse> buildError(
            HttpStatus status,
            String error,
            String message,
            String path
    ) {
        return ResponseEntity.status(status).body(new ApiErrorResponse(
                error,
                message,
                Instant.now(),
                path,
                status.value()
        ));
    }
}

package br.com.lucast.atlas_query_engine.demo.controller;

import java.time.Instant;

public record ApiErrorResponse(
        String error,
        String message,
        Instant timestamp,
        String path,
        int status
) {
}

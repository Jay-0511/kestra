package io.kestra.core.runners;

import io.kestra.core.models.HasUID;
import io.kestra.core.models.executions.Execution;

import java.time.Instant;

public record ExecutionEvent(String tenantId, String executionId, Instant eventDate, ExecutionEventType eventType) implements HasUID {
    public ExecutionEvent(Execution execution, ExecutionEventType eventType) {
        this(execution.getTenantId(), execution.getId(), Instant.now(), eventType);
    }

    @Override
    public String uid() {
        return executionId;
    }
}

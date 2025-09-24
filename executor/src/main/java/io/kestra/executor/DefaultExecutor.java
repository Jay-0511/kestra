package io.kestra.executor;

import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.exceptions.InternalException;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.*;
import io.kestra.core.models.tasks.WorkerGroup;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.ExecutionRepositoryInterface;
import io.kestra.core.runners.*;
import io.kestra.core.server.ServiceStateChangeEvent;
import io.kestra.core.server.ServiceType;
import io.kestra.core.services.PluginDefaultService;
import io.kestra.core.services.WorkerGroupService;
import io.kestra.core.trace.Tracer;
import io.kestra.core.trace.TracerFactory;
import io.kestra.core.utils.Either;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TruthUtils;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@Slf4j
public class DefaultExecutor implements ExecutorInterface {
    @Inject
    private ApplicationEventPublisher<ServiceStateChangeEvent> eventPublisher;

    @Inject
    private ExecutionRepositoryInterface executionRepository;
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_EVENT_NAMED)
    private QueueInterface<ExecutionEvent> executionEventQueue;
    @Inject
    @Named(QueueFactoryInterface.WORKERJOB_NAMED)
    private QueueInterface<WorkerJob> workerJobQueue;
    @Inject
    @Named(QueueFactoryInterface.WORKERTASKRESULT_NAMED)
    private QueueInterface<WorkerTaskResult> workerTaskResultQueue;
    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Inject
    private SkipExecutionService skipExecutionService;
    @Inject
    private PluginDefaultService pluginDefaultService;
    @Inject
    private ExecutorService executorService;
    @Inject
    private WorkerGroupService workerGroupService;

    @Inject
    private FlowMetaStoreInterface flowMetaStore;

    private final AtomicReference<ServiceState> state = new AtomicReference<>();
    private final String id = IdUtils.create();
    private final List<Runnable> receiveCancellations = new ArrayList<>();

    private final Tracer tracer;

    @Inject
    public DefaultExecutor(TracerFactory tracerFactory) {
        this.tracer = tracerFactory.getTracer(DefaultExecutor.class, "EXECUTOR");
    }

    @Override
    public void run() {
        setState(ServiceState.CREATED);

        // listen to executor related queues
        this.receiveCancellations.addFirst(this.executionQueue.receive(Executor.class, execution -> executionQueue(execution)));
        this.receiveCancellations.addFirst(this.executionEventQueue.receive(Executor.class, execution -> executionEventQueue(execution)));
        this.receiveCancellations.addFirst(this.workerTaskResultQueue.receive(Executor.class, workerTaskResults -> workerTaskResultQueue(workerTaskResults)));

        setState(ServiceState.RUNNING);
        log.info("Executor started");
    }

    // This serves as a temporal bridge between the old execution queue and the new execution event queue to avoid updating all code that uses the old queue
    private void executionQueue(Either<Execution, DeserializationException> either) {
        if (either.isRight()) {
            log.error("Unable to deserialize an execution: {}", either.getRight().getMessage());
            return;
        }

        Execution message = either.getLeft();
        if (skipExecutionService.skipExecution(message)) {
            log.warn("Skipping execution {}", message.getId());
            return;
        }

        try {
            executionEventQueue.emit(new ExecutionEvent(message, ExecutionEventType.CREATED));
        } catch (QueueException e) {
            // If we cannot send the execution event we fail the execution
            executionRepository.lock(message.getId(), pair -> {
                Execution execution = pair.getLeft();
                try {
                    Execution failed = execution.failedExecutionFromExecutor(e).getExecution().withState(State.Type.FAILED);
                    ExecutionEvent event = new ExecutionEvent(failed, ExecutionEventType.UPDATED); // TODO terminated
                    // TODO transaction between repo and queue
                    this.executionRepository.update(failed);
                    this.executionEventQueue.emit(event);
                } catch (QueueException ex) {
                    log.error("Unable to emit the execution {}", execution.getId(), ex);
                }
                return null;
            });
        }
    }

    private void executionEventQueue(Either<ExecutionEvent, DeserializationException> either) {
        if (either.isRight()) {
            log.error("Unable to deserialize an execution: {}", either.getRight().getMessage());
            return;
        }

        ExecutionEvent message = either.getLeft();
        if (skipExecutionService.skipExecution(message.executionId())) { // TODO we may add tenant/namespace/flow for skip them
            log.warn("Skipping execution {}", message.executionId());
            return;
        }

        // FIXME remove the state as it's not needed anymore
        Executor result = executionRepository.lock(message.executionId(), pair -> {
            Execution execution = pair.getLeft();
            ExecutorState executorState = pair.getRight();

            return tracer.inCurrentContext(
                execution,
                FlowId.uidWithoutRevision(execution),
                () -> {
                        final FlowWithSource flow = findFlow(execution);
                        Executor executor = new Executor(execution, null).withFlow(flow);

                        // process the execution
                        if (log.isDebugEnabled()) {
                            executorService.log(log, true, executor);
                        }
                        executor = executorService.process(executor);

                        if (!executor.getNexts().isEmpty()) {
                            executor.withExecution(
                                executorService.onNexts(executor.getExecution(), executor.getNexts()),
                                "onNexts"
                            );
                        }

                        // worker task
                        if (!executor.getWorkerTasks().isEmpty()) {
                            List<WorkerTaskResult> workerTaskResults = new ArrayList<>();
                            executor
                                .getWorkerTasks()
                                .forEach(throwConsumer(workerTask -> {
                                    try {
                                        if (!TruthUtils.isTruthy(workerTask.getRunContext().render(workerTask.getTask().getRunIf()))) {
                                            workerTaskResults.add(new WorkerTaskResult(workerTask.getTaskRun().withState(State.Type.SKIPPED)));
                                        } else {
                                            if (workerTask.getTask().isSendToWorkerTask()) {
                                                Optional<WorkerGroup> maybeWorkerGroup = workerGroupService.resolveGroupFromJob(flow, workerTask);
                                                String workerGroupKey = maybeWorkerGroup.map(throwFunction(workerGroup -> workerTask.getRunContext().render(workerGroup.getKey())))
                                                    .orElse(null);
                                                workerJobQueue.emit(workerGroupKey, workerTask);
                                            }
                                            if (workerTask.getTask().isFlowable()) {
                                                workerTaskResults.add(new WorkerTaskResult(workerTask.getTaskRun().withState(State.Type.RUNNING)));
                                            }
                                        }
                                    } catch (Exception e) {
                                        workerTaskResults.add(new WorkerTaskResult(workerTask.getTaskRun().withState(State.Type.FAILED)));
                                        workerTask.getRunContext().logger().error("Failed to evaluate the runIf condition for task {}. Cause: {}", workerTask.getTask().getId(), e.getMessage(), e);
                                    }
                                }));

                            try {
                                executorService.addWorkerTaskResults(executor, workerTaskResults);
                            } catch (InternalException e) {
                                log.error("Unable to add a worker task result to the execution", e);
                            }
                        }

                        return Pair.of(
                            executor,
                            executorState
                        );
                }
            );
        });

        if (result != null) {
            this.toExecution(result);
        }
    }

    private void workerTaskResultQueue(Either<WorkerTaskResult, DeserializationException> either) {
        if (either.isRight()) {
            log.error("Unable to deserialize a worker task result: {}", either.getRight().getMessage(), either.getRight());
            return;
        }

        WorkerTaskResult message = either.getLeft();
        if (skipExecutionService.skipExecution(message.getTaskRun())) {
            log.warn("Skipping execution {}", message.getTaskRun().getExecutionId());
            return;
        }

        if (log.isDebugEnabled()) {
            executorService.log(log, true, message);
        }

        Executor executor = executionRepository.lock(message.getTaskRun().getExecutionId(), pair -> {
            Execution execution = pair.getLeft();
            Executor current = new Executor(execution, null);

            if (execution == null) {
                throw new IllegalStateException("Execution state don't exist for " + message.getTaskRun().getExecutionId() + ", receive " + message);
            }

            if (execution.hasTaskRunJoinable(message.getTaskRun())) {
                try {
                    // process worker task result
                    executorService.addWorkerTaskResult(current, () -> findFlow(execution), message);
                    // join worker result
                    return Pair.of(
                        current,
                        pair.getRight()
                    );
                } catch (InternalException e) {
                    return Pair.of(
                        handleFailedExecutionFromExecutor(current, e),
                        pair.getRight()
                    );
                }
            }

            return null;
        });

        if (executor != null) {
            this.toExecution(executor);
        }
    }

    private void toExecution(Executor executor) {
        try {
            boolean shouldSend = false;

            if (executor.getException() != null) {
                executor = handleFailedExecutionFromExecutor(executor, executor.getException());
                shouldSend = true;
            } else if (executor.isExecutionUpdated()) {
                shouldSend = true;
            }

            if (!shouldSend) {
                return;
            }

            if (log.isDebugEnabled()) {
                executorService.log(log, false, executor);
            }
            ExecutionEvent event = new ExecutionEvent(executor.getExecution(), ExecutionEventType.UPDATED); // TODO terminated
            // TODO transaction between repo and queue
            this.executionRepository.update(executor.getExecution());
            this.executionEventQueue.emit(event);
        } catch (QueueException e) {
            // If we cannot add the new worker task result to the execution, we fail it
            executionRepository.lock(executor.getExecution().getId(), pair -> {
                Execution execution = pair.getLeft();
                try {
                    Execution failed = execution.failedExecutionFromExecutor(e).getExecution().withState(State.Type.FAILED);
                    ExecutionEvent event = new ExecutionEvent(failed, ExecutionEventType.UPDATED); // TODO terminated
                    // TODO transaction between repo and queue
                    this.executionRepository.update(failed);
                    this.executionEventQueue.emit(event);
                } catch (QueueException ex) {
                    log.error("Unable to emit the execution {}", execution.getId(), ex);
                }
                return null;
            });
        }
    }

    private FlowWithSource findFlow(Execution execution) {
        FlowInterface flow = flowMetaStore.findByExecution(execution).orElseThrow();
        return  pluginDefaultService.injectDefaults(flow, execution);
    }

    private Executor handleFailedExecutionFromExecutor(Executor executor, Exception e) {
        Execution.FailedExecutionWithLog failedExecutionWithLog = executor.getExecution().failedExecutionFromExecutor(e);

        try {
            logQueue.emitAsync(failedExecutionWithLog.getLogs());
        } catch (QueueException ex) {
            // fail silently
        }

        return executor.withExecution(failedExecutionWithLog.getExecution(), "exception");
    }


    @Override
    public String getId() {
        return id;
    }

    @Override
    public ServiceType getType() {
        return ServiceType.EXECUTOR;
    }

    @Override
    public ServiceState getState() {
        return state.get();
    }

    private void setState(final ServiceState state) {
        this.state.set(state);
        eventPublisher.publishEvent(new ServiceStateChangeEvent(this));
    }
}

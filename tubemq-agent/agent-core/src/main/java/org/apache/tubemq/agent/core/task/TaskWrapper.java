/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.core.task;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tubemq.agent.common.AgentThreadFactory;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.core.AgentManager;
import org.apache.tubemq.agent.message.EndMessage;
import org.apache.tubemq.agent.plugin.Message;
import org.apache.tubemq.agent.state.AbstractStateWrapper;
import org.apache.tubemq.agent.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskWrapper is used in taskManager, it maintains the life cycle of
 * running task.
 */
public class TaskWrapper extends AbstractStateWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskWrapper.class);

    private final TaskManager taskManager;
    private final Task task;

    private final AtomicInteger retryTime = new AtomicInteger(0);
    private final int maxRetryTime;
    private final int pushMaxWaitTime;
    private final int pullMaxWaitTime;
    private ExecutorService executorService;

    public TaskWrapper(AgentManager manager, Task task) {
        super();
        this.taskManager = manager.getTaskManager();
        this.task = task;
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        maxRetryTime = conf.getInt(
            AgentConstants.TASK_MAX_RETRY_TIME, AgentConstants.DEFAULT_TASK_MAX_RETRY_TIME);
        pushMaxWaitTime = conf.getInt(
            AgentConstants.TASK_PUSH_MAX_SECOND, AgentConstants.DEFAULT_TASK_PUSH_MAX_SECOND);
        pullMaxWaitTime = conf.getInt(
            AgentConstants.TASK_PULL_MAX_SECOND, AgentConstants.DEFAULT_TASK_PULL_MAX_SECOND);
        if (executorService == null) {
            executorService = Executors.newCachedThreadPool(
                    new AgentThreadFactory("task-reader-writer"));
        }
        doChangeState(State.ACCEPTED);
    }

    /**
     * submit read thread
     *
     * @return CompletableFuture
     */
    private CompletableFuture<?> submitReadThread() {
        return CompletableFuture.runAsync(() -> {
            Message message = null;
            while (!task.isReadFinished() && !isException()) {
                if (message == null || task.getChannel()
                        .push(message, pushMaxWaitTime, TimeUnit.SECONDS)) {
                    message = task.getReader().read();
                }
            }
            // write end message
            task.getChannel().push(new EndMessage());
        }, executorService);
    }

    /**
     * submit write thread
     *
     * @return CompletableFuture
     */
    private CompletableFuture<?> submitWriteThread() {
        return CompletableFuture.runAsync(() -> {
            while (!isException()) {
                Message message = task.getChannel().pull(pullMaxWaitTime, TimeUnit.SECONDS);
                if (message instanceof EndMessage) {
                    break;
                }
                task.getSink().write(message);
            }
        }, executorService);
    }

    /**
     * submit reader/writer
     */
    private void submitThreadsAndWait() {
        task.init();
        CompletableFuture<?> reader = submitReadThread();
        CompletableFuture<?> writer = submitWriteThread();
        CompletableFuture.anyOf(reader, writer)
                .exceptionally(ex -> {
                    doChangeState(State.FAILED);
                    LOGGER.error("exception caught", ex);
                    return null;
                }).join();
    }

    /**
     * kill task
     */
    void kill() {
        doChangeState(State.KILLED);
    }

    /**
     * whether task retry times exceed max retry time.
     *
     * @return - whether should retry
     */
    boolean shouldRetry() {
        return retryTime.get() < maxRetryTime;
    }

    Task getTask() {
        return task;
    }

    @Override
    public void addCallbacks() {
        this.addCallback(State.ACCEPTED, State.RUNNING, (before, after) -> {
            taskManager.getTaskMetrics().runningTasks.incr();
        }).addCallback(State.RUNNING, State.FAILED, (before, after) -> {
            retryTime.incrementAndGet();
            if (!shouldRetry()) {
                doChangeState(State.FATAL);
                taskManager.getTaskMetrics().fatalTasks.incr();
            }
        }).addCallback(State.FAILED, State.FATAL, (before, after) -> {

        }).addCallback(State.FAILED, State.ACCEPTED, (before, after) -> {
            retryTime.incrementAndGet();
        }).addCallback(State.RUNNING, State.SUCCEEDED, (before, after) -> {
            taskManager.getTaskMetrics().runningTasks.decr();
        });
    }


    @Override
    public void run() {
        try {
            LOGGER.info("start to run {}", task.getTaskId());
            task.init();
            doChangeState(State.RUNNING);
            submitThreadsAndWait();
            if (!isException()) {
                doChangeState(State.SUCCEEDED);
            }
            task.destroy();
        } catch (Exception ex) {
            LOGGER.error("error while running wrapper", ex);
            doChangeState(State.FAILED);
        }
    }


}

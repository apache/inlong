/*
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.tubemq.agent.common.AbstractDaemon;
import org.apache.tubemq.agent.common.AgentThreadFactory;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.core.AgentManager;
import org.apache.tubemq.agent.core.job.JobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task manager maintains lots of tasks and communicate with job level components.
 * It also provide functions to execute commands from job level like killing/submit tasks.
 */
public class TaskManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    // task thread pool;
    private final ThreadPoolExecutor runningPool;
    private final AgentManager agentManager;
    private final TaskMetrics taskMetrics;
    private final ConcurrentHashMap<String, TaskWrapper> tasks;
    private final BlockingQueue<TaskWrapper> retryTasks;
    private final int monitorInterval;
    private final int taskMaxCapacity;
    private final int taskRetryMaxTime;
    private final long waitTime;

    /**
     * Init task manager.
     *
     * @param agentManager - agent manager
     */
    public TaskManager(AgentManager agentManager) {
        this.agentManager = agentManager;
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        // control running tasks by setting pool size.
        this.runningPool = new ThreadPoolExecutor(
                conf.getInt(
                    AgentConstants.TASK_RUNNING_THREAD_CORE_SIZE, AgentConstants.DEFAULT_TASK_RUNNING_THREAD_CORE_SIZE),
                conf.getInt(
                    AgentConstants.TASK_RUNNING_THREAD_MAX_SIZE, AgentConstants.DEFAULT_TASK_RUNNING_THREAD_MAX_SIZE),
                conf.getLong(AgentConstants.TASK_RUNNING_THREAD_KEEP_ALIVE,
                        AgentConstants.DEFAULT_TASK_RUNNING_THREAD_KEEP_ALIVE),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(
                        conf.getInt(
                            AgentConstants.TASK_PENDING_MAX, AgentConstants.DEFAULT_TASK_PENDING_MAX)),
                new AgentThreadFactory("task"));
        // metric for task level
        taskMetrics = TaskMetrics.getMetrics();
        tasks = new ConcurrentHashMap<>();
        retryTasks = new LinkedBlockingQueue<>(
                conf.getInt(
                    AgentConstants.TASK_RETRY_MAX_CAPACITY, AgentConstants.DEFAULT_TASK_RETRY_MAX_CAPACITY));
        monitorInterval = conf.getInt(
            AgentConstants.TASK_MONITOR_INTERVAL, AgentConstants.DEFAULT_TASK_MONITOR_INTERVAL);
        taskRetryMaxTime = conf
                .getInt(AgentConstants.TASK_RETRY_SUBMIT_WAIT_SECONDS,
                    AgentConstants.DEFAULT_TASK_RETRY_SUBMIT_WAIT_SECONDS);
        taskMaxCapacity = conf.getInt(
            AgentConstants.TASK_RETRY_MAX_CAPACITY, AgentConstants.DEFAULT_TASK_RETRY_MAX_CAPACITY);
        waitTime = conf.getLong(
            AgentConstants.THREAD_POOL_AWAIT_TIME, AgentConstants.DEFAULT_THREAD_POOL_AWAIT_TIME);
    }

    /**
     * Get task metrics
     *
     * @return task metrics
     */
    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    public TaskWrapper getTaskWrapper(String taskId) {
        return tasks.get(taskId);
    }

    /**
     * submit task, wait if task queue is full.
     *
     * @param task - task
     */
    public void submitTask(Task task) {
        try {
            TaskWrapper taskWrapper = new TaskWrapper(agentManager, task);
            taskWrapper =
                    tasks.putIfAbsent(task.getTaskId(), taskWrapper) == null ? taskWrapper : null;
            if (taskWrapper != null) {
                this.runningPool.submit(taskWrapper).get();
            }
        } catch (Exception ex) {
            LOGGER.warn("reject task {}", task.getTaskId(), ex);
        }
    }

    /**
     * retry task.
     *
     * @param wrapper - task wrapper
     */
    void retryTask(TaskWrapper wrapper) {
        LOGGER.info("retry submit task {}", wrapper.getTask().getTaskId());
        try {
            boolean success = retryTasks.offer(wrapper, taskRetryMaxTime, TimeUnit.SECONDS);
            if (!success) {
                LOGGER.error("cannot submit to retry queue, max {}, current {}", taskMaxCapacity,
                        retryTasks.size());
            }
        } catch (Exception ex) {
            LOGGER.error("error while offer task", ex);
        }
    }

    /**
     * Check whether task is finished
     *
     * @param taskId - task id
     * @return - true if task is finished otherwise false
     */
    public boolean isTaskFinished(String taskId) {
        TaskWrapper wrapper = tasks.get(taskId);
        if (wrapper != null) {
            return wrapper.isFinished();
        }
        return false;
    }

    /**
     * Check if task is success
     *
     * @param taskId task id
     * @return true if task is success otherwise false
     */
    public boolean isTaskSuccess(String taskId) {
        TaskWrapper wrapper = tasks.get(taskId);
        if (wrapper != null) {
            return wrapper.isSuccess();
        }
        return false;
    }

    /**
     * Remove task by task id
     *
     * @param taskId - task id
     */
    public void removeTask(String taskId) {
        tasks.remove(taskId);
    }

    /**
     * kill task
     *
     * @param task task
     * @return
     */
    public boolean killTask(Task task) {
        // kill running tasks.
        TaskWrapper taskWrapper = tasks.get(task.getTaskId());
        if (taskWrapper != null) {
            taskWrapper.kill();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Thread for checking whether task should retry.
     *
     * @return - runnable thread
     */
    public Runnable createTaskMonitorThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    for (String taskId : tasks.keySet()) {
                        TaskWrapper wrapper = tasks.get(taskId);
                        if (wrapper != null && wrapper.isFailed() && wrapper.shouldRetry()) {
                            retryTask(wrapper);
                        }
                    }
                    TimeUnit.SECONDS.sleep(monitorInterval);
                } catch (Exception ex) {
                    LOGGER.error("Exception caught", ex);
                }
            }
        };
    }

    /**
     * start service.
     */
    @Override
    public void start() {
        submitWorker(createTaskMonitorThread());
    }

    /**
     * stop service.
     */
    @Override
    public void stop() throws Exception {
        waitForTerminate();
        this.runningPool.shutdown();
    }
}

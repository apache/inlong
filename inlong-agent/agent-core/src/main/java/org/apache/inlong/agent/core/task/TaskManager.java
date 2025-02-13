/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.core.task;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.file.Task;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.store.TaskStore;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_JOB_NUMBER_LIMIT;
import static org.apache.inlong.agent.constant.AgentConstants.JOB_NUMBER_LIMIT;
import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_STATE;

/**
 * handle the task config from manager, including add, delete, update etc.
 * the task config is store in both task store and memory.
 */
public class TaskManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);
    public static final int CONFIG_QUEUE_CAPACITY = 1;
    public static final int CORE_THREAD_SLEEP_TIME = 1000;
    public static final int CORE_THREAD_PRINT_TIME = 10000;
    private static final int ACTION_QUEUE_CAPACITY = 1000;
    private long lastPrintTime = 0;
    // task basic store
    private final Store taskBasicStore;
    // instance basic store
    private final Store instanceBasicStore;
    // offset basic store
    private final Store offsetBasicStore;
    // task in task store
    private final TaskStore taskStore;
    // task in memory
    private final ConcurrentHashMap<String, Task> taskMap;
    // task config from manager.
    private final BlockingQueue<List<TaskProfile>> configQueue;
    // task thread pool;
    private final ThreadPoolExecutor runningPool;
    // tasks which are not accepted by running pool.
    private final BlockingQueue<Task> pendingTasks;
    private final int taskMaxLimit;
    private static final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    // instance profile queue.
    private final BlockingQueue<TaskAction> actionQueue;
    private String taskResultMd5;
    private Integer taskResultVersion = -1;

    private class TaskPrintStat {

        public int newCount = 0;
        public int runningCont = 0;
        public int frozenCount = 0;
        public int finishedCount = 0;
        public int otherCount = 0;

        private void stat(TaskStateEnum state) {
            switch (state) {
                case NEW: {
                    newCount++;
                    break;
                }
                case RUNNING: {
                    runningCont++;
                    break;
                }
                case FROZEN: {
                    frozenCount++;
                    break;
                }
                case RETRY_FINISH: {
                    finishedCount++;
                    break;
                }
                default: {
                    otherCount++;
                }
            }
        }

        @Override
        public String toString() {
            return String.format("new %d running %d frozen %d finished %d other %d", newCount, runningCont, frozenCount,
                    finishedCount, otherCount);
        }
    }

    /**
     * Init task manager.
     */
    public TaskManager() {
        taskBasicStore = initStore(
                agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.AGENT_STORE_PATH_TASK));
        taskStore = new TaskStore(taskBasicStore);
        instanceBasicStore = initStore(
                agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.AGENT_STORE_PATH_INSTANCE));
        offsetBasicStore =
                initStore(agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.AGENT_STORE_PATH_OFFSET));
        OffsetManager.init(taskBasicStore, instanceBasicStore, offsetBasicStore);
        this.runningPool = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new AgentThreadFactory("task-manager-running-pool"));
        taskMap = new ConcurrentHashMap<>();
        taskMaxLimit = agentConf.getInt(JOB_NUMBER_LIMIT, DEFAULT_JOB_NUMBER_LIMIT);
        pendingTasks = new LinkedBlockingQueue<>(taskMaxLimit);
        configQueue = new LinkedBlockingQueue<>(CONFIG_QUEUE_CAPACITY);
        actionQueue = new LinkedBlockingQueue<>(ACTION_QUEUE_CAPACITY);
    }

    public TaskStore getTaskStore() {
        return taskStore;
    }

    public Store getInstanceBasicStore() {
        return instanceBasicStore;
    }

    /**
     * init store by class name
     *
     * @return store
     */
    public static Store initStore(String childPath) {
        try {
            Constructor<?> constructor =
                    Class.forName(agentConf.get(AgentConstants.AGENT_STORE_CLASSNAME,
                            AgentConstants.DEFAULT_AGENT_STORE_CLASSNAME)).getDeclaredConstructor(String.class);
            constructor.setAccessible(true);
            return (Store) constructor.newInstance(childPath);
        } catch (Exception ex) {
            throw new UnsupportedClassVersionError(ex.getMessage());
        }
    }

    public void submitTaskProfiles(List<TaskProfile> taskProfiles) {
        if (taskProfiles == null) {
            return;
        }
        while (configQueue.size() != 0) {
            configQueue.poll();
        }
        for (int i = 0; i < taskProfiles.size(); i++) {
            LOGGER.info("submitTaskProfiles index {} total {} {}", i, taskProfiles.size(),
                    taskProfiles.get(i).toJsonStr());
        }
        configQueue.add(taskProfiles);
    }

    public boolean submitAction(TaskAction action) {
        if (action == null) {
            return false;
        }
        return actionQueue.offer(action);
    }

    /**
     * thread for core thread.
     *
     * @return runnable profile.
     */
    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("task-manager-core");
            while (isRunnable()) {
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                    printTaskDetail();
                    dealWithConfigQueue(configQueue);
                    dealWithActionQueue(actionQueue);
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TASK_MGR_HEARTBEAT, "", "",
                            AgentUtils.getCurrentTime(), 1, 1);
                } catch (Throwable ex) {
                    LOGGER.error("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private void printTaskDetail() {
        if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_TIME) {
            List<TaskProfile> tasksInStore = taskStore.getTasks();
            TaskPrintStat stat = new TaskPrintStat();
            for (int i = 0; i < tasksInStore.size(); i++) {
                TaskProfile task = tasksInStore.get(i);
                stat.stat(task.getState());
            }
            LOGGER.info("taskManager running! mem {} total {} {} ", taskMap.size(), tasksInStore.size(), stat);
            lastPrintTime = AgentUtils.getCurrentTime();
        }
    }

    private void dealWithConfigQueue(BlockingQueue<List<TaskProfile>> queue) {
        List<TaskProfile> dataConfigs = queue.poll();
        if (dataConfigs == null) {
            return;
        }
        keepPaceWithManager(dataConfigs);
        keepPaceWithStore();
    }

    private void dealWithActionQueue(BlockingQueue<TaskAction> queue) {
        while (isRunnable()) {
            try {
                TaskAction action = queue.poll();
                if (action == null) {
                    break;
                }
                TaskProfile profile = action.getProfile();
                switch (action.getActionType()) {
                    case FINISH:
                        LOGGER.info("deal finish action, taskId {}", profile.getTaskId());
                        finishTask(profile);
                        break;
                    default:
                        LOGGER.error("invalid action type for action queue: taskId {} type {}", profile.getTaskId(),
                                action.getActionType());
                }
            } catch (Throwable ex) {
                LOGGER.error("dealWithActionQueue", ex);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            }
        }
    }

    /**
     * keep pace with data config from manager, task state should be only RUNNING or FROZEN.
     * NEW and STOP only used in manager
     */
    private void keepPaceWithManager(List<TaskProfile> taskProfiles) {
        Map<String, TaskProfile> tasksFromManager = new ConcurrentHashMap<>();
        taskProfiles.forEach((profile) -> {
            TaskStateEnum state = profile.getState();
            if (state == TaskStateEnum.RUNNING || state == TaskStateEnum.FROZEN) {
                tasksFromManager.put(profile.getTaskId(), profile);
            } else {
                LOGGER.error("task {} invalid task state {}", profile, state);
            }
        });
        traverseManagerTasksToStore(tasksFromManager);
        traverseStoreTasksToManager(tasksFromManager);
    }

    /**
     * keep pace with task in task store
     */
    private void keepPaceWithStore() {
        traverseStoreTasksToMemory();
        traverseMemoryTasksToStore();
    }

    /**
     * keep pace with task in task store
     */
    private void traverseManagerTasksToStore(Map<String, TaskProfile> tasksFromManager) {
        tasksFromManager.values().forEach((profileFromManager) -> {
            TaskProfile taskFromStore = taskStore.getTask(profileFromManager.getTaskId());
            if (taskFromStore == null) {
                LOGGER.info("traverseManagerTasksToStore task {} not found in task store retry {} state {}, add it",
                        profileFromManager.getTaskId(),
                        profileFromManager.isRetry(), profileFromManager.getState());
                addTask(profileFromManager);
            } else {
                TaskStateEnum managerState = profileFromManager.getState();
                TaskStateEnum storeState = taskFromStore.getState();
                if (managerState == storeState) {
                    return;
                }
                if (storeState == TaskStateEnum.RETRY_FINISH) {
                    LOGGER.info("traverseManagerTasksToStore task {} storeState {} retry {}, do nothing",
                            taskFromStore.getTaskId(), storeState,
                            taskFromStore.isRetry());
                    return;
                }
                if (managerState == TaskStateEnum.RUNNING) {
                    LOGGER.info("traverseManagerTasksToStore task {} storeState {} retry {}, active it",
                            taskFromStore.getTaskId(), storeState, taskFromStore.isRetry());
                    activeTask(profileFromManager);
                } else {
                    LOGGER.info("traverseManagerTasksToStore task {} storeState {} retry {}, freeze it",
                            taskFromStore.getTaskId(), storeState, taskFromStore.isRetry());
                    freezeTask(profileFromManager);
                }
            }
        });
    }

    /**
     * traverse tasks in task store, if not found in tasks from manager then delete it
     */
    private void traverseStoreTasksToManager(Map<String, TaskProfile> tasksFromManager) {
        taskStore.getTasks().forEach((profileFromStore) -> {
            if (!tasksFromManager.containsKey(profileFromStore.getTaskId())) {
                LOGGER.info("traverseStoreTasksToManager try to delete task {}", profileFromStore.getTaskId());
                deleteTask(profileFromStore);
            }
        });
    }

    /**
     * manager task state is RUNNING and taskMap not found then add
     * manager task state is FROZE and taskMap found thrn delete
     */
    private void traverseStoreTasksToMemory() {
        taskStore.getTasks().forEach((profileFromStore) -> {
            TaskStateEnum storeState = profileFromStore.getState();
            Task task = taskMap.get(profileFromStore.getTaskId());
            if (storeState == TaskStateEnum.RUNNING) {
                if (task == null) {
                    LOGGER.info("traverseStoreTasksToMemory add task to mem taskId {}", profileFromStore.getTaskId());
                    addToMemory(profileFromStore);
                }
            } else if (storeState == TaskStateEnum.FROZEN) {
                if (task != null) {
                    LOGGER.info("traverseStoreTasksToMemory delete task from mem taskId {}",
                            profileFromStore.getTaskId());
                    deleteFromMemory(profileFromStore.getTaskId());
                }
            } else {
                if (storeState != TaskStateEnum.RETRY_FINISH) {
                    LOGGER.error("task {} invalid state {}", profileFromStore.getTaskId(), storeState);
                }
            }
        });
    }

    /**
     * task in taskMap but not in task store then delete
     * task in taskMap but task state from task store is FROZEN then delete
     */
    private void traverseMemoryTasksToStore() {
        taskMap.values().forEach((task) -> {
            TaskProfile profileFromStore = taskStore.getTask(task.getTaskId());
            if (profileFromStore == null) {
                deleteFromMemory(task.getTaskId());
                return;
            }
            TaskStateEnum stateFromStore = profileFromStore.getState();
            if (stateFromStore != TaskStateEnum.RUNNING) {
                deleteFromMemory(task.getTaskId());
            }
        });
    }

    /**
     * add task profile to task store
     * if task state is RUNNING then add task to memory
     */
    private void addTask(TaskProfile taskProfile) {
        if (taskMap.size() >= taskMaxLimit) {
            LOGGER.error("taskMap size {} over limit {}", taskMap.size(), taskMaxLimit);
            return;
        }
        if (!isProfileValid(taskProfile)) {
            LOGGER.error("task profile invalid {}", taskProfile.toJsonStr());
            return;
        }
        addToStore(taskProfile);
        TaskStateEnum state = TaskStateEnum.getTaskState(taskProfile.getInt(TASK_STATE));
        if (state == TaskStateEnum.RUNNING) {
            addToMemory(taskProfile);
        } else {
            LOGGER.info("taskId {} state {} no need to add to memory", taskProfile.getTaskId(),
                    taskProfile.getState());
        }
    }

    private void deleteTask(TaskProfile taskProfile) {
        deleteFromStore(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void freezeTask(TaskProfile taskProfile) {
        updateToStore(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void finishTask(TaskProfile taskProfile) {
        taskProfile.setState(TaskStateEnum.RETRY_FINISH);
        updateToStore(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void activeTask(TaskProfile taskProfile) {
        updateToStore(taskProfile);
        addToMemory(taskProfile);
    }

    private void restoreFromStore() {
        LOGGER.info("restore from task store start");
        List<TaskProfile> taskProfileList = taskStore.getTasks();
        taskProfileList.forEach((profile) -> {
            if (profile.getState() == TaskStateEnum.RUNNING) {
                LOGGER.info("restore from task store taskId {}", profile.getTaskId());
                profile.setBoolean(RESTORE_FROM_DB, true);
                addToMemory(profile);
            }
        });
        LOGGER.info("restore from task store end");
    }

    private void stopAllTasks() {
        taskMap.values().forEach((task) -> {
            task.destroy();
        });
        taskMap.clear();
    }

    private boolean isProfileValid(TaskProfile profile) {
        try {
            Class<?> taskClass = Class.forName(profile.getTaskClass());
            Task task = (Task) taskClass.newInstance();
            return task.isProfileValid(profile);
        } catch (Throwable t) {
            LOGGER.error("isProfileValid error: ", t);
        }
        return false;
    }

    /**
     * add task to task store, it was expected that there is no record refer the task id.
     * cause the task id will change if the task content changes, replace the record
     * if it is found, the memory record will be updated by the task store.
     */
    private void addToStore(TaskProfile taskProfile) {
        if (taskStore.getTask(taskProfile.getTaskId()) != null) {
            LOGGER.error("task {} should not exist", taskProfile.getTaskId());
        }
        taskStore.storeTask(taskProfile);
    }

    private void deleteFromStore(TaskProfile taskProfile) {
        if (taskStore.getTask(taskProfile.getTaskId()) == null) {
            LOGGER.error("try to delete task {} but not found in task store", taskProfile);
            return;
        }
        taskStore.deleteTask(taskProfile.getTaskId());
    }

    private void updateToStore(TaskProfile taskProfile) {
        if (taskStore.getTask(taskProfile.getTaskId()) == null) {
            LOGGER.error("task {} not found, agent may have been reinstalled", taskProfile);
        }
        taskStore.storeTask(taskProfile);
    }

    /**
     * add task to memory, if there is a record refer to the task id exist we need to destroy it first.
     */
    private void addToMemory(TaskProfile taskProfile) {
        Task oldTask = taskMap.get(taskProfile.getTaskId());
        if (oldTask != null) {
            oldTask.destroy();
            taskMap.remove(taskProfile.getTaskId());
            LOGGER.error("old task {} should not exist, try stop it first",
                    taskProfile.getTaskId());
        }
        try {
            Class<?> taskClass = Class.forName(taskProfile.getTaskClass());
            Task task = (Task) taskClass.newInstance();
            task.init(this, taskProfile, instanceBasicStore);
            taskMap.put(taskProfile.getTaskId(), task);
            runningPool.submit(task);
            LOGGER.info(
                    "add task {} into memory, taskMap size {}, runningPool task total {}, runningPool task active {}",
                    task.getTaskId(), taskMap.size(), runningPool.getTaskCount(),
                    runningPool.getActiveCount());
        } catch (Throwable t) {
            LOGGER.error("add task error: ", t);
        }
    }

    private void deleteFromMemory(String taskId) {
        Task oldTask = taskMap.get(taskId);
        if (oldTask == null) {
            LOGGER.error("old task {} not found", taskId);
            return;
        }
        oldTask.destroy();
        taskMap.remove(oldTask.getTaskId());
        LOGGER.info(
                "delete task {} from memory, taskMap size {}, runningPool task total {}, runningPool task active {}",
                oldTask.getTaskId(), taskMap.size(), runningPool.getTaskCount(),
                runningPool.getActiveCount());
    }

    public Task getTask(String taskId) {
        return taskMap.get(taskId);
    }

    public int getInstanceNum() {
        int num = 0;
        for (Task task : taskMap.values()) {
            num += task.getInstanceNum();
        }
        return num;
    }

    public TaskProfile getTaskProfile(String taskId) {
        return taskStore.getTask(taskId);
    }

    public String getTaskResultMd5() {
        return taskResultMd5;
    }

    public void setTaskResultMd5(String taskResultMd5) {
        this.taskResultMd5 = taskResultMd5;
    }

    public Integer getTaskResultVersion() {
        return taskResultVersion;
    }

    public void setTaskResultVersion(Integer taskResultVersion) {
        this.taskResultVersion = taskResultVersion;
    }

    @Override
    public void start() throws Exception {
        restoreFromStore();
        submitWorker(coreThread());
        OffsetManager.getInstance().start();
    }

    @Override
    public void stop() throws Exception {
        stopAllTasks();
        waitForTerminate();
        runningPool.shutdown();
    }
}

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

package org.apache.inlong.agent.core.task.file;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.core.task.TaskAction;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.db.RocksDbImp;
import org.apache.inlong.agent.db.TaskProfileDb;
import org.apache.inlong.agent.plugin.file.Task;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static org.apache.inlong.agent.constant.TaskConstants.TASK_STATE;

/**
 * handle the task config from manager, including add, delete, update etc.
 * the task config is store in both db and memory.
 */
public class TaskManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);
    public static final int CONFIG_QUEUE_CAPACITY = 1;
    public static final int CORE_THREAD_SLEEP_TIME = 1000;
    private static final int ACTION_QUEUE_CAPACITY = 100000;
    // task basic db
    private final Db taskBasicDb;
    // instance basic db
    private final Db instanceBasicDb;
    // task in db
    private final TaskProfileDb taskDb;
    // task in memory
    private final ConcurrentHashMap<String, Task> taskMap;
    // task config from manager.
    private final BlockingQueue<List<TaskProfile>> configQueue;
    // task thread pool;
    private final ThreadPoolExecutor runningPool;
    // tasks which are not accepted by running pool.
    private final BlockingQueue<Task> pendingTasks;
    private final int taskMaxLimit;
    private final AgentConfiguration agentConf;
    // instance profile queue.
    private final BlockingQueue<TaskAction> actionQueue;

    /**
     * Init task manager.
     */
    public TaskManager() {
        this.agentConf = AgentConfiguration.getAgentConf();
        this.taskBasicDb = initDb(
                agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.AGENT_LOCAL_DB_PATH_TASK));
        this.instanceBasicDb = initDb(
                agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.AGENT_LOCAL_DB_PATH_INSTANCE));
        taskDb = new TaskProfileDb(taskBasicDb);
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

    /**
     * init db by class name
     *
     * @return db
     */
    public static Db initDb(String childPath) {
        try {
            return new RocksDbImp(childPath);
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
                    dealWithConfigQueue(configQueue);
                    dealWithActionQueue(actionQueue);
                } catch (Throwable ex) {
                    LOGGER.error("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private void dealWithConfigQueue(BlockingQueue<List<TaskProfile>> queue) {
        List<TaskProfile> dataConfigs = queue.poll();
        if (dataConfigs == null) {
            return;
        }
        keepPaceWithManager(dataConfigs);
        keepPaceWithDb();
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
        traverseManagerTasksToDb(tasksFromManager);
        traverseDbTasksToManager(tasksFromManager);
    }

    /**
     * keep pace with task in db
     */
    private void keepPaceWithDb() {
        traverseDbTasksToMemory();
        traverseMemoryTasksToDb();
    }

    /**
     * keep pace with task in db
     */
    private void traverseManagerTasksToDb(Map<String, TaskProfile> tasksFromManager) {
        tasksFromManager.values().forEach((profileFromManager) -> {
            TaskProfile taskFromDb = taskDb.getTask(profileFromManager.getTaskId());
            if (taskFromDb == null) {
                LOGGER.info("traverseManagerTasksToDb task {} not found in db retry {} state {}, add it",
                        profileFromManager.getTaskId(),
                        profileFromManager.isRetry(), profileFromManager.getState());
                addTask(profileFromManager);
            } else {
                TaskStateEnum managerState = profileFromManager.getState();
                TaskStateEnum dbState = taskFromDb.getState();
                if (managerState == dbState) {
                    return;
                }
                if (dbState == TaskStateEnum.FINISH) {
                    LOGGER.info("traverseManagerTasksToDb task {} dbState {} retry {}, do nothing",
                            taskFromDb.getTaskId(), dbState,
                            taskFromDb.isRetry());
                    return;
                }
                if (managerState == TaskStateEnum.RUNNING) {
                    LOGGER.info("traverseManagerTasksToDb task {} dbState {} retry {}, active it",
                            taskFromDb.getTaskId(), dbState, taskFromDb.isRetry());
                    activeTask(profileFromManager);
                } else {
                    LOGGER.info("traverseManagerTasksToDb task {} dbState {} retry {}, freeze it",
                            taskFromDb.getTaskId(), dbState, taskFromDb.isRetry());
                    freezeTask(profileFromManager);
                }
            }
        });
    }

    /**
     * traverse tasks in db, if not found in tasks from manager then delete it
     */
    private void traverseDbTasksToManager(Map<String, TaskProfile> tasksFromManager) {
        taskDb.getTasks().forEach((profileFromDb) -> {
            if (!tasksFromManager.containsKey(profileFromDb.getTaskId())) {
                LOGGER.info("traverseDbTasksToManager try to delete task {}", profileFromDb.getTaskId());
                deleteTask(profileFromDb);
            }
        });
    }

    /**
     * manager task state is RUNNING and taskMap not found then add
     * manager task state is FROZE and taskMap found thrn delete
     */
    private void traverseDbTasksToMemory() {
        taskDb.getTasks().forEach((profileFromDb) -> {
            TaskStateEnum dbState = profileFromDb.getState();
            Task task = taskMap.get(profileFromDb.getTaskId());
            if (dbState == TaskStateEnum.RUNNING) {
                if (task == null) {
                    LOGGER.info("traverseDbTasksToMemory add task to mem taskId {}", profileFromDb.getTaskId());
                    addToMemory(profileFromDb);
                }
            } else if (dbState == TaskStateEnum.FROZEN) {
                if (task != null) {
                    LOGGER.info("traverseDbTasksToMemory delete task from mem taskId {}",
                            profileFromDb.getTaskId());
                    deleteFromMemory(profileFromDb.getTaskId());
                }
            } else {
                if (dbState != TaskStateEnum.FINISH) {
                    LOGGER.error("task {} invalid state {}", profileFromDb.getTaskId(), dbState);
                }
            }
        });
    }

    /**
     * task in taskMap but not in taskDb then delete
     * task in taskMap but task state from db is FROZEN then delete
     */
    private void traverseMemoryTasksToDb() {
        taskMap.values().forEach((task) -> {
            TaskProfile profileFromDb = taskDb.getTask(task.getTaskId());
            if (profileFromDb == null) {
                deleteFromMemory(task.getTaskId());
                return;
            }
            TaskStateEnum stateFromDb = profileFromDb.getState();
            if (stateFromDb != TaskStateEnum.RUNNING) {
                deleteFromMemory(task.getTaskId());
            }
        });
    }

    /**
     * add task profile to db
     * if task state is RUNNING then add task to memory
     */
    private void addTask(TaskProfile taskProfile) {
        if (taskMap.size() >= taskMaxLimit) {
            LOGGER.error("taskMap size {} over limit {}", taskMap.size(), taskMaxLimit);
            return;
        }
        addToDb(taskProfile);
        TaskStateEnum state = TaskStateEnum.getTaskState(taskProfile.getInt(TASK_STATE));
        if (state == TaskStateEnum.RUNNING) {
            addToMemory(taskProfile);
        } else {
            LOGGER.info("taskId {} state {} no need to add to memory", taskProfile.getTaskId(),
                    taskProfile.getState());
        }
    }

    private void deleteTask(TaskProfile taskProfile) {
        deleteFromDb(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void freezeTask(TaskProfile taskProfile) {
        updateToDb(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void finishTask(TaskProfile taskProfile) {
        taskProfile.setState(TaskStateEnum.FINISH);
        updateToDb(taskProfile);
        deleteFromMemory(taskProfile.getTaskId());
    }

    private void activeTask(TaskProfile taskProfile) {
        updateToDb(taskProfile);
        addToMemory(taskProfile);
    }

    private void restoreFromDb() {
        List<TaskProfile> taskProfileList = taskDb.getTasks();
        taskProfileList.forEach((profile) -> {
            if (profile.getState() == TaskStateEnum.RUNNING) {
                LOGGER.info("restoreFromDb taskId {}", profile.getTaskId());
                addToMemory(profile);
            }
        });
    }

    private void stopAllTasks() {
        taskMap.values().forEach((task) -> {
            task.destroy();
        });
        taskMap.clear();
    }

    /**
     * add task to db, it was expected that there is no record refer the task id.
     * cause the task id will change if the task content changes, replace the record
     * if it is found, the memory record will be updated by the db.
     */
    private void addToDb(TaskProfile taskProfile) {
        if (taskDb.getTask(taskProfile.getTaskId()) != null) {
            LOGGER.error("task {} should not exist", taskProfile.getTaskId());
        }
        taskDb.storeTask(taskProfile);
    }

    private void deleteFromDb(TaskProfile taskProfile) {
        if (taskDb.getTask(taskProfile.getTaskId()) == null) {
            LOGGER.error("try to delete task {} but not found in db", taskProfile);
            return;
        }
        taskDb.deleteTask(taskProfile.getTaskId());
    }

    private void updateToDb(TaskProfile taskProfile) {
        if (taskDb.getTask(taskProfile.getTaskId()) == null) {
            LOGGER.error("task {} not found, agent may have been reinstalled", taskProfile);
        }
        taskDb.storeTask(taskProfile);
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
            task.init(this, taskProfile, instanceBasicDb);
            taskMap.put(taskProfile.getTaskId(), task);
            runningPool.submit(task);
            LOGGER.info(
                    "add task {} into memory, taskMap size {}, runningPool task total {}, runningPool task active {}",
                    task.getTaskId(), taskMap.size(), runningPool.getTaskCount(),
                    runningPool.getActiveCount());
        } catch (Throwable t) {
            LOGGER.error("add task error {}", t.getMessage());
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

    public TaskProfile getTaskProfile(String taskId) {
        return taskDb.getTask(taskId);
    }

    @Override
    public void start() throws Exception {
        restoreFromDb();
        submitWorker(coreThread());
    }

    @Override
    public void stop() throws Exception {
        stopAllTasks();
        waitForTerminate();
        runningPool.shutdown();
    }
}

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

package org.apache.inlong.agent.core.instance;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Instance;
import org.apache.inlong.agent.store.InstanceStore;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.store.TaskStore;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_AUDIT_VERSION;

/**
 * handle the instance created by task, including add, delete, update etc.
 * the instance info is saved in both store and memory.
 */
public class InstanceManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceManager.class);
    private static final int ACTION_QUEUE_CAPACITY = 100;
    public volatile int CORE_THREAD_SLEEP_TIME_MS = 1000;
    public static final int INSTANCE_PRINT_INTERVAL_MS = 10000;
    public static final long INSTANCE_KEEP_ALIVE_MS = 5 * 60 * 1000;
    public static final long KEEP_PACE_INTERVAL_MS = 60 * 1000;
    private long lastPrintTime = 0;
    private long lastTraverseTime = 0;
    // instance in instance store
    private final InstanceStore instanceStore;
    private TaskStore taskStore;
    private TaskProfile taskFromStore;
    // task in memory
    private final ConcurrentHashMap<String, Instance> instanceMap;
    // instance profile queue.
    private final BlockingQueue<InstanceAction> actionQueue;
    private final BlockingQueue<InstanceAction> addActionQueue;
    // task thread pool;
    private final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("instance-manager"));

    private final int instanceLimit;
    private final AgentConfiguration agentConf;
    private final String taskId;
    private long auditVersion;
    private volatile boolean running = false;
    private final double reserveCoefficient = 0.8;

    private class InstancePrintStat {

        public int defaultCount = 0;
        public int finishedCount = 0;
        public int deleteCount = 0;
        public int otherCount = 0;

        private void stat(InstanceStateEnum state) {
            switch (state) {
                case DEFAULT: {
                    defaultCount++;
                    break;
                }
                case FINISHED: {
                    finishedCount++;
                    break;
                }
                case DELETE: {
                    deleteCount++;
                }
                default: {
                    otherCount++;
                }
            }
        }

        @Override
        public String toString() {
            return String.format("default %d finished %d delete %d other %d", defaultCount, finishedCount,
                    deleteCount, otherCount);
        }
    }

    /**
     * Init task manager.
     */
    public InstanceManager(String taskId, int instanceLimit, Store basicStore, TaskStore taskStore) {
        this.taskId = taskId;
        instanceStore = new InstanceStore(basicStore);
        this.taskStore = taskStore;
        this.agentConf = AgentConfiguration.getAgentConf();
        instanceMap = new ConcurrentHashMap<>();
        this.instanceLimit = instanceLimit;
        actionQueue = new LinkedBlockingQueue<>(ACTION_QUEUE_CAPACITY);
        addActionQueue = new LinkedBlockingQueue<>(ACTION_QUEUE_CAPACITY);
    }

    public String getTaskId() {
        return taskId;
    }

    public InstanceStore getInstanceStore() {
        return instanceStore;
    }

    public Instance getInstance(String instanceId) {
        return instanceMap.get(instanceId);
    }

    public InstanceProfile getInstanceProfile(String instanceId) {
        return instanceStore.getInstance(taskId, instanceId);
    }

    public boolean submitAction(InstanceAction action) {
        if (action == null) {
            return false;
        }
        if (isFull()) {
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
            Thread.currentThread().setName("instance-manager-core-" + taskId);
            running = true;
            while (isRunnable()) {
                long currentTime = AgentUtils.getCurrentTime();
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME_MS);
                    printInstanceState();
                    dealWithActionQueue();
                    dealWithAddActionQueue();
                    if (currentTime - lastTraverseTime > KEEP_PACE_INTERVAL_MS) {
                        keepPaceWithStore();
                        lastTraverseTime = currentTime;
                    }
                    String inlongGroupId = taskFromStore.getInlongGroupId();
                    String inlongStreamId = taskFromStore.getInlongStreamId();
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_INSTANCE_MGR_HEARTBEAT, inlongGroupId, inlongStreamId,
                            AgentUtils.getCurrentTime(), 1, 1, auditVersion);
                } catch (Throwable ex) {
                    LOGGER.error("coreThread error: ", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
            running = false;
        };
    }

    private void printInstanceState() {
        long currentTime = AgentUtils.getCurrentTime();
        if (currentTime - lastPrintTime > INSTANCE_PRINT_INTERVAL_MS) {
            List<InstanceProfile> instances = instanceStore.getInstances(taskId);
            InstancePrintStat stat = new InstancePrintStat();
            for (int i = 0; i < instances.size(); i++) {
                InstanceProfile instance = instances.get(i);
                stat.stat(instance.getState());
            }
            LOGGER.info(
                    "instanceManager running! taskId {} mem {} total {} {} action count {}",
                    taskId, instanceMap.size(), instances.size(), stat, actionQueue.size());
            lastPrintTime = currentTime;
        }
    }

    private void keepPaceWithStore() {
        traverseStoreTasksToMemory();
        traverseMemoryTasksToStore();
    }

    private void traverseStoreTasksToMemory() {
        instanceStore.getInstances(taskId).forEach((profileFromStore) -> {
            InstanceStateEnum storeState = profileFromStore.getState();
            Instance instance = instanceMap.get(profileFromStore.getInstanceId());
            switch (storeState) {
                case DEFAULT: {
                    if (instance == null) {
                        LOGGER.info("traverseStoreTasksToMemory add instance to mem taskId {} instanceId {}",
                                profileFromStore.getTaskId(), profileFromStore.getInstanceId());
                        addToMemory(profileFromStore);
                    }
                    break;
                }
                case FINISHED:
                    DELETE: {
                        if (instance != null) {
                            LOGGER.info("traverseStoreTasksToMemory delete instance from mem taskId {} instanceId {}",
                                    profileFromStore.getTaskId(), profileFromStore.getInstanceId());
                            deleteFromMemory(profileFromStore.getInstanceId());
                        }
                        break;
                    }
                default: {
                    LOGGER.error("instance invalid state {} taskId {} instanceId {}", storeState,
                            profileFromStore.getTaskId(),
                            profileFromStore.getInstanceId());
                }
            }
        });
    }

    private void traverseMemoryTasksToStore() {
        instanceMap.values().forEach((instance) -> {
            InstanceProfile profileFromStore =
                    instanceStore.getInstance(instance.getTaskId(), instance.getInstanceId());
            if (profileFromStore == null) {
                deleteFromMemory(instance.getInstanceId());
                return;
            }
            InstanceStateEnum stateFromStore = profileFromStore.getState();
            if (stateFromStore != InstanceStateEnum.DEFAULT) {
                deleteFromMemory(instance.getInstanceId());
            }
            if (AgentUtils.getCurrentTime() - instance.getLastHeartbeatTime() > INSTANCE_KEEP_ALIVE_MS) {
                LOGGER.error("instance heartbeat timeout, id: {}, will be deleted from memory",
                        instance.getInstanceId());
                deleteFromMemory(instance.getInstanceId());
            }
        });
    }

    private void dealWithActionQueue() {
        while (isRunnable()) {
            try {
                InstanceAction action = actionQueue.poll();
                if (action == null) {
                    break;
                }
                switch (action.getActionType()) {
                    case ADD:
                        if (!addActionQueue.offer(action)) {
                            LOGGER.error("it should never happen: addQueue is full");
                        }
                        break;
                    case FINISH:
                        finishInstance(action.getProfile());
                        break;
                    case DELETE:
                        deleteInstance(action.getProfile().getInstanceId());
                        break;
                    default:
                        LOGGER.error("invalid action type for instance manager: taskId {} type {}", taskId,
                                action.getActionType());
                }
            } catch (Throwable ex) {
                LOGGER.error("dealWithActionQueue", ex);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            }
        }
    }

    private void dealWithAddActionQueue() {
        while (isRunnable()) {
            if (instanceMap.size() > instanceLimit) {
                LOGGER.error("instanceMap size {} over limit {}", instanceMap.size(), instanceLimit);
                return;
            }
            InstanceAction action = addActionQueue.poll();
            if (action == null) {
                break;
            }
            addInstance(action.getProfile());
        }
    }

    @Override
    public void start() {
        restoreFromStore();
        submitWorker(coreThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
        stopAllInstances();
    }

    public void waitForTerminate() {
        super.waitForTerminate();
        while (running) {
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME_MS);
        }
    }

    private void restoreFromStore() {
        taskFromStore = taskStore.getTask(taskId);
        auditVersion = Long.parseLong(taskFromStore.get(TASK_AUDIT_VERSION));
        List<InstanceProfile> profileList = instanceStore.getInstances(taskId);
        profileList.forEach((profile) -> {
            InstanceStateEnum state = profile.getState();
            if (state == InstanceStateEnum.DEFAULT) {
                LOGGER.info("instance restoreFromStore addToMem state {} taskId {} instanceId {}", state, taskId,
                        profile.getInstanceId());
                profile.setBoolean(RESTORE_FROM_DB, true);
                addToMemory(profile);
            } else {
                LOGGER.info("instance restoreFromStore ignore state {} taskId {} instanceId {}", state, taskId,
                        profile.getInstanceId());
            }
        });
    }

    private void addInstance(InstanceProfile profile) {
        LOGGER.info("add instance taskId {} instanceId {}", taskId, profile.getInstanceId());
        if (!shouldAddAgain(profile.getInstanceId(), profile.getFileUpdateTime())) {
            LOGGER.info("addInstance shouldAddAgain returns false skip taskId {} instanceId {}", taskId,
                    profile.getInstanceId());
            return;
        }
        addToStore(profile, true);
        addToMemory(profile);
    }

    private void finishInstance(InstanceProfile profile) {
        profile.setState(InstanceStateEnum.FINISHED);
        profile.setModifyTime(AgentUtils.getCurrentTime());
        addToStore(profile, false);
        deleteFromMemory(profile.getInstanceId());
        LOGGER.info("finished instance state {} taskId {} instanceId {}", profile.getState(),
                profile.getTaskId(), profile.getInstanceId());
    }

    private void deleteInstance(String instanceId) {
        deleteFromStore(instanceId);
        deleteFromMemory(instanceId);
    }

    private void deleteFromStore(String instanceId) {
        InstanceProfile profile = instanceStore.getInstance(taskId, instanceId);
        if (profile == null) {
            LOGGER.error("try to delete instance from store but not found: taskId {} instanceId {}", taskId,
                    instanceId);
            return;
        }
        String inlongGroupId = profile.getInlongGroupId();
        String inlongStreamId = profile.getInlongStreamId();
        instanceStore.deleteInstance(taskId, instanceId);
        LOGGER.info("delete instance from instance store: taskId {} instanceId {} result {}", taskId,
                instanceId, instanceStore.getInstance(taskId, instanceId));
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_DB, inlongGroupId, inlongStreamId,
                profile.getSinkDataTime(), 1, 1, auditVersion);
    }

    private void deleteFromMemory(String instanceId) {
        Instance instance = instanceMap.get(instanceId);
        if (instance == null) {
            LOGGER.error("try to delete instance from memory but not found: taskId {} instanceId {}", taskId,
                    instanceId);
            return;
        }
        String inlongGroupId = instance.getProfile().getInlongGroupId();
        String inlongStreamId = instance.getProfile().getInlongStreamId();
        instance.destroy();
        instanceMap.remove(instanceId);
        LOGGER.info("delete instance from memory: taskId {} instanceId {}", taskId, instance.getInstanceId());
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM, inlongGroupId, inlongStreamId,
                instance.getProfile().getSinkDataTime(), 1, 1, auditVersion);
    }

    private void notifyDestroyInstance(String instanceId) {
        Instance instance = instanceMap.get(instanceId);
        if (instance == null) {
            LOGGER.error("try to notify destroy instance but not found: taskId {} instanceId {}", taskId, instanceId);
            return;
        }
        instance.notifyDestroy();
    }

    private void addToStore(InstanceProfile profile, boolean addNew) {
        LOGGER.info("add instance to instance store state {} instanceId {}", profile.getState(),
                profile.getInstanceId());
        instanceStore.storeInstance(profile);
        if (addNew) {
            String inlongGroupId = profile.getInlongGroupId();
            String inlongStreamId = profile.getInlongStreamId();
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_DB, inlongGroupId, inlongStreamId,
                    profile.getSinkDataTime(), 1, 1, auditVersion);
        }
    }

    /**
     * add instance to memory, if there is a record refer to the instance id exist we need to destroy it first.
     */
    private void addToMemory(InstanceProfile instanceProfile) {
        String inlongGroupId = instanceProfile.getInlongGroupId();
        String inlongStreamId = instanceProfile.getInlongStreamId();
        Instance oldInstance = instanceMap.get(instanceProfile.getInstanceId());
        if (oldInstance != null) {
            oldInstance.destroy();
            instanceMap.remove(instanceProfile.getInstanceId());
            LOGGER.error("old instance {} should not exist, try stop it first",
                    instanceProfile.getInstanceId());
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM_UNUSUAL, inlongGroupId, inlongStreamId,
                    instanceProfile.getSinkDataTime(), 1, 1, auditVersion);
        }
        LOGGER.info("instanceProfile {}", instanceProfile.toJsonStr());
        try {
            if (instanceMap.size() > instanceLimit) {
                LOGGER.info("add instance to memory refused because instanceMap size over limit {}",
                        instanceProfile.getInstanceId());
                return;
            }
            Class<?> taskClass = Class.forName(instanceProfile.getInstanceClass());
            Instance instance = (Instance) taskClass.newInstance();
            boolean initSuc = instance.init(this, instanceProfile);
            if (initSuc) {
                instanceMap.put(instanceProfile.getInstanceId(), instance);
                EXECUTOR_SERVICE.submit(instance);
                LOGGER.info(
                        "add instance to memory instanceId {} instanceMap size {}, runningPool instance total {}, runningPool instance active {}",
                        instance.getInstanceId(), instanceMap.size(), EXECUTOR_SERVICE.getTaskCount(),
                        EXECUTOR_SERVICE.getActiveCount());
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM, inlongGroupId, inlongStreamId,
                        instanceProfile.getSinkDataTime(), 1, 1, auditVersion);
            } else {
                LOGGER.error(
                        "add instance to memory init failed instanceId {}", instance.getInstanceId());
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM_FAILED, inlongGroupId, inlongStreamId,
                        instanceProfile.getSinkDataTime(), 1, 1, auditVersion);
            }
        } catch (Throwable t) {
            LOGGER.error("add instance error.", t);
        }
    }

    private void stopAllInstances() {
        instanceMap.values().forEach((instance) -> {
            notifyDestroyInstance(instance.getInstanceId());
        });
        instanceMap.values().forEach((instance) -> {
            deleteFromMemory(instance.getInstanceId());
        });
        instanceMap.clear();
    }

    public boolean shouldAddAgain(String fileName, long lastModifyTime) {
        InstanceProfile profileFromStore = instanceStore.getInstance(taskId, fileName);
        if (profileFromStore == null) {
            LOGGER.debug("not in instance store should add {}", fileName);
            return true;
        } else {
            InstanceStateEnum state = profileFromStore.getState();
            if (state == InstanceStateEnum.FINISHED && lastModifyTime > profileFromStore.getModifyTime()) {
                LOGGER.debug("finished but file update again {}", fileName);
                return true;
            }
            if (state == InstanceStateEnum.DELETE) {
                LOGGER.debug("delete and add again {}", fileName);
                return true;
            }
            return false;
        }
    }

    public boolean isFull() {
        return (actionQueue.size() + addActionQueue.size()) >= ACTION_QUEUE_CAPACITY * reserveCoefficient;
    }

    public long getFinishedInstanceCount() {
        int count = 0;
        List<InstanceProfile> instances = instanceStore.getInstances(taskId);
        for (int i = 0; i < instances.size(); i++) {
            if (instances.get(i).getState() == InstanceStateEnum.FINISHED) {
                count++;
            }
        }
        return count;
    }
}
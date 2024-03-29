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
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.db.InstanceDb;
import org.apache.inlong.agent.db.OffsetDb;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * used to store instance offset to db
 * where key is task id + read file name and value is instance offset
 */
public class OffsetManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);
    public static final int CORE_THREAD_SLEEP_TIME = 60 * 1000;
    private static volatile OffsetManager offsetManager = null;
    private final OffsetDb offsetDb;
    // instance in db
    private final InstanceDb instanceDb;

    private OffsetManager(Db offsetBasicDb, Db instanceBasicDb) {
        this.offsetDb = new OffsetDb(offsetBasicDb);
        instanceDb = new InstanceDb(instanceBasicDb);
    }

    /**
     * thread for core thread.
     *
     * @return runnable profile.
     */
    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("offset-manager-core");
            while (isRunnable()) {
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                    List<OffsetProfile> offsets = offsetDb.listAllOffsets();
                    offsets.forEach(offset -> {
                        String taskId = offset.getTaskId();
                        String instanceId = offset.getInstanceId();
                        InstanceProfile instanceProfile = instanceDb.getInstance(taskId, instanceId);
                        if (instanceProfile == null) {
                            deleteOffset(taskId, instanceId);
                            LOGGER.info("instance not found, delete offset taskId {} instanceId {}", taskId,
                                    instanceId);
                        }
                    });
                    LOGGER.info("offsetManager running! offsets count {}", offsets.size());
                } catch (Throwable ex) {
                    LOGGER.error("offset-manager-core: ", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    /**
     * task position manager singleton, can only generated by agent manager
     */
    public static void init(Db offsetBasicDb, Db instanceBasicDb) {
        if (offsetManager == null) {
            synchronized (OffsetManager.class) {
                if (offsetManager == null) {
                    offsetManager = new OffsetManager(offsetBasicDb, instanceBasicDb);
                }
            }
        }
    }

    /**
     * get taskPositionManager singleton
     */
    public static OffsetManager getInstance() {
        if (offsetManager == null) {
            throw new RuntimeException("task position manager has not been initialized by agentManager");
        }
        return offsetManager;
    }

    public void setOffset(OffsetProfile profile) {
        offsetDb.setOffset(profile);
    }

    public void deleteOffset(String taskId, String instanceId) {
        offsetDb.deleteOffset(taskId, instanceId);
    }

    public OffsetProfile getOffset(String taskId, String instanceId) {
        return offsetDb.getOffset(taskId, instanceId);
    }

    @Override
    public void start() throws Exception {
        submitWorker(coreThread());
    }

    @Override
    public void stop() throws Exception {

    }
}

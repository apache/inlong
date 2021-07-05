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

package org.apache.inlong.agent.db;

import static java.util.Objects.requireNonNull;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.constants.AgentConstants;
import org.apache.inlong.agent.constants.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DB implement based on berkeley db.
 */
public class BerkeleyDbImp implements Db {

    private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyDbImp.class);

    private final EntityStore jobStore;
    private final EntityStore commandStore;
    private final PrimaryIndex<String, KeyValueEntity> primaryIndex;
    private final SecondaryIndex<StateSearchKey, String, KeyValueEntity> secondaryIndex;
    private final PrimaryIndex<String, CommandEntity> commandPrimaryIndex;
    private final SecondaryIndex<String, String, KeyValueEntity> fileNameSecondaryIndex;
    private final SecondaryIndex<Boolean, String, CommandEntity> commandSecondaryIndex;

    private final AgentConfiguration agentConf;

    public BerkeleyDbImp() {
        this.agentConf = AgentConfiguration.getAgentConf();
        StoreConfig storeConfig = initStoreConfig();
        Environment environment = initEnv();
        String instanceName = agentConf.get(
            AgentConstants.AGENT_DB_INSTANCE_NAME, AgentConstants.DEFAULT_AGENT_DB_INSTANCE_NAME);
        this.jobStore = new EntityStore(environment, instanceName, storeConfig);
        this.commandStore = new EntityStore(environment, CommonConstants.COMMAND_STORE_INSTANCE_NAME, storeConfig);
        commandPrimaryIndex = this.commandStore.getPrimaryIndex(String.class, CommandEntity.class);
        commandSecondaryIndex = commandStore.getSecondaryIndex(
            commandPrimaryIndex, Boolean.class, "isAcked");
        primaryIndex = this.jobStore.getPrimaryIndex(String.class, KeyValueEntity.class);
        secondaryIndex = this.jobStore.getSecondaryIndex(primaryIndex, StateSearchKey.class,
                "stateSearchKey");
        fileNameSecondaryIndex = this.jobStore.getSecondaryIndex(primaryIndex,
            String.class, "fileName");
    }

    /**
     * init store by config
     *
     * @return store config
     */
    private StoreConfig initStoreConfig() {
        return new StoreConfig()
                .setReadOnly(agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_READONLY,
                    AgentConstants.DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setAllowCreate(!agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_READONLY,
                    AgentConstants.DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setTransactional(agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_TRANSACTIONAL,
                    AgentConstants.DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL));
    }

    /**
     * init local bdb path and get it.
     * @return local path.
     */
    private File tryToInitAndGetPath() {
        String storePath = agentConf.get(
            AgentConstants.AGENT_LOCAL_STORE_PATH, AgentConstants.DEFAULT_AGENT_LOCAL_STORE_PATH);
        String parentPath = agentConf.get(
            AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
        File finalPath = new File(parentPath, storePath);
        try {
            boolean result = finalPath.mkdirs();
            LOGGER.info("try to create local path {}, result is {}", finalPath, result);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return finalPath;
    }

    /**
     * init env by config
     *
     * @return env config
     */
    private Environment initEnv() {
        EnvironmentConfig envConfig = new EnvironmentConfig()
                .setReadOnly(agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_READONLY, AgentConstants.DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setAllowCreate(!agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_READONLY, AgentConstants.DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setTransactional(agentConf.getBoolean(
                        AgentConstants.AGENT_LOCAL_STORE_TRANSACTIONAL,
                    AgentConstants.DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL))
                .setLockTimeout(
                        agentConf.getInt(AgentConstants.AGENT_LOCAL_STORE_LOCK_TIMEOUT,
                                AgentConstants.DEFAULT_AGENT_LOCAL_STORE_LOCK_TIMEOUT),
                        TimeUnit.MILLISECONDS);
        envConfig.setTxnNoSyncVoid(agentConf.getBoolean(
            AgentConstants.AGENT_LOCAL_STORE_NO_SYNC_VOID,
                AgentConstants.DEFAULT_AGENT_LOCAL_STORE_NO_SYNC_VOID));
        envConfig.setTxnWriteNoSyncVoid(agentConf.getBoolean(
            AgentConstants.AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID,
                AgentConstants.DEFAULT_AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID));
        return new Environment(tryToInitAndGetPath(), envConfig);
    }

    @Override
    public KeyValueEntity get(String key) {
        requireNonNull(key);
        return primaryIndex.get(key);
    }


    @Override
    public CommandEntity getCommand(String commandId) {
        requireNonNull(commandId);
        return commandPrimaryIndex.get(commandId);
    }


    @Override
    public CommandEntity putCommand(CommandEntity entity) {
        requireNonNull(entity);
        return commandPrimaryIndex.put(entity);
    }

    @Override
    public void set(KeyValueEntity entity) {
        requireNonNull(entity);
        primaryIndex.put(entity);
    }

    @Override
    public KeyValueEntity put(KeyValueEntity entity) {
        requireNonNull(entity);
        return primaryIndex.put(entity);
    }

    @Override
    public KeyValueEntity remove(String key) {
        requireNonNull(key);
        KeyValueEntity entity = primaryIndex.get(key);
        primaryIndex.delete(key);
        return entity;
    }

    @Override
    public List<KeyValueEntity> search(StateSearchKey searchKey) {
        requireNonNull(searchKey);
        List<KeyValueEntity> ret = new ArrayList<>();
        try (EntityCursor<KeyValueEntity> children = secondaryIndex.subIndex(searchKey)
                .entities()) {
            for (KeyValueEntity entity : children) {
                ret.add(entity);
            }
        }
        return ret;
    }

    @Override
    public List<CommandEntity> searchCommands(boolean isAcked) {
        requireNonNull(isAcked);
        List<CommandEntity> ret = new ArrayList<>();
        try (EntityCursor<CommandEntity> children = commandSecondaryIndex.subIndex(isAcked)
            .entities()) {
            for (CommandEntity entity : children) {
                ret.add(entity);
            }
        }
        return ret;
    }

    @Override
    public KeyValueEntity searchOne(StateSearchKey searchKey) {
        requireNonNull(searchKey);
        return secondaryIndex.get(searchKey);
    }

    @Override
    public KeyValueEntity searchOne(String fileName) {
        requireNonNull(fileName);
        return fileNameSecondaryIndex.get(fileName);
    }

    @Override
    public List<KeyValueEntity> findAll(String prefix) {
        requireNonNull(prefix);
        List<KeyValueEntity> ret = new ArrayList<>();
        try (EntityCursor<KeyValueEntity> children = primaryIndex.entities()) {
            for (KeyValueEntity entity : children) {
                if (entity.getKey().startsWith(prefix)) {
                    ret.add(entity);
                }
            }
        }
        return ret;
    }

    @Override
    public void close() {
        jobStore.close();
    }
}

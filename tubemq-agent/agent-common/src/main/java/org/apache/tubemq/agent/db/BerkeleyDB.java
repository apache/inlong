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
package org.apache.tubemq.agent.db;

import static java.util.Objects.requireNonNull;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_DB_INSTANCE_NAME;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_LOCK_TIMEOUT;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_NO_SYNC_VOID;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_PATH;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_READONLY;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_TRANSACTIONAL;
import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_DB_INSTANCE_NAME;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_LOCK_TIMEOUT;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_NO_SYNC_VOID;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_PATH;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_READONLY;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL;
import static org.apache.tubemq.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DB implement based on berkeley db.
 */
public class BerkeleyDB implements DB, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyDB.class);

    private final EntityStore store;
    private final PrimaryIndex<String, KeyValueEntity> primaryIndex;
    private final SecondaryIndex<StateSearchKey, String, KeyValueEntity> secondaryIndex;
    private final AgentConfiguration agentConf;

    public BerkeleyDB() {
        this.agentConf = AgentConfiguration.getAgentConf();
        StoreConfig storeConfig = initStoreConfig();
        Environment environment = initEnv();
        String instanceName = agentConf.get(AGENT_DB_INSTANCE_NAME, DEFAULT_AGENT_DB_INSTANCE_NAME);
        this.store = new EntityStore(environment, instanceName, storeConfig);
        primaryIndex = this.store.getPrimaryIndex(String.class, KeyValueEntity.class);
        secondaryIndex = this.store.getSecondaryIndex(primaryIndex, StateSearchKey.class,
                "stateSearchKey");
    }

    /**
     * init store by config
     *
     * @return store config
     */
    private StoreConfig initStoreConfig() {
        return new StoreConfig()
                .setReadOnly(agentConf.getBoolean(
                        AGENT_LOCAL_STORE_READONLY, DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setAllowCreate(!agentConf.getBoolean(
                        AGENT_LOCAL_STORE_READONLY, DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setTransactional(agentConf.getBoolean(
                        AGENT_LOCAL_STORE_TRANSACTIONAL, DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL));
    }

    /**
     * init local bdb path and get it.
     * @return local path.
     */
    private File tryToInitAndGetPath() {
        String storePath = agentConf.get(AGENT_LOCAL_STORE_PATH, DEFAULT_AGENT_LOCAL_STORE_PATH);
        String parentPath = System.getProperty("agent.home");
        File finalPath = new File(parentPath, storePath);
        try {
            LOGGER.info("try to create local path {}", finalPath);
            FileUtils.forceMkdir(finalPath);
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
                        AGENT_LOCAL_STORE_READONLY, DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setAllowCreate(!agentConf.getBoolean(
                        AGENT_LOCAL_STORE_READONLY, DEFAULT_AGENT_LOCAL_STORE_READONLY))
                .setTransactional(agentConf.getBoolean(
                        AGENT_LOCAL_STORE_TRANSACTIONAL, DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL))
                .setLockTimeout(
                        agentConf.getInt(AGENT_LOCAL_STORE_LOCK_TIMEOUT,
                                DEFAULT_AGENT_LOCAL_STORE_LOCK_TIMEOUT),
                        TimeUnit.MILLISECONDS);
        envConfig.setTxnNoSyncVoid(agentConf.getBoolean(AGENT_LOCAL_STORE_NO_SYNC_VOID,
                DEFAULT_AGENT_LOCAL_STORE_NO_SYNC_VOID));
        envConfig.setTxnWriteNoSyncVoid(agentConf.getBoolean(AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID,
                DEFAULT_AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID));
        return new Environment(tryToInitAndGetPath(), envConfig);
    }

    @Override
    public KeyValueEntity get(String key) {
        requireNonNull(key);
        return primaryIndex.get(key);
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
    public KeyValueEntity searchOne(StateSearchKey searchKey) {
        requireNonNull(searchKey);
        return secondaryIndex.get(searchKey);
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
        store.close();
    }
}

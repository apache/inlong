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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.constants.AgentConstants;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DB implement based on rocks db.
 * TODO: this is low priority.
 */
public class RocksDbImp implements Db {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbImp.class);

    private final AgentConfiguration conf;
    private final RocksDB db;

    public RocksDbImp() {
        // init rocks db
        this.conf = AgentConfiguration.getAgentConf();
        this.db = initEnv();
    }

    private RocksDB initEnv() {
        String storePath = conf.get(
            AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.DEFAULT_AGENT_ROCKS_DB_PATH);
        String parentPath = conf.get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
        File finalPath = new File(parentPath, storePath);
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            boolean result = finalPath.mkdirs();
            LOGGER.info("create directory {}, result is {}", finalPath, result);
            return RocksDB.open(options, finalPath.getAbsolutePath());
        } catch (Exception ex) {
            // cannot create local path, stop running.
            throw new RuntimeException(ex);
        }
    }

    @Override
    public KeyValueEntity get(String key) {
        return null;
    }

    @Override
    public CommandEntity getCommand(String commandId) {
        return null;
    }

    @Override
    public CommandEntity putCommand(CommandEntity entity) {
        return null;
    }

    @Override
    public void set(KeyValueEntity entity) {

    }

    @Override
    public KeyValueEntity put(KeyValueEntity entity) {
        return null;
    }

    @Override
    public KeyValueEntity remove(String key) {
        return null;
    }

    @Override
    public List<KeyValueEntity> search(StateSearchKey searchKey) {
        return null;
    }

    @Override
    public List<CommandEntity> searchCommands(boolean isAcked) {
        return null;
    }

    @Override
    public KeyValueEntity searchOne(StateSearchKey searchKey) {
        return null;
    }

    @Override
    public KeyValueEntity searchOne(String fileName) {
        return null;
    }

    @Override
    public List<KeyValueEntity> findAll(String prefix) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

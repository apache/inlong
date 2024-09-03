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

package org.apache.inlong.agent.plugin.utils;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.plugin.store.RocksDBStoreImpl;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.store.TaskStore;

import java.util.List;

public class RocksDBUtils {

    public static void main(String[] args) {
        AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
        Store store = new RocksDBStoreImpl(
                agentConf.get(AgentConstants.AGENT_ROCKS_DB_PATH, AgentConstants.DEFAULT_AGENT_ROCKS_DB_PATH));
        upgrade(store);
    }

    public static void upgrade(Store store) {
        TaskStore triggerProfileDb = new TaskStore(store);
        List<TaskProfile> allTaskProfiles = triggerProfileDb.getTasks();
        allTaskProfiles.forEach(triggerProfile -> {
            if (triggerProfile.hasKey(TaskConstants.TASK_DIR_FILTER_PATTERN)) {
                triggerProfile.set(TaskConstants.FILE_DIR_FILTER_PATTERNS,
                        triggerProfile.get(TaskConstants.TASK_DIR_FILTER_PATTERN));
                triggerProfile.set(TaskConstants.TASK_DIR_FILTER_PATTERN, null);
            }

            triggerProfileDb.storeTask(triggerProfile);
        });
    }

    public static void printTrigger(Store store) {
    }
}

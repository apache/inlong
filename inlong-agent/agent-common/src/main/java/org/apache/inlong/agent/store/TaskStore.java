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

package org.apache.inlong.agent.store;

import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CommonConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Store for task profile
 */
public class TaskStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStore.class);

    private final Store store;

    public TaskStore(Store store) {
        this.store = store;
    }

    /**
     * get task list from task store.
     *
     * @return list of task
     */
    public List<TaskProfile> getTasks() {
        List<KeyValueEntity> result = this.store.findAll(getKey());
        List<TaskProfile> taskList = new ArrayList<>();
        for (KeyValueEntity entity : result) {
            taskList.add(entity.getAsTaskProfile());
        }
        return taskList;
    }

    /**
     * store task profile.
     *
     * @param task task
     */
    public void storeTask(TaskProfile task) {
        if (task.allRequiredKeyExist()) {
            String keyName = getKeyByTaskId(task.getTaskId());
            KeyValueEntity entity = new KeyValueEntity(keyName,
                    task.toJsonStr(), "");
            store.put(entity);
        }
    }

    /**
     * get task profile.
     *
     * @param taskId taskId
     */
    public TaskProfile getTask(String taskId) {
        KeyValueEntity result = this.store.get(getKeyByTaskId(taskId));
        if (result == null) {
            return null;
        }
        return result.getAsTaskProfile();
    }

    /**
     * delete task by id.
     */
    public void deleteTask(String taskId) {
        store.remove(getKeyByTaskId(taskId));
    }

    public String getKey() {
        if (store.getUniqueKey().isEmpty()) {
            return CommonConstants.TASK_ID_PREFIX;
        } else {
            return store.getUniqueKey() + store.getSplitter() + CommonConstants.TASK_ID_PREFIX;
        }
    }

    public String getKeyByTaskId(String taskId) {
        return getKey() + store.getSplitter() + taskId;
    }
}

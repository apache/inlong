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

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.common.enums.InstanceStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Store for instance profile
 */
public class InstanceStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceStore.class);

    private final Store store;

    public InstanceStore(Store store) {
        this.store = store;
    }

    /**
     * list all instance from instance store.
     *
     * @return list of task
     */
    public List<InstanceProfile> listAllInstances() {
        List<KeyValueEntity> result = this.store.findAll(store.getUniqueKey());
        List<InstanceProfile> instanceList = new ArrayList<>();
        for (KeyValueEntity entity : result) {
            instanceList.add(entity.getAsInstanceProfile());
        }
        return instanceList;
    }

    public int getRunningInstanceCount() {
        List<KeyValueEntity> result = this.store.findAll(store.getUniqueKey());
        int count = 0;
        for (KeyValueEntity entity : result) {
            if (entity.getAsInstanceProfile().getState() == InstanceStateEnum.DEFAULT) {
                count++;
            }
        }
        return count;
    }

    /**
     * get instance list from instance store.
     *
     * @return list of task
     */
    public List<InstanceProfile> getInstances(String taskId) {
        List<KeyValueEntity> result = this.store.findAll(getKeyByTaskId(taskId));
        List<InstanceProfile> instanceList = new ArrayList<>();
        for (KeyValueEntity entity : result) {
            instanceList.add(entity.getAsInstanceProfile());
        }
        return instanceList;
    }

    /**
     * store instance profile.
     *
     * @param instance instance
     */
    public void storeInstance(InstanceProfile instance) {
        if (instance.allRequiredKeyExist()) {
            String keyName = getKeyByTaskAndInstanceId(instance.get(TaskConstants.TASK_ID),
                    instance.get(TaskConstants.INSTANCE_ID));
            KeyValueEntity entity = new KeyValueEntity(keyName,
                    instance.toJsonStr(), instance.get(TaskConstants.INSTANCE_ID));
            store.put(entity);
        } else {
            LOGGER.error("instance profile invalid!");
        }
    }

    /**
     * get instance profile.
     *
     * @param taskId task id from manager
     * @param instanceId it can be file name(file collect), table name(db sync) etc
     */
    public InstanceProfile getInstance(String taskId, String instanceId) {
        KeyValueEntity result = this.store.get(getKeyByTaskAndInstanceId(taskId, instanceId));
        if (result == null) {
            return null;
        }
        return result.getAsInstanceProfile();
    }

    /**
     * delete instance
     *
     * @param taskId task id from manager
     * @param instanceId it can be file name(file collect), table name(db sync) etc
     */
    public void deleteInstance(String taskId, String instanceId) {
        store.remove(getKeyByTaskAndInstanceId(taskId, instanceId));
    }

    public String getKey() {
        if (store.getUniqueKey().isEmpty()) {
            return CommonConstants.INSTANCE_ID_PREFIX + store.getSplitter();
        } else {
            return store.getUniqueKey() + store.getSplitter() + CommonConstants.INSTANCE_ID_PREFIX
                    + store.getSplitter();
        }
    }

    public String getKeyByTaskId(String taskId) {
        return getKey() + taskId;
    }

    public String getKeyByTaskAndInstanceId(String taskId, String instanceId) {
        return getKeyByTaskId(taskId) + store.getSplitter() + store.replaceKeywords(instanceId);
    }
}

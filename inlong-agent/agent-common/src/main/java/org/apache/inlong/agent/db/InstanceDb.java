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

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * db interface for instance profile.
 */
public class InstanceDb {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskProfileDb.class);

    private final Db db;

    public InstanceDb(Db db) {
        this.db = db;
    }

    /**
     * get instance list from db.
     *
     * @return list of task
     */
    public List<InstanceProfile> getInstances(String taskId) {
        List<KeyValueEntity> result = this.db.findAll(getKeyByTaskId(taskId));
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
            db.put(entity);
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
        KeyValueEntity result = this.db.get(getKeyByTaskAndInstanceId(taskId, instanceId));
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
        db.remove(getKeyByTaskAndInstanceId(taskId, instanceId));
    }

    private String getKey() {
        return CommonConstants.INSTANCE_ID_PREFIX;
    }

    private String getKeyByTaskId(String taskId) {
        return CommonConstants.INSTANCE_ID_PREFIX + taskId;
    }

    private String getKeyByTaskAndInstanceId(String taskId, String instanceId) {
        return CommonConstants.INSTANCE_ID_PREFIX + taskId + "_" + instanceId;
    }
}

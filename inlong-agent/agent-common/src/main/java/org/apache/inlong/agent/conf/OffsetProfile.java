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

package org.apache.inlong.agent.conf;

import org.apache.inlong.agent.constant.TaskConstants;

import com.google.gson.Gson;

/**
 * job profile which contains details describing properties of one job.
 */
public class OffsetProfile extends AbstractConfiguration {

    private static final Gson GSON = new Gson();

    /**
     * parse json string to configuration instance.
     *
     * @return job configuration
     */
    public static OffsetProfile parseJsonStr(String jsonStr) {
        OffsetProfile offsetProfile = new OffsetProfile();
        offsetProfile.loadJsonStrResource(jsonStr);
        return offsetProfile;
    }

    public OffsetProfile() {
    }

    public OffsetProfile(String taskId, String instanceId, String offset, String inodeInfo) {
        setTaskId(taskId);
        setInstanceId(instanceId);
        setOffset(offset);
        setInodeInfo(inodeInfo);
    }

    public String toJsonStr() {
        return GSON.toJson(getConfigStorage());
    }

    public String getTaskId() {
        return get(TaskConstants.TASK_ID);
    }

    public void setTaskId(String taskId) {
        set(TaskConstants.TASK_ID, taskId);
    }

    public String getInstanceId() {
        return get(TaskConstants.INSTANCE_ID);
    }

    public void setInstanceId(String instanceId) {
        set(TaskConstants.INSTANCE_ID, instanceId);
    }

    public long getLastUpdateTime() {
        return getLong(TaskConstants.LAST_UPDATE_TIME, 0);
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        setLong(TaskConstants.LAST_UPDATE_TIME, lastUpdateTime);
    }

    public String getOffset() {
        return get(TaskConstants.OFFSET, TaskConstants.DEFAULT_OFFSET);
    }

    public void setOffset(String offset) {
        set(TaskConstants.OFFSET, offset);
    }

    public String getInodeInfo() {
        return get(TaskConstants.INODE_INFO);
    }

    public void setInodeInfo(String inodeInfo) {
        set(TaskConstants.INODE_INFO, inodeInfo);
    }

    @Override
    public boolean allRequiredKeyExist() {
        return hasKey(TaskConstants.TASK_ID) && hasKey(TaskConstants.INSTANCE_ID)
                && hasKey(TaskConstants.INODE_INFO) && hasKey(TaskConstants.OFFSET)
                && hasKey(TaskConstants.LAST_UPDATE_TIME);
    }
}

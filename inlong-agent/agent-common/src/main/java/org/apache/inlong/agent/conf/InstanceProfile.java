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
import org.apache.inlong.agent.utils.file.FileUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;
import org.apache.inlong.common.pojo.dataproxy.DataProxyTopicInfo;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;

import com.google.common.collect.ComparisonChain;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.TaskConstants.FILE_TASK_RETRY;
import static org.apache.inlong.agent.constant.TaskConstants.INSTANCE_STATE;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_MQ_CLUSTERS;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_MQ_TOPIC;

/**
 * job profile which contains details describing properties of one job.
 */
public class InstanceProfile extends AbstractConfiguration implements Comparable<InstanceProfile> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProfile.class);
    private static final Gson GSON = new Gson();

    /**
     * parse json string to configuration instance.
     *
     * @return job configuration
     */
    public static InstanceProfile parseJsonStr(String jsonStr) {
        InstanceProfile conf = new InstanceProfile();
        conf.loadJsonStrResource(jsonStr);
        return conf;
    }

    public String toJsonStr() {
        return GSON.toJson(getConfigStorage());
    }

    public void setInstanceClass(String className) {
        set(TaskConstants.INSTANCE_CLASS, className);
    }

    public String getInstanceClass() {
        return get(TaskConstants.INSTANCE_CLASS);
    }

    public String getTaskId() {
        return get(TaskConstants.TASK_ID);
    }

    public String getInstanceId() {
        return get(TaskConstants.INSTANCE_ID);
    }

    public String getCycleUnit() {
        return get(TaskConstants.TASK_CYCLE_UNIT);
    }

    public String getSourceClass() {
        return get(TaskConstants.TASK_SOURCE);
    }

    public String getSinkClass() {
        return get(TaskConstants.TASK_SINK);
    }

    public InstanceStateEnum getState() {
        int value = getInt(INSTANCE_STATE, InstanceStateEnum.DEFAULT.ordinal());
        return InstanceStateEnum.getTaskState(value);
    }

    public void setState(InstanceStateEnum state) {
        setInt(INSTANCE_STATE, state.ordinal());
    }

    public long getFileUpdateTime() {
        return getLong(TaskConstants.FILE_UPDATE_TIME, 0);
    }

    public void setFileUpdateTime(long lastUpdateTime) {
        setLong(TaskConstants.FILE_UPDATE_TIME, lastUpdateTime);
    }

    public String getPredefineFields() {
        return get(TaskConstants.PREDEFINE_FIELDS, "");
    }

    public String getInlongGroupId() {
        return get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
    }

    public String getInlongStreamId() {
        return get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
    }

    @Override
    public boolean allRequiredKeyExist() {
        return hasKey(TaskConstants.FILE_UPDATE_TIME);
    }

    /**
     * get MQClusterInfo list from config
     */
    public List<MQClusterInfo> getMqClusters() {
        List<MQClusterInfo> result = null;
        String mqClusterStr = get(TASK_MQ_CLUSTERS);
        if (StringUtils.isNotBlank(mqClusterStr)) {
            result = GSON.fromJson(mqClusterStr, new TypeToken<List<MQClusterInfo>>() {
            }.getType());
        }
        return result;
    }

    /**
     * get mqTopic from config
     */
    public DataProxyTopicInfo getMqTopic() {
        DataProxyTopicInfo result = null;
        String topicStr = get(TASK_MQ_TOPIC);
        if (StringUtils.isNotBlank(topicStr)) {
            result = GSON.fromJson(topicStr, DataProxyTopicInfo.class);
        }
        return result;
    }

    public void setCreateTime(Long time) {
        setLong(TaskConstants.INSTANCE_CREATE_TIME, time);
    }

    public Long getCreateTime() {
        return getLong(TaskConstants.INSTANCE_CREATE_TIME, 0);
    }

    public void setModifyTime(Long time) {
        setLong(TaskConstants.INSTANCE_MODIFY_TIME, time);
    }

    public Long getModifyTime() {
        return getLong(TaskConstants.INSTANCE_MODIFY_TIME, 0);
    }

    public void setInstanceId(String instanceId) {
        set(TaskConstants.INSTANCE_ID, instanceId);
    }

    public void setSourceDataTime(String dataTime) {
        set(TaskConstants.SOURCE_DATA_TIME, dataTime);
    }

    public String getSourceDataTime() {
        return get(TaskConstants.SOURCE_DATA_TIME);
    }

    public void setSinkDataTime(Long dataTime) {
        setLong(TaskConstants.SINK_DATA_TIME, dataTime);
    }

    public Long getSinkDataTime() {
        return getLong(TaskConstants.SINK_DATA_TIME, 0);
    }

    @Override
    public int compareTo(InstanceProfile object) {
        int ret = ComparisonChain.start()
                .compare(getSourceDataTime(), object.getSourceDataTime())
                .compare(FileUtils.getFileCreationTime(getInstanceId()),
                        FileUtils.getFileCreationTime(object.getInstanceId()))
                .compare(FileUtils.getFileLastModifyTime(getInstanceId()),
                        FileUtils.getFileLastModifyTime(object.getInstanceId()))
                .result();
        return ret;
    }

    public boolean isRetry() {
        return getBoolean(FILE_TASK_RETRY, false);
    }
}

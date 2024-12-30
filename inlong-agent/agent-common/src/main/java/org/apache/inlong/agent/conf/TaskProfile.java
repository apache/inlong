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
import org.apache.inlong.agent.pojo.TaskProfileDto;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;
import org.apache.inlong.common.enums.TaskStateEnum;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_RETRY;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_STATE;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_TYPE;

/**
 * job profile which contains details describing properties of one job.
 */
public class TaskProfile extends AbstractConfiguration {

    public static final String SQL_TASK = "org.apache.inlong.agent.plugin.task.logcollection.SQLTask";
    public static final String FILE_TASK = "org.apache.inlong.agent.plugin.task.logcollection.local.FileTask";
    public static final String COS_TASK = "org.apache.inlong.agent.plugin.task.logcollection.cos.COSTask";
    public static final String KAFKA_TASK = "org.apache.inlong.agent.plugin.task.KafkaTask";
    public static final String PULSAR_TASK = "org.apache.inlong.agent.plugin.task.PulsarTask";
    public static final String MONGODB_TASK = "org.apache.inlong.agent.plugin.task.MongoDBTask";
    public static final String ORACLE_TASK = "org.apache.inlong.agent.plugin.task.OracleTask";
    public static final String REDIS_TASK = "org.apache.inlong.agent.plugin.task.RedisTask";
    public static final String POSTGRESQL_TASK = "org.apache.inlong.agent.plugin.task.PostgreSQLTask";
    public static final String MQTT_TASK = "org.apache.inlong.agent.plugin.task.MqttTask";
    public static final String SQLSERVER_TASK = "org.apache.inlong.agent.plugin.task.SQLServerTask";
    public static final String MOCK_TASK = "org.apache.inlong.agent.plugin.task.MockTask";

    private static final Gson GSON = new Gson();
    private static final Logger logger = LoggerFactory.getLogger(TaskProfile.class);

    /**
     * Get a TaskProfile from a DataConfig
     */
    public static TaskProfile convertToTaskProfile(DataConfig dataConfig) {
        if (dataConfig == null) {
            return null;
        }
        return TaskProfileDto.convertToTaskProfile(dataConfig);
    }

    public String getTaskClass() {
        TaskTypeEnum taskType = TaskTypeEnum.getTaskType(getInt(TASK_TYPE, TaskTypeEnum.FILE.getType()));
        switch (requireNonNull(taskType)) {
            case SQL:
                return SQL_TASK;
            case FILE:
                return FILE_TASK;
            case KAFKA:
                return KAFKA_TASK;
            case PULSAR:
                return PULSAR_TASK;
            case POSTGRES:
                return POSTGRESQL_TASK;
            case ORACLE:
                return ORACLE_TASK;
            case SQLSERVER:
                return SQLSERVER_TASK;
            case MONGODB:
                return MONGODB_TASK;
            case REDIS:
                return REDIS_TASK;
            case MQTT:
                return MQTT_TASK;
            case COS:
                return COS_TASK;
            case MOCK:
                return MOCK_TASK;
            default:
                logger.error("invalid task type {}", taskType);
                return null;
        }
    }

    public String getTaskId() {
        return get(TaskConstants.TASK_ID);
    }

    public String getCycleUnit() {
        return get(TaskConstants.TASK_CYCLE_UNIT);
    }

    public String getTimeZone() {
        return get(TaskConstants.TASK_TIME_ZONE);
    }

    public TaskStateEnum getState() {
        return TaskStateEnum.getTaskState(getInt(TASK_STATE));
    }

    public void setState(TaskStateEnum state) {
        setInt(TASK_STATE, state.ordinal());
    }

    public boolean isRetry() {
        return getBoolean(TASK_RETRY, false);
    }

    public String getInlongGroupId() {
        return get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
    }

    public String getInlongStreamId() {
        return get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
    }

    /**
     * parse json string to configuration instance.
     *
     * @return job configuration
     */
    public static TaskProfile parseJsonStr(String jsonStr) {
        TaskProfile conf = new TaskProfile();
        conf.loadJsonStrResource(jsonStr);
        return conf;
    }

    /**
     * check whether required keys exists.
     *
     * @return return true if all required keys exists else false.
     */
    @Override
    public boolean allRequiredKeyExist() {
        return hasKey(TaskConstants.TASK_ID)
                && hasKey(TaskConstants.TASK_SINK) && hasKey(TaskConstants.TASK_CHANNEL)
                && hasKey(TaskConstants.TASK_GROUP_ID) && hasKey(TaskConstants.TASK_STREAM_ID);
    }

    public String toJsonStr() {
        return GSON.toJson(getConfigStorage());
    }

    public InstanceProfile createInstanceProfile(String fileName, String cycleUnit, String dataTime,
            long fileUpdateTime) {
        InstanceProfile instanceProfile = InstanceProfile.parseJsonStr(toJsonStr());
        instanceProfile.setInstanceId(fileName);
        instanceProfile.setSourceDataTime(dataTime);
        Long sinkDataTime = 0L;
        try {
            sinkDataTime = DateTransUtils.timeStrConvertToMillSec(dataTime, cycleUnit,
                    TimeZone.getTimeZone(getTimeZone()));
        } catch (ParseException e) {
            logger.error("createInstanceProfile ParseException error: ", e);
            return null;
        } catch (Exception e) {
            logger.error("createInstanceProfile Exception error: ", e);
            return null;
        }
        instanceProfile.setSinkDataTime(sinkDataTime);
        instanceProfile.setCreateTime(AgentUtils.getCurrentTime());
        instanceProfile.setModifyTime(AgentUtils.getCurrentTime());
        instanceProfile.setState(InstanceStateEnum.DEFAULT);
        instanceProfile.setFileUpdateTime(fileUpdateTime);
        return instanceProfile;
    }
}

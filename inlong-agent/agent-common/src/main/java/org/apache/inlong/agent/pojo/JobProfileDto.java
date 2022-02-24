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

package org.apache.inlong.agent.pojo;

import com.google.gson.Gson;
import lombok.Data;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.enums.TaskTypeEnum;

import static java.util.Objects.requireNonNull;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_HOST;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PORT;

@Data
public class JobProfileDto {

    public static final String DEFAULT_TRIGGER = "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String MANAGER_JOB = "MANAGER_JOB";
    public static final String DEFAULT_DATAPROXY_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";

    /**
     * file source
     */
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.TextFileSource";
    /**
     * binlog source
     */
    public static final String BINLOG_SOURCE = "org.apache.inlong.agent.plugin.sources.BinlogSource";
    /**
     * kafka source
     */
    public static final String KAFKA_SOURCE = "org.apache.inlong.agent.plugin.sources.KafkaSource";

    private static final Gson GSON = new Gson();

    private Job job;
    private Proxy proxy;

    private static BinlogJob getBinlogJob(DataConfig dataConfigs) {
        BinlogJob.BinlogJobTaskConfig binlogJobTaskConfig = GSON.fromJson(dataConfigs.getExtParams(),
                BinlogJob.BinlogJobTaskConfig.class);

        BinlogJob binlogJob = new BinlogJob();
        binlogJob.setHostname(binlogJobTaskConfig.getHostname());
        binlogJob.setPassword(binlogJobTaskConfig.getPassword());
        binlogJob.setTimeZone(binlogJobTaskConfig.getTimeZone());
        binlogJob.setSnapshotMode(binlogJobTaskConfig.getSnapshotMode());
        binlogJob.setUser(binlogJobTaskConfig.getUser());
        binlogJob.setStoreHistoryFilename(binlogJobTaskConfig.getStoreHistoryFilename());
        binlogJob.setIntervalMs(binlogJobTaskConfig.getIntervalMs());
        binlogJob.setSnapshotMode(binlogJobTaskConfig.getSnapshotMode());
        binlogJob.setOffset(binlogJobTaskConfig.getOffset());

        binlogJob.setChannel(DEFAULT_CHANNEL);
        binlogJob.setName(MANAGER_JOB);
        binlogJob.setSource(BINLOG_SOURCE);
        binlogJob.setSink(DEFAULT_DATAPROXY_SINK);
        binlogJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        binlogJob.setOp(dataConfigs.getOp());

        return binlogJob;
    }

    private static FileJob getFileJob(DataConfig dataConfigs) {
        FileJob fileJob = new FileJob();
        fileJob.setTrigger(DEFAULT_TRIGGER);
        fileJob.setChannel(DEFAULT_CHANNEL);
        fileJob.setName(MANAGER_JOB);
        fileJob.setSource(DEFAULT_SOURCE);
        fileJob.setSink(DEFAULT_DATAPROXY_SINK);

        FileJob.FileJobTaskConfig fileJobTaskConfig = GSON.fromJson(dataConfigs.getExtParams(),
                FileJob.FileJobTaskConfig.class);

        FileJob.Dir dir = new FileJob.Dir();
        dir.setPattern(fileJobTaskConfig.getDataName());
        dir.setPath(fileJobTaskConfig.getPath());
        fileJob.setDir(dir);

        fileJob.setId(fileJobTaskConfig.getTaskId());
        fileJob.setTimeOffset(fileJobTaskConfig.getTimeOffset());

        if (!fileJobTaskConfig.getAdditionalAttr().isEmpty()) {
            fileJob.setAddictiveString(fileJobTaskConfig.getAdditionalAttr());
        }
        if (fileJobTaskConfig.getCycleUnit() != null) {
            fileJob.setCycleUnit(fileJobTaskConfig.getCycleUnit());
        }
        fileJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        fileJob.setOp(dataConfigs.getOp());

        return fileJob;
    }

    private static KafkaJob getKafkaJob(DataConfig dataConfigs) {

        KafkaJob.KafkaJobTaskConfig kafkaJobTaskConfig = GSON.fromJson(dataConfigs.getExtParams(),
                KafkaJob.KafkaJobTaskConfig.class);
        KafkaJob kafkaJob = new KafkaJob();
        kafkaJob.setTopic(kafkaJobTaskConfig.getTopic());
        kafkaJob.setKeyDeserializer(kafkaJobTaskConfig.getValueDeserializer());
        kafkaJob.setValueDeserializer(kafkaJobTaskConfig.getKeyDeserializer());
        kafkaJob.setBootstrapServers(kafkaJobTaskConfig.getBootstrapServers());
        kafkaJob.setGroupId(kafkaJobTaskConfig.getGroupId());
        kafkaJob.setRecordSpeed(kafkaJobTaskConfig.getRecordSpeed());
        kafkaJob.setByteSpeedLimit(kafkaJobTaskConfig.getByteSpeedLimit());
        kafkaJob.setMinInterval(kafkaJobTaskConfig.getMinInterval());
        kafkaJob.setOffset(kafkaJobTaskConfig.getOffset());

        kafkaJob.setChannel(DEFAULT_CHANNEL);
        kafkaJob.setName(MANAGER_JOB);
        kafkaJob.setSource(KAFKA_SOURCE);
        kafkaJob.setSink(DEFAULT_DATAPROXY_SINK);
        kafkaJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        kafkaJob.setOp(dataConfigs.getOp());

        return kafkaJob;
    }

    private static Proxy getProxy(DataConfig dataConfigs) {
        Proxy proxy = new Proxy();
        Manager manager = new Manager();
        AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
        manager.setHost(agentConf.get(AGENT_MANAGER_VIP_HTTP_HOST));
        manager.setPort(agentConf.get(AGENT_MANAGER_VIP_HTTP_PORT));
        proxy.setInlongGroupId(dataConfigs.getInlongGroupId());
        proxy.setInlongStreamId(dataConfigs.getInlongStreamId());
        proxy.setManager(manager);
        return proxy;
    }

    public static TriggerProfile convertToTriggerProfile(DataConfig dataConfigs) {
        if (!dataConfigs.isValid()) {
            throw new IllegalArgumentException("input dataConfig" + dataConfigs + "is invalid please check");
        }
        TaskTypeEnum taskType = TaskTypeEnum.getTaskType(dataConfigs.getTaskType());
        JobProfileDto profileDto = new JobProfileDto();
        Proxy proxy = getProxy(dataConfigs);
        profileDto.setProxy(proxy);
        Job job = new Job();
        switch (requireNonNull(taskType)) {
            case SQL:
            case BINLOG:
                BinlogJob binlogJob = getBinlogJob(dataConfigs);
                job.setBinlogJob(binlogJob);
                profileDto.setJob(job);
                break;
            case FILE:
                FileJob fileJob = getFileJob(dataConfigs);
                job.setFileJob(fileJob);
                profileDto.setJob(job);
                break;
            case KAFKA:
                KafkaJob kafkaJob = getKafkaJob(dataConfigs);
                job.setKafkaJob(kafkaJob);
                profileDto.setJob(job);
                break;
            default:
        }
        return TriggerProfile.parseJsonStr(GSON.toJson(profileDto));
    }

    @Data
    public static class Job {

        private FileJob fileJob;
        private BinlogJob binlogJob;
        private KafkaJob kafkaJob;
    }

    @Data
    public static class Manager {

        private String port;
        private String host;
    }

    @Data
    public static class Proxy {

        private String inlongGroupId;
        private String inlongStreamId;
        private Manager manager;
    }

}
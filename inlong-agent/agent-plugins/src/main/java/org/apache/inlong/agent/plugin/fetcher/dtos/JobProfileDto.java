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

package org.apache.inlong.agent.plugin.fetcher.dtos;

import static java.util.Objects.requireNonNull;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_HOST;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PORT;

import com.google.gson.Gson;
import lombok.Data;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.dto.DataConfig;
import org.apache.inlong.agent.enums.TaskTypeEnum;

@Data
public class JobProfileDto {

    private static final Gson GSON = new Gson();

    private Job job;
    private Proxy proxy;

    public static final String DEFAULT_TRIGGER = "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String MANAGER_JOB = "MANAGER_JOB";
    public static final String DEFAULT_DATAPROXY_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.TextFileSource";

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

    private static BinlogJob getBinlogJob(DataConfig dataConfigs) {

        BinlogJob.BinlogJobTaskConfig binlogJobTaskConfig = new BinlogJob.BinlogJobTaskConfig();
        Gson gson = new Gson();
        binlogJobTaskConfig = gson.fromJson(dataConfigs.getTaskConfig(), BinlogJob.BinlogJobTaskConfig.class);

        binlogJobTaskConfig.setTrigger(DEFAULT_TRIGGER);
        binlogJobTaskConfig.setChannel(DEFAULT_CHANNEL);
        binlogJobTaskConfig.setName(MANAGER_JOB);
        binlogJobTaskConfig.setSource(DEFAULT_SOURCE);
        binlogJobTaskConfig.setSink(DEFAULT_DATAPROXY_SINK);

        binlogJobTaskConfig.setHostname(binlogJobTaskConfig.getHostname());
        binlogJobTaskConfig.setPassword(binlogJobTaskConfig.getPassword());
        binlogJobTaskConfig.setTimeZone(binlogJobTaskConfig.getTimeZone());
        binlogJobTaskConfig.setSnapshotMode(binlogJobTaskConfig.getSnapshotMode());
        binlogJobTaskConfig.setUser(binlogJobTaskConfig.getUser());
        String storeHistoryFilename = binlogJobTaskConfig.getStoreHistoryFilename();
        binlogJobTaskConfig.setStoreHistoryFilename(storeHistoryFilename);
        String intervalMs = binlogJobTaskConfig.getIntervalMs();
        binlogJobTaskConfig.setIntervalMs(intervalMs);
        binlogJobTaskConfig.setSnapshotMode(binlogJobTaskConfig.getSnapshotMode());
        binlogJobTaskConfig.setOffset(binlogJobTaskConfig.getOffset());

        BinlogJob binlogJob = new BinlogJob();

        binlogJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        binlogJob.setOp(dataConfigs.getOp());
        binlogJob.setBinlogJobTaskConfig(binlogJobTaskConfig);

        return binlogJob;
    }

    private static FileJob getFileJob(DataConfig dataConfigs) {

        FileJob fileJob = new FileJob();
        fileJob.setTrigger(DEFAULT_TRIGGER);
        fileJob.setChannel(DEFAULT_CHANNEL);
        fileJob.setName(MANAGER_JOB);
        fileJob.setSource(DEFAULT_SOURCE);
        fileJob.setSink(DEFAULT_DATAPROXY_SINK);

        FileJob.FileJobTaskConfig fileJobTaskConfig = new FileJob.FileJobTaskConfig();
        Gson gson = new Gson();
        fileJobTaskConfig = gson.fromJson(dataConfigs.getTaskConfig(), FileJob.FileJobTaskConfig.class);

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

        KafkaJob.KafkaJobTaskConfig kafkaJobTaskConfig = new KafkaJob.KafkaJobTaskConfig();
        Gson gson = new Gson();
        kafkaJobTaskConfig = gson.fromJson(dataConfigs.getTaskConfig(), KafkaJob.KafkaJobTaskConfig.class);

        kafkaJobTaskConfig.setTrigger(DEFAULT_TRIGGER);
        kafkaJobTaskConfig.setChannel(DEFAULT_CHANNEL);
        kafkaJobTaskConfig.setName(MANAGER_JOB);
        kafkaJobTaskConfig.setSource(DEFAULT_SOURCE);
        kafkaJobTaskConfig.setSink(DEFAULT_DATAPROXY_SINK);

        kafkaJobTaskConfig.setTopic(kafkaJobTaskConfig.getTopic());
        kafkaJobTaskConfig.setKeyDeserializer(kafkaJobTaskConfig.getValueDeserializer());
        kafkaJobTaskConfig.setValueDeserializer(kafkaJobTaskConfig.getKeyDeserializer());
        kafkaJobTaskConfig.setBootstrapServers(kafkaJobTaskConfig.getBootstrapServers());
        kafkaJobTaskConfig.setGroupId(kafkaJobTaskConfig.getGroupId());
        kafkaJobTaskConfig.setRecordSpeed(kafkaJobTaskConfig.getRecordSpeed());
        kafkaJobTaskConfig.setByteSpeedLimit(kafkaJobTaskConfig.getByteSpeedLimit());
        kafkaJobTaskConfig.setMinInterval(kafkaJobTaskConfig.getMinInterval());
        kafkaJobTaskConfig.setOffset(kafkaJobTaskConfig.getOffset());

        KafkaJob kafkaJob = new KafkaJob();
        kafkaJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        kafkaJob.setOp(dataConfigs.getOp());
        kafkaJob.setKafkaJobTaskConfig(kafkaJobTaskConfig);

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
}
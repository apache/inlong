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

        BinlogJob binlogJob = new BinlogJob();

        BinlogJob.BinlogJobTaskConfig binlogJobTaskConfig =new BinlogJob.BinlogJobTaskConfig();
        Gson gson = new Gson();
        binlogJobTaskConfig=gson.fromJson(dataConfigs.getTaskConfig(), BinlogJob.BinlogJobTaskConfig.class);


        binlogJobTaskConfig.setTrigger(DEFAULT_TRIGGER);
        binlogJobTaskConfig.setChannel(DEFAULT_CHANNEL);
        binlogJobTaskConfig.setName(MANAGER_JOB);
        binlogJobTaskConfig.setSource(DEFAULT_SOURCE);
        binlogJobTaskConfig.setSink(DEFAULT_DATAPROXY_SINK);

        binlogJobTaskConfig.setJob_database_hostname(binlogJobTaskConfig.getJob_database_hostname());
        binlogJobTaskConfig.setJob_database_password(binlogJobTaskConfig.getJob_database_password());
        binlogJobTaskConfig.setJob_database_server_time_zone(binlogJobTaskConfig.getJob_database_server_time_zone());
        binlogJobTaskConfig.setJob_database_snapshot_mode(binlogJobTaskConfig.getJob_database_snapshot_mode());
        binlogJobTaskConfig.setJob_database_user(binlogJobTaskConfig.getJob_database_user());
        binlogJobTaskConfig.setJob_database_store_history_filename(binlogJobTaskConfig.getJob_database_store_history_filename());
        binlogJobTaskConfig.setJob_database_store_offset_interval_ms(binlogJobTaskConfig.getJob_database_store_offset_interval_ms());
        binlogJobTaskConfig.setJob_database_snapshot_mode(binlogJobTaskConfig.getJob_database_snapshot_mode());
        binlogJobTaskConfig.setJob_database_offset(binlogJobTaskConfig.getJob_database_offset());

        binlogJob.setDeliveryTime(dataConfigs.getDeliveryTime());
        binlogJob.setOp(dataConfigs.getOp());
        binlogJob.setBinlogJobTaskConfig(binlogJobTaskConfig);

        return binlogJob;
    }

    private static FileJob getFileJob(DataConfig dataConfigs) {

        FileJob fileJob = new FileJob();
        FileJob.Dir dir = new FileJob.Dir();
        FileJob.FileJobTaskConfig fileJobTaskConfig =new FileJob.FileJobTaskConfig();
        Gson gson = new Gson();
        fileJobTaskConfig=gson.fromJson(dataConfigs.getTaskConfig(), FileJob.FileJobTaskConfig.class);

        fileJob.setTrigger(DEFAULT_TRIGGER);
        fileJob.setChannel(DEFAULT_CHANNEL);
        fileJob.setName(MANAGER_JOB);
        fileJob.setSource(DEFAULT_SOURCE);
        fileJob.setSink(DEFAULT_DATAPROXY_SINK);


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

        KafkaJob kafkaJob = new KafkaJob();

        KafkaJob.KafkaJobTaskConfig kafkaJobTaskConfig =new KafkaJob.KafkaJobTaskConfig();
        Gson gson = new Gson();
        kafkaJobTaskConfig=gson.fromJson(dataConfigs.getTaskConfig(), KafkaJob.KafkaJobTaskConfig.class);

        kafkaJobTaskConfig.setTrigger(DEFAULT_TRIGGER);
        kafkaJobTaskConfig.setChannel(DEFAULT_CHANNEL);
        kafkaJobTaskConfig.setName(MANAGER_JOB);
        kafkaJobTaskConfig.setSource(DEFAULT_SOURCE);
        kafkaJobTaskConfig.setSink(DEFAULT_DATAPROXY_SINK);

        kafkaJobTaskConfig.setSource_kafka_topic(kafkaJobTaskConfig.getSource_kafka_topic());
        kafkaJobTaskConfig.setSource_kafka_key_deserializer(kafkaJobTaskConfig.getSource_kafka_value_deserializer());
        kafkaJobTaskConfig.setSource_kafka_value_deserializer(kafkaJobTaskConfig.getSource_kafka_key_deserializer());
        kafkaJobTaskConfig.setSource_kafka_bootstrap_servers(kafkaJobTaskConfig.getSource_kafka_bootstrap_servers());
        kafkaJobTaskConfig.setSource_kafka_group_id(kafkaJobTaskConfig.getSource_kafka_group_id());
        kafkaJobTaskConfig.setSource_kafka_record_speed(kafkaJobTaskConfig.getSource_kafka_record_speed());
        kafkaJobTaskConfig.setSource_kafka_byte_speed_limit(kafkaJobTaskConfig.getSource_kafka_byte_speed_limit());
        kafkaJobTaskConfig.setSource_kafka_min_interval(kafkaJobTaskConfig.getSource_kafka_min_interval());
        kafkaJobTaskConfig.setSource_kafka_offset(kafkaJobTaskConfig.getSource_kafka_offset());

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
        TaskTypeEnum taskType=TaskTypeEnum.getTaskType(dataConfigs.getTaskType());
        JobProfileDto profileDto = new JobProfileDto();
        Proxy proxy = getProxy(dataConfigs);
        Job job =new Job();
        switch (taskType) {
            case SQL:
            case BINLOG:
                BinlogJob binlogJob = getBinlogJob(dataConfigs);
                job.setBinlogJob(binlogJob);
                profileDto.setJob(job);
            case FILE:
                FileJob fileJob = getFileJob(dataConfigs);
                job.setFileJob(fileJob);
                profileDto.setJob(job);
            case KAFKA:
                KafkaJob kafkaJob = getKafkaJob(dataConfigs);
                job.setKafkaJob(kafkaJob);
                profileDto.setJob(job);
            default:
        }
        profileDto.setProxy(proxy);
        return TriggerProfile.parseJsonStr(GSON.toJson(profileDto));
    }
}
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

    private BinlogJob binlogJob;
    private FileJob fileJob;
    private Proxy proxy;

    public static final String DEFAULT_TRIGGER = "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String MANAGER_JOB = "MANAGER_JOB";
    public static final String DEFAULT_DATAPROXY_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.TextFileSource";

    @Data
    public static class Dir {

        private String path;
        private String pattern;
    }

    @Data
    public static class Running {

        private String core;
    }

    @Data
    public static class Thread {

        private Running running;
    }

    @Data
    public static class FileJob {

        private String op;
        private String name;
        private String source;
        private String sink;
        private String channel;
        private String trigger;
        private String deliveryTime;


        private Dir dir;
        private int id;
        private Thread thread;
        private String pattern;
        private String cycleUnit;
        private String timeOffset;
        private String addictiveString;
    }

    @Data
    public static class FileJobTaskConfig {

        private String dataName;
        private String path;
        private int taskId;
        private Thread thread;
        private String pattern;
        private String cycleUnit;
        private String timeOffset;
        private String additionalAttr;
    }

    @Data
    public static class BinlogJob {

        private String op;
        private String trigger;
        private String name;
        private String source;
        private String sink;
        private String channel;
        private String deliveryTime;

        private  String job_database_user;
        private  String job_database_password;
        private  String job_database_hostname ;
        private  String job_database_whitelist ;
        private  String job_database_server_time_zone;
        private  String job_database_store_offset_interval_ms;
        private  String job_database_store_history_filename;
        private  String job_database_snapshot_mode;
    }

    @Data
    public static class BinlogJobTaskConfig {

        private  String job_database_user;
        private  String job_database_password;
        private  String job_database_hostname ;
        private  String job_database_whitelist ;
        private  String job_database_server_time_zone;
        private  String job_database_store_offset_interval_ms;
        private  String job_database_store_history_filename;
        private  String job_database_snapshot_mode;
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
        BinlogJobTaskConfig binlogJobTaskConfig =new BinlogJobTaskConfig();
        Gson gson = new Gson();
        binlogJobTaskConfig=gson.fromJson(dataConfigs.getTaskConfig(),BinlogJobTaskConfig.class);


        binlogJob.setTrigger(DEFAULT_TRIGGER);
        binlogJob.setChannel(DEFAULT_CHANNEL);
        binlogJob.setName(MANAGER_JOB);
        binlogJob.setSource(DEFAULT_SOURCE);
        binlogJob.setSink(DEFAULT_DATAPROXY_SINK);
        binlogJob.setOp(dataConfigs.getOp());
        binlogJob.setDeliveryTime(dataConfigs.getDeliveryTime());

        binlogJob.setJob_database_hostname(binlogJobTaskConfig.getJob_database_hostname());
        binlogJob.setJob_database_password(binlogJobTaskConfig.getJob_database_password());
        binlogJob.setJob_database_server_time_zone(binlogJobTaskConfig.getJob_database_server_time_zone());
        binlogJob.setJob_database_snapshot_mode(binlogJobTaskConfig.getJob_database_snapshot_mode());
        binlogJob.setJob_database_user(binlogJobTaskConfig.getJob_database_user());
        binlogJob.setJob_database_store_history_filename(binlogJobTaskConfig.getJob_database_store_history_filename());
        binlogJob.setJob_database_store_offset_interval_ms(binlogJobTaskConfig.getJob_database_store_offset_interval_ms());
        binlogJob.setJob_database_snapshot_mode(binlogJobTaskConfig.getJob_database_snapshot_mode());

        return binlogJob;
    }

    private static FileJob getFileJob(DataConfig dataConfigs) {

        FileJob fileJob = new FileJob();
        Dir dir = new Dir();
        FileJobTaskConfig fileJobTaskConfig =new FileJobTaskConfig();
        Gson gson = new Gson();
        fileJobTaskConfig=gson.fromJson(dataConfigs.getTaskConfig(),FileJobTaskConfig.class);

        fileJob.setTrigger(DEFAULT_TRIGGER);
        fileJob.setChannel(DEFAULT_CHANNEL);
        fileJob.setName(MANAGER_JOB);
        fileJob.setSource(DEFAULT_SOURCE);
        fileJob.setSink(DEFAULT_DATAPROXY_SINK);
        fileJob.setOp(dataConfigs.getOp());
        fileJob.setDeliveryTime(dataConfigs.getDeliveryTime());

        dir.setPattern(fileJobTaskConfig.getDataName());
        fileJob.setDir(dir);

        fileJob.setId(fileJobTaskConfig.getTaskId());
        fileJob.setTimeOffset(fileJobTaskConfig.getTimeOffset());

        if (!fileJobTaskConfig.getAdditionalAttr().isEmpty()) {
            fileJob.setAddictiveString(fileJobTaskConfig.getAdditionalAttr());
        }
        if (fileJobTaskConfig.getCycleUnit() != null) {
            fileJob.setCycleUnit(fileJobTaskConfig.getCycleUnit());
        }
        return fileJob;
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
        switch (taskType) {
            case SQL:
            case BINLOG:
                BinlogJob binlogJob = getBinlogJob(dataConfigs);
                profileDto.setBinlogJob(binlogJob);
            case FILE:
                FileJob job = getFileJob(dataConfigs);
                profileDto.setFileJob(job);
            case KAFKA:
            default:
        }
        profileDto.setProxy(proxy);
        return TriggerProfile.parseJsonStr(GSON.toJson(profileDto));
    }
}
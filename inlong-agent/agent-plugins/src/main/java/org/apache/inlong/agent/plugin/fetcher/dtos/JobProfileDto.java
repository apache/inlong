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

@Data
public class JobProfileDto {

    private static final Gson GSON = new Gson();
    private Job job;
    private Proxy proxy;

    public static final String DEFAULT_TRIGGER = "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String MANAGER_JOB = "MANAGER_JOB";
    public static final String DEFAULT_DATAPROXY_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.DataBaseSource";

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
    public static class Job {

        private Dir dir;
        private String trigger;
        private int id;
        private Thread thread;
        private String name;
        private String source;
        private String sink;
        private String channel;
        private String pattern;
        private String op;
        private String cycleUnit;
        private String timeOffset;
        private String deliveryTime;
        private String addictiveString;
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

    private static Job getJob(DataConfig dataConfigs) {
        Job job = new Job();
        Dir dir = new Dir();
        dir.setPattern(dataConfigs.getDataName());
        job.setDir(dir);
        job.setTrigger(DEFAULT_TRIGGER);
        job.setChannel(DEFAULT_CHANNEL);
        job.setName(MANAGER_JOB);
        job.setSource(DEFAULT_SOURCE);
        job.setSink(DEFAULT_DATAPROXY_SINK);
        job.setId(dataConfigs.getTaskId());
        job.setTimeOffset(dataConfigs.getTimeOffset());
        job.setOp(dataConfigs.getOp());
        job.setDeliveryTime(dataConfigs.getDeliveryTime());
        if (!dataConfigs.getAdditionalAttr().isEmpty()) {
            job.setAddictiveString(dataConfigs.getAdditionalAttr());
        }
        if (dataConfigs.getCycleUnit() != null) {
            job.setCycleUnit(dataConfigs.getCycleUnit());
        }
        return job;
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
        JobProfileDto profileDto = new JobProfileDto();
        Proxy proxy = getProxy(dataConfigs);
        Job job = getJob(dataConfigs);
        profileDto.setProxy(proxy);
        profileDto.setJob(job);
        return TriggerProfile.parseJsonStr(GSON.toJson(profileDto));
    }
}
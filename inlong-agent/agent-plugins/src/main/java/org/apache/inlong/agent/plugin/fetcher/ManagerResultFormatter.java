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


package org.apache.inlong.agent.plugin.fetcher;

import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_HOST;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PORT;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.plugin.fetcher.dtos.DataConfig;
import org.apache.inlong.agent.plugin.fetcher.dtos.JobProfileDto;
import org.apache.inlong.agent.plugin.fetcher.dtos.JobProfileDto.Proxy;
import org.apache.inlong.agent.plugin.fetcher.dtos.JobProfileDto.Dir;
import org.apache.inlong.agent.plugin.fetcher.dtos.JobProfileDto.Job;
import org.apache.inlong.agent.plugin.fetcher.dtos.JobProfileDto.Manager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * check manager interface result with json formatter.
 */
public class ManagerResultFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerResultFormatter.class);

    private static final String RESULT_CODE = "success";
    private static final String RESULT_DATA = "data";
    public static final String SUCCESS_CODE = "true";

    private static final Gson GSON = new Gson();
    public static final String DEFAULT_TRIGGER = "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger";
    public static final String DEFAULT_CHANNEL = "org.apache.inlong.agent.plugin.channel.MemoryChannel";
    public static final String TDM_JOB = "TDM_JOB";
    public static final String DEFAULT_BUS_SINK = "org.apache.inlong.agent.plugin.sinks.ProxySink";
    public static final String DEFAULT_SOURCE = "org.apache.inlong.agent.plugin.sources.TextFileSource";

    /**
     * get json result
     * @return json object
     */
    public static JsonObject getResultData(String jsonStr) {
        JsonObject object = GSON.fromJson(jsonStr, JsonObject.class);
        if (object == null || !object.has(RESULT_CODE)
                || !object.has(RESULT_DATA)
                || !SUCCESS_CODE.equals(object.get(RESULT_CODE).getAsString())) {
            throw new IllegalArgumentException("cannot get result data,"
                + " please check manager status, return str is" + jsonStr);

        }
        return object;
    }

    /**
     * get random list of base list.
     * @param baseList - base list
     * @param num - max Num
     * @return random list
     */
    public static <T> List<T> getRandomList(List<T> baseList, int num) {
        if (baseList == null) {
            return new ArrayList<>();
        }
        // make sure num cannot exceed size of base list
        List<T> newHostList = new ArrayList<>(baseList);
        Collections.shuffle(newHostList);
        num = Math.min(num, baseList.size());
        return newHostList.subList(0, num);
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

    private static Job getJob(DataConfig dataConfigs) {
        Job job = new Job();
        Dir dir = new Dir();
        dir.setPattern(dataConfigs.getDataName());
        job.setDir(dir);
        job.setTrigger(DEFAULT_TRIGGER);
        job.setChannel(DEFAULT_CHANNEL);
        job.setName(TDM_JOB);
        job.setSource(DEFAULT_SOURCE);
        job.setSink(DEFAULT_BUS_SINK);
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
        proxy.setBid(dataConfigs.getBusinessIdentifier());
        proxy.setTid(dataConfigs.getDataStreamIdentifier());
        proxy.setManager(manager);
        return proxy;
    }


}

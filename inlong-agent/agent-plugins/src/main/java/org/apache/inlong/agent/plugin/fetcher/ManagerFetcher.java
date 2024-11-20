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

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.pojo.agent.AgentConfigInfo;
import org.apache.inlong.common.pojo.agent.AgentConfigRequest;
import org.apache.inlong.common.pojo.agent.AgentResponseCode;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_TASK_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_EXIST_TASK_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.ManagerResultFormatter.getResultData;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalIp;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalUuid;

/**
 * Fetch command from Inlong-Manager
 */
public class ManagerFetcher extends AbstractDaemon implements ProfileFetcher {

    public static final String AGENT = "agent";
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerFetcher.class);
    private static final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = gsonBuilder.create();
    private final String baseManagerUrl;
    private final String staticConfigUrl;
    private final String agentConfigInfoUrl;
    private final AgentConfiguration conf;
    private final String uniqId;
    private final AgentManager agentManager;
    private final HttpManager httpManager;
    private String localIp;
    private String uuid;
    private String clusterTag;
    private String clusterName;

    public ManagerFetcher(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.conf = AgentConfiguration.getAgentConf();
        if (requiredKeys(conf)) {
            httpManager = new HttpManager(conf);
            baseManagerUrl = httpManager.getBaseUrl();
            staticConfigUrl = buildStaticConfigUrl(baseManagerUrl);
            agentConfigInfoUrl = buildAgentConfigInfoUrl(baseManagerUrl);
            uniqId = conf.get(AGENT_UNIQ_ID, DEFAULT_AGENT_UNIQ_ID);
            clusterTag = conf.get(AGENT_CLUSTER_TAG);
            clusterName = conf.get(AGENT_CLUSTER_NAME);
        } else {
            throw new RuntimeException("init manager error, cannot find required key");
        }
    }

    private boolean requiredKeys(AgentConfiguration conf) {
        return conf.hasKey(AGENT_MANAGER_ADDR);
    }

    /**
     * Build task config url for manager according to config
     *
     * example - http://127.0.0.1:8080/inlong/manager/openapi/agent/getTaskConf
     */
    private String buildStaticConfigUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_TASK_HTTP_PATH, DEFAULT_AGENT_MANAGER_EXIST_TASK_HTTP_PATH);
    }

    /**
     * Build agent config info url for manager according to config
     *
     * example - http://127.0.0.1:8080/inlong/manager/openapi/agent/getConfig
     */
    private String buildAgentConfigInfoUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_CONFIG_HTTP_PATH, DEFAULT_AGENT_MANAGER_CONFIG_HTTP_PATH);
    }

    /**
     * Request manager to get task config, make sure it is not throwing exceptions
     */
    public TaskResult getStaticConfig() {
        LOGGER.info("Get static config start");
        String resultStr = httpManager.doSentPost(staticConfigUrl, getTaskRequest());
        LOGGER.info("Url to get static config staticConfigUrl {}", staticConfigUrl);
        JsonObject resultData = getResultData(resultStr);
        JsonElement element = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        LOGGER.info("Get static config  end");
        if (element != null) {
            LOGGER.info("Get static config  not null {}", resultData);
            return GSON.fromJson(element.getAsJsonObject(), TaskResult.class);
        } else {
            LOGGER.info("Get static config  nothing to do");
            return null;
        }
    }

    /**
     * Request manager to get config, make sure it is not throwing exceptions
     */
    public AgentConfigInfo getAgentConfigInfo() {
        LOGGER.info("Get agent config info");
        String resultStr = httpManager.doSentPost(agentConfigInfoUrl, getAgentConfigInfoRequest());
        LOGGER.info("Url to get agent config agentConfigInfoUrl {}", agentConfigInfoUrl);
        JsonObject resultData = getResultData(resultStr);
        JsonElement element = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        LOGGER.info("Get agent config end");
        if (element != null) {
            LOGGER.info("Get agent config not null {}", resultData);
            return GSON.fromJson(element.getAsJsonObject(), AgentConfigInfo.class);
        } else {
            LOGGER.info("Get agent config nothing to do");
            return null;
        }
    }

    public TaskRequest getTaskRequest() {
        TaskRequest request = new TaskRequest();
        request.setMd5(agentManager.getTaskManager().getTaskResultMd5());
        request.setAgentIp(localIp);
        request.setUuid(uuid);
        request.setClusterName(clusterName);
        request.setPullJobType(PullJobTypeEnum.NEW.getType());
        request.setCommandInfo(null);
        return request;
    }

    public AgentConfigRequest getAgentConfigInfoRequest() {
        AgentConfigRequest request = new AgentConfigRequest();
        if (AgentManager.getAgentConfigInfo() != null) {
            request.setMd5(AgentManager.getAgentConfigInfo().getMd5());
        }
        request.setClusterTag(clusterTag);
        request.setClusterName(clusterName);
        request.setIp(localIp);
        return request;
    }

    /**
     * Thread for profile fetcher.
     *
     * @return runnable profile.
     */
    private Runnable configFetchThread() {
        return () -> {
            Thread.currentThread().setName("ManagerFetcher");
            while (isRunnable()) {
                try {
                    TaskResult taskResult = getStaticConfig();
                    if (taskResult != null && taskResult.getCode().equals(AgentResponseCode.SUCCESS)
                            && agentManager.getTaskManager().getTaskResultVersion() < taskResult.getVersion()) {
                        List<TaskProfile> taskProfiles = new ArrayList<>();
                        taskResult.getDataConfigs().forEach((config) -> {
                            TaskProfile profile = TaskProfile.convertToTaskProfile(config);
                            taskProfiles.add(profile);
                        });
                        agentManager.getTaskManager().submitTaskProfiles(taskProfiles);
                        agentManager.getTaskManager().setTaskResultMd5(taskResult.getMd5());
                        agentManager.getTaskManager().setTaskResultVersion(taskResult.getVersion());
                    }
                    AgentConfigInfo config = getAgentConfigInfo();
                    if (config != null && config.getCode().equals(AgentResponseCode.SUCCESS)) {
                        if (AgentManager.getAgentConfigInfo() == null
                                || AgentManager.getAgentConfigInfo().getVersion() < config.getVersion()) {
                            agentManager.subNewAgentConfigInfo(config);
                        }
                    }
                } catch (Throwable ex) {
                    LOGGER.warn("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                } finally {
                    AgentUtils.silenceSleepInSeconds(AgentUtils.getRandomBySeed(
                            conf.getInt(AGENT_FETCHER_INTERVAL, DEFAULT_AGENT_FETCHER_INTERVAL)));
                }
            }
        };
    }

    private TaskResult getTestConfig(String testDir, int normalTaskId, int retryTaskId, int state) {
        List<DataConfig> configs = new ArrayList<>();
        String normalPattern = testDir + "YYYY/YYYYMMDDhhmm_2.log_[0-9]+";
        String retryPattern = testDir + "YYYY/YYYYMMDD_1.log_[0-9]+";
        configs.add(getTestDataConfig(normalTaskId, normalPattern, false, "202307100000", "202307220000",
                CycleUnitType.MINUTE, state));
        configs.add(
                getTestDataConfig(retryTaskId, retryPattern, true, "20230710", "20230722", CycleUnitType.DAY, state));
        return TaskResult.builder().dataConfigs(configs).build();
    }

    private DataConfig getTestDataConfig(int taskId, String pattern, boolean retry, String startTime, String endTime,
            String cycleUnit, int state) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("devcloud_group_id");
        dataConfig.setInlongStreamId("devcloud_stream_id");
        dataConfig.setDataReportType(0);
        dataConfig.setTaskType(3);
        dataConfig.setTaskId(taskId);
        dataConfig.setState(state);
        dataConfig.setTimeZone("GMT+8:00");
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);
        fileTaskConfig.setTimeOffset("0d");
        fileTaskConfig.setMaxFileCount(100);
        fileTaskConfig.setCycleUnit(cycleUnit);
        fileTaskConfig.setRetry(retry);
        fileTaskConfig.setDataTimeFrom(startTime);
        fileTaskConfig.setDataTimeTo(endTime);
        fileTaskConfig.setDataContentStyle("CSV");
        fileTaskConfig.setDataSeparator("|");
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }

    @Override
    public void start() throws Exception {
        // when agent start, check local ip and fetch manager ip list;
        localIp = fetchLocalIp();
        uuid = fetchLocalUuid();
        submitWorker(configFetchThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
    }
}

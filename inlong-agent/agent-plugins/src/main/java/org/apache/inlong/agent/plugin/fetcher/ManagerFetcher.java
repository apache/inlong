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
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.db.CommandEntity;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_TASK_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_TASK_HTTP_PATH;
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
    private final String taskConfigUrl;
    private final String staticConfigUrl;
    private final AgentConfiguration conf;
    private final String uniqId;
    private final AgentManager agentManager;
    private final HttpManager httpManager;
    private String localIp;
    private String uuid;
    private String clusterName;

    public ManagerFetcher(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.conf = AgentConfiguration.getAgentConf();
        if (requiredKeys(conf)) {
            httpManager = new HttpManager(conf);
            baseManagerUrl = HttpManager.buildBaseUrl();
            taskConfigUrl = buildTaskConfigUrl(baseManagerUrl);
            staticConfigUrl = buildStaticConfigUrl(baseManagerUrl);
            uniqId = conf.get(AGENT_UNIQ_ID, DEFAULT_AGENT_UNIQ_ID);
            clusterName = conf.get(AGENT_CLUSTER_NAME);
        } else {
            throw new RuntimeException("init manager error, cannot find required key");
        }
    }

    private boolean requiredKeys(AgentConfiguration conf) {
        return conf.hasKey(AGENT_MANAGER_ADDR);
    }

    /**
     * build task config url for manager according to config
     *
     * example - http://127.0.0.1:8080/inlong/manager/openapi/fileAgent/getTaskConf
     */
    private String buildTaskConfigUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_TASK_HTTP_PATH, DEFAULT_AGENT_MANAGER_TASK_HTTP_PATH);
    }

    /**
     * build task config url for manager according to config
     *
     * example - http://127.0.0.1:8080/inlong/manager/openapi/fileAgent/getTaskConf
     */
    private String buildStaticConfigUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_TASK_HTTP_PATH, DEFAULT_AGENT_MANAGER_CONFIG_HTTP_PATH);
    }

    /**
     * request manager to get commands, make sure it is not throwing exceptions
     */
    public TaskResult fetchTaskConfig() {
        LOGGER.info("fetchTaskConfig start");
        String resultStr = httpManager.doSentPost(taskConfigUrl, getFetchRequest(null));
        JsonObject resultData = getResultData(resultStr);
        JsonElement element = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        LOGGER.info("fetchTaskConfig end");
        if (element != null) {
            LOGGER.info("fetchTaskConfig not null {}", resultData);
            return GSON.fromJson(element.getAsJsonObject(), TaskResult.class);
        } else {
            LOGGER.info("fetchTaskConfig nothing to do");
            return null;
        }
    }

    /**
     * request manager to get commands, make sure it is not throwing exceptions
     */
    public TaskResult getStaticConfig() {
        LOGGER.info("getStaticConfig start");
        String resultStr = httpManager.doSentPost(staticConfigUrl, getFetchRequest(null));
        LOGGER.info("test123 staticConfigUrl {}", staticConfigUrl);
        JsonObject resultData = getResultData(resultStr);
        JsonElement element = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        LOGGER.info("getStaticConfig end");
        if (element != null) {
            LOGGER.info("test123 getStaticConfig not null {}", resultData);
            return GSON.fromJson(element.getAsJsonObject(), TaskResult.class);
        } else {
            LOGGER.info("getStaticConfig nothing to do");
            return null;
        }
    }

    /**
     * form file command fetch request
     */
    public TaskRequest getFetchRequest(List<CommandEntity> unackedCommands) {
        TaskRequest request = new TaskRequest();
        request.setAgentIp(localIp);
        request.setUuid(uuid);
        request.setClusterName(clusterName);
        request.setPullJobType(PullJobTypeEnum.NEW.getType());
        request.setCommandInfo(unackedCommands);
        return request;
    }

    /**
     * thread for profile fetcher.
     *
     * @return runnable profile.
     */
    private Runnable taskConfigFetchThread() {
        return () -> {
            Thread.currentThread().setName("ManagerFetcher");
            while (isRunnable()) {
                try {
                    int configSleepTime = conf.getInt(AGENT_FETCHER_INTERVAL, DEFAULT_AGENT_FETCHER_INTERVAL);
                    TaskResult taskResult = getStaticConfig();
                    if (taskResult != null) {
                        List<TaskProfile> taskProfiles = new ArrayList<>();
                        taskResult.getDataConfigs().forEach((config) -> {
                            TaskProfile profile = TaskProfile.convertToTaskProfile(config);
                            taskProfiles.add(profile);
                        });
                        agentManager.getTaskManager().submitTaskProfiles(taskProfiles);
                    }
                    TimeUnit.SECONDS.sleep(AgentUtils.getRandomBySeed(configSleepTime));
                } catch (Throwable ex) {
                    LOGGER.warn("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private TaskResult getTestConfig(String testDir, int normalTaskId, int retryTaskId, int state) {
        List<DataConfig> configs = new ArrayList<>();
        String startStr = "2023-07-10 00:00:00";
        String endStr = "2023-07-22 00:00:00";
        Long start = 0L;
        Long end = 0L;
        String normalPattern = testDir + "YYYY/YYYYMMDD_2.log_[0-9]+";
        String retryPattern = testDir + "YYYY/YYYYMMDD_1.log_[0-9]+";
        try {
            Date parse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startStr);
            start = parse.getTime();
            parse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endStr);
            end = parse.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        configs.add(getTestDataConfig(normalTaskId, normalPattern, false, start, end, state));
        configs.add(getTestDataConfig(retryTaskId, retryPattern, true, start, end, state));
        return TaskResult.builder().dataConfigs(configs).build();
    }

    private DataConfig getTestDataConfig(int taskId, String pattern, boolean retry, Long startTime, Long endTime,
            int state) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId"); // 老字段 groupId
        dataConfig.setInlongStreamId("testStreamId"); // 老字段 streamId
        dataConfig.setDataReportType(1); // 老字段 reportType
        dataConfig.setTaskType(3); // 老字段 任务类型，3 代表文件采集
        dataConfig.setTaskId(taskId); // 老字段 任务 id
        dataConfig.setState(state); // 新增！ 任务状态 1 正常 2 暂停
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);// 正则
        fileTaskConfig.setTimeOffset("0d"); // 老字段 时间偏移 "-1d" 采一天前的 "-2h" 采 2 小时前的
        fileTaskConfig.setMaxFileCount(100); // 最大文件数
        fileTaskConfig.setCycleUnit("D"); // 新增！ 任务周期 "D" 天 "h" 小时
        fileTaskConfig.setRetry(retry); // 新增！ 是否补录，如果是补录任务则为 true
        fileTaskConfig.setStartTime(startTime);
        fileTaskConfig.setEndTime(endTime);
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }

    @Override
    public void start() throws Exception {
        // when agent start, check local ip and fetch manager ip list;
        localIp = fetchLocalIp();
        uuid = fetchLocalUuid();
        submitWorker(taskConfigFetchThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
    }
}

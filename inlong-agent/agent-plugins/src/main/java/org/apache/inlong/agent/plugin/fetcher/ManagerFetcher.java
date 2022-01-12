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

import static java.util.Objects.requireNonNull;
import static org.apache.inlong.agent.constants.AgentConstants.AGENT_HOME;
import static org.apache.inlong.agent.constants.AgentConstants.AGENT_LOCAL_CACHE;
import static org.apache.inlong.agent.constants.AgentConstants.AGENT_LOCAL_CACHE_TIMEOUT;
import static org.apache.inlong.agent.constants.AgentConstants.AGENT_LOCAL_IP;
import static org.apache.inlong.agent.constants.AgentConstants.AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_AGENT_HOME;
import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_CACHE;
import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_AGENT_LOCAL_CACHE_TIMEOUT;
import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_AGENT_UNIQ_ID;
import static org.apache.inlong.agent.constants.JobConstants.JOB_OP;
import static org.apache.inlong.agent.constants.JobConstants.JOB_RETRY_TIME;
import static org.apache.inlong.agent.plugin.fetcher.ManagerResultFormatter.getResultData;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_HOST;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PORT;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_IP_CHECK_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_IP;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_TASK_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_AGENT_TDM_IP_CHECK_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_AGENT_MANAGER_TASK_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_AGENT_TDM_VIP_HTTP_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.plugin.fetcher.constants.FetcherConstants.DEFAULT_LOCAL_IP;
import static org.apache.inlong.agent.plugin.utils.PluginUtils.copyJobProfile;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.inlong.agent.cache.LocalFileCache;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.db.CommandDb;
import org.apache.inlong.agent.db.CommandEntity;
import org.apache.inlong.agent.plugin.Trigger;
import org.apache.inlong.agent.plugin.fetcher.dtos.CmdConfig;
import org.apache.inlong.agent.plugin.fetcher.dtos.ConfirmAgentIpRequest;
import org.apache.inlong.agent.plugin.fetcher.dtos.TaskRequestDto;
import org.apache.inlong.agent.plugin.fetcher.dtos.TaskResult;
import org.apache.inlong.agent.plugin.fetcher.enums.ManagerOpEnum;
import org.apache.inlong.agent.plugin.utils.HttpManager;
import org.apache.inlong.agent.plugin.utils.PluginUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * fetch command from manager
 */
public class ManagerFetcher extends AbstractDaemon implements ProfileFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerFetcher.class);

    private static final Gson GSON = new Gson();
    private static final int MAX_RETRY = 2;
    public static final String AGENT = "agent";
    private final String managerVipUrl;
    private final String baseManagerUrl;
    private final String managerTaskUrl;
    private final String managerIpsCheckUrl;
    private final AgentConfiguration conf;
    private final LocalFileCache localFileCache;
    private final String uniqId;
    private List<String> managerList;
    private final AgentManager agentManager;
    private final HttpManager httpManager;
    private String localIp;

    private CommandDb commandDb;

    private boolean requiredKeys(AgentConfiguration conf) {
        return conf.hasKey(AGENT_MANAGER_VIP_HTTP_HOST) && conf.hasKey(AGENT_MANAGER_VIP_HTTP_PORT);
    }

    public ManagerFetcher(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.conf = AgentConfiguration.getAgentConf();
        if (requiredKeys(conf)) {
            httpManager = new HttpManager(conf);
            baseManagerUrl = "http://" + conf.get(AGENT_MANAGER_VIP_HTTP_HOST)
                + ":" + conf.get(AGENT_MANAGER_VIP_HTTP_PORT) + conf.get(
                AGENT_MANAGER_VIP_HTTP_PREFIX_PATH,
                DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH);
            managerVipUrl = baseManagerUrl + conf.get(AGENT_MANAGER_VIP_HTTP_PATH,
                DEFAULT_AGENT_TDM_VIP_HTTP_PATH);
            managerTaskUrl = baseManagerUrl + conf.get(AGENT_MANAGER_TASK_HTTP_PATH,
                DEFAULT_AGENT_MANAGER_TASK_HTTP_PATH);
            managerIpsCheckUrl = baseManagerUrl + conf.get(AGENT_MANAGER_IP_CHECK_HTTP_PATH,
                DEFAULT_AGENT_TDM_IP_CHECK_HTTP_PATH);
            uniqId = conf.get(AGENT_UNIQ_ID, DEFAULT_AGENT_UNIQ_ID);
            Path localStorage = Paths.get(conf.get(AGENT_HOME, DEFAULT_AGENT_HOME),
                conf.get(AGENT_LOCAL_CACHE, DEFAULT_AGENT_LOCAL_CACHE), "managerList.txt");
            long timeout = TimeUnit.MINUTES.toMillis(conf.getInt(AGENT_LOCAL_CACHE_TIMEOUT,
                DEFAULT_AGENT_LOCAL_CACHE_TIMEOUT));
            localFileCache = new LocalFileCache(localStorage.toFile(), timeout);
            this.commandDb = agentManager.getCommandDb();
        } else {
            throw new RuntimeException("init manager error, cannot find required key");
        }
    }

    /**
     * for manager to get job profiles
     * @return -  job profile list
     */
    @Override
    public List<JobProfile> getJobProfiles() {
        getTriggerProfiles();
        return null;
    }

    public void requestTdmList() {
        JsonObject result = getResultData(httpManager.doSendGet(managerVipUrl));
        JsonArray data = result.get(AGENT_MANAGER_RETURN_PARAM_DATA).getAsJsonArray();
        List<String> managerIpList = new ArrayList<>();
        for (JsonElement datum : data) {
            JsonObject asJsonObject = datum.getAsJsonObject();
            managerIpList.add(asJsonObject.get(AGENT_MANAGER_RETURN_PARAM_IP).getAsString());
        }
        if (managerIpList.isEmpty()) {
            return;
        }
        localFileCache.writeToCache(String.join(",", managerIpList));
    }

    /**
     * request manager to get commands, make sure it not throwing exceptions
     */
    public void fetchCommand() {
        List<CommandEntity> unackedCommands = commandDb.getUnackedCommands();
        JsonObject resultData = getResultData(httpManager.doSentPost(managerTaskUrl, getFetchRequest(unackedCommands)));
        dealWithFetchResult(GSON.fromJson(resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA).getAsJsonObject(),
            TaskResult.class));
        ackCommands(unackedCommands);
    }

    private void ackCommands(List<CommandEntity> unackedCommands) {
        for (CommandEntity command : unackedCommands) {
            command.setAcked(true);
            commandDb.storeCommand(command);
        }
    }

    /**
     * the fetch command can be normal or special
     * @param taskResult
     */
    private void dealWithFetchResult(TaskResult taskResult) {
        LOGGER.info("deal with fetch result {}", taskResult);
        for (TriggerProfile profile : taskResult.getTriggerProfiles()) {
            dealWithTdmTriggerProfile(profile);
        }
        for (CmdConfig cmdConfig : taskResult.getCmdConfigs()) {
            dealWithTdmCmd(cmdConfig);
        }
    }

    /**
     * form command fetch request
     * @return
     * @param unackedCommands
     */
    public TaskRequestDto getFetchRequest(
        List<CommandEntity> unackedCommands) {
        TaskRequestDto requset = new TaskRequestDto();
        requset.setAgentIp(localIp);
        requset.setCommandInfo(unackedCommands);
        return requset;
    }

    /**
     * get command db
     * @return
     */
    public CommandDb getCommandDb() {
        return commandDb;
    }

    /**
     * deal with special command retry\backtrack
     * @param cmdConfig
     */
    public void dealWithTdmCmd(CmdConfig cmdConfig) {
        Trigger trigger = agentManager.getTriggerManager().getTrigger(
            cmdConfig.getTaskId().toString());
        if (trigger == null) {
            LOGGER.error("trigger {} doesn't exist, cmd is {}",
                cmdConfig.getTaskId(), cmdConfig);
            commandDb.saveSpecialCmds(cmdConfig.getId(),
                cmdConfig.getTaskId(), false);
            return;
        }
        TriggerProfile copiedProfile =
            TriggerProfile.parseJsonStr(trigger.getTriggerProfile().toJsonStr());
        String dataTime = cmdConfig.getDataTime();
        // set job retry time
        copiedProfile.set(JOB_RETRY_TIME, dataTime);
        boolean cmdResult = executeCmd(copiedProfile,
            ManagerOpEnum.getOpType(cmdConfig.getOp()), dataTime);
        commandDb.saveSpecialCmds(cmdConfig.getId(),
            cmdConfig.getTaskId(), cmdResult);
    }

    /**
     * execute commands
     * @param triggerProfile
     * @param opType
     * @param dataTime
     * @return
     */
    private boolean executeCmd(TriggerProfile triggerProfile,
        ManagerOpEnum opType, String dataTime) {
        switch (opType) {
            case RETRY:
            case BACKTRACK:
                return agentManager.getJobManager().submitJobProfile(triggerProfile);
            case MAKEUP:
                return makeUpFiles(triggerProfile, dataTime);
            case CHECK:
                return !PluginUtils.findSuitFiles(triggerProfile).isEmpty();
            default:
        }
        LOGGER.error("do not support such opType {}", opType);
        return false;
    }

    /**
     * when execute make up command, files scanned before should not be executed.
     * @param triggerProfile
     * @param dataTime
     * @return
     */
    private boolean makeUpFiles(TriggerProfile triggerProfile, String dataTime) {
        LOGGER.info("start to make up files with trigger {}, dataTime {}",
            triggerProfile, dataTime);
        Collection<File> suitFiles = PluginUtils.findSuitFiles(triggerProfile);
        // filter files exited before
        List<File> pendingFiles = suitFiles.stream().filter(file ->
            !agentManager.getJobManager().checkJobExsit(file.getAbsolutePath()))
            .collect(Collectors.toList());
        for (File pendingFile : pendingFiles) {
            JobProfile copiedProfile = copyJobProfile(triggerProfile, dataTime,
                pendingFile);
            LOGGER.info("ready to make up file with job {}", copiedProfile.toJsonStr());
            agentManager.getJobManager().submitJobProfile(copiedProfile);
        }
        return true;
    }

    /**
     * the trigger profile returned from manager should be parsed
     * @param triggerProfile
     */
    public void dealWithTdmTriggerProfile(TriggerProfile triggerProfile) {
        ManagerOpEnum opType = ManagerOpEnum.getOpType(triggerProfile.getInt(JOB_OP));
        boolean success = false;
        switch (requireNonNull(opType)) {
            case ACTIVE:
            case ADD:
                success = agentManager.getTriggerManager().submitTrigger(triggerProfile);
                break;
            case DEL:
            case FROZEN:
                success = agentManager.getTriggerManager().deleteTrigger(triggerProfile.getTriggerId());
                break;
            default:
        }
        commandDb.saveNormalCmds(triggerProfile, success);
    }

    /**
     * check agent ip from manager
     */
    private void fetchLocalIp() {
        localIp = AgentConfiguration.getAgentConf().get(AGENT_LOCAL_IP, DEFAULT_LOCAL_IP);
    }

    /**
     * confirm local ips from manager
     * @param localIps
     * @return
     */
    private String confirmLocalIps(List<String> localIps) {
        ConfirmAgentIpRequest request = new ConfirmAgentIpRequest(AGENT, localIps);
        JsonObject resultData = getResultData(httpManager.doSentPost(managerIpsCheckUrl, request))
            .get(AGENT_MANAGER_RETURN_PARAM_DATA).getAsJsonObject();
        if (!resultData.has(AGENT_MANAGER_RETURN_PARAM_IP)) {
            throw new IllegalArgumentException("cannot get ip from data " + resultData.getAsString());
        }
        return resultData.get(AGENT_MANAGER_RETURN_PARAM_IP).getAsString();
    }

    /**
     * fetch manager list, make sure it not throwing exceptions
     * @param isInitial - is initial
     * @param retryTime - retry time
     */
    private void fetchTdmList(boolean isInitial, int retryTime) {
        if (retryTime > MAX_RETRY) {
            return;
        }
        try {
            // check local cache time, make sure cache not timeout
            if (!isInitial && !localFileCache.cacheIsExpired()) {
                String result = localFileCache.getCacheInfo();
                managerList = Arrays.stream(result.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
            } else {
                requestTdmList();
            }
        } catch (Exception ex) {
            fetchTdmList(false, retryTime + 1);
        }
    }

    /**
     * thread for profile fetcher.
     *
     * @return runnable profile.
     */
    private Runnable profileFetchThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    int configSleepTime = conf.getInt(AGENT_FETCHER_INTERVAL,
                        DEFAULT_AGENT_FETCHER_INTERVAL);
                    TimeUnit.SECONDS.sleep(AgentUtils.getRandomBySeed(configSleepTime));
                    // fetch commands from manager
                    fetchCommand();
                    // fetch manager list from vip
                    fetchTdmList(false, 0);
                } catch (Exception ex) {
                    LOGGER.warn("exception caught", ex);
                }
            }
        };
    }

    /**
     * request manager to get trigger profiles.
     * @return - trigger profile list
     */
    @Override
    public List<TriggerProfile> getTriggerProfiles() {
        return null;
    }

    @Override
    public void start() throws Exception {
        // when agent start, check local ip and fetch manager ip list;
        fetchLocalIp();
        fetchTdmList(true, 0);
        submitWorker(profileFetchThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
    }
}

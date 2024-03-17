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

package org.apache.inlong.agent.installer;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.db.CommandEntity;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.common.pojo.agent.installer.ModuleConfig;
import org.apache.inlong.common.pojo.agent.installer.PackageConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_INSTALLER_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.INSTALLER_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.installer.ManagerResultFormatter.getResultData;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalIp;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalUuid;

/**
 * Fetch command from Inlong-Manager
 */
public class ManagerFetcher extends AbstractDaemon implements ProfileFetcher {

    public static final String MANAGER_ADDR = "manager.addr";
    public static final String MANAGER_AUTH_SECRET_ID = "manager.auth.secretId";
    public static final String MANAGER_AUTH_SECRET_KEY = "manager.auth.secretKey";
    public static final String CLUSTER_NAME = "cluster.name";
    public static final String CLUSTER_TAG = "cluster.tag";
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerFetcher.class);
    private static final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = gsonBuilder.create();
    private final String baseManagerUrl;
    private final String staticConfigUrl;
    private final InstallerConfiguration conf;
    private final HttpManager httpManager;
    private final Manager manager;
    private String localIp;
    private String uuid;
    private String clusterName;

    public ManagerFetcher(Manager manager) {
        this.manager = manager;
        this.conf = InstallerConfiguration.getInstallerConf();
        if (requiredKeys(conf)) {
            String managerAddr = conf.get(MANAGER_ADDR);
            String managerHttpPrefixPath = conf.get(AGENT_MANAGER_VIP_HTTP_PREFIX_PATH,
                    DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH);
            int timeout = conf.getInt(AGENT_MANAGER_REQUEST_TIMEOUT,
                    DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT);
            String secretId = conf.get(MANAGER_AUTH_SECRET_ID);
            String secretKey = conf.get(MANAGER_AUTH_SECRET_KEY);
            httpManager = new HttpManager(managerAddr, managerHttpPrefixPath, timeout, secretId, secretKey);
            baseManagerUrl = httpManager.getBaseUrl();
            staticConfigUrl = buildStaticConfigUrl(baseManagerUrl);
            clusterName = conf.get(CLUSTER_NAME);
        } else {
            throw new RuntimeException("init manager error, cannot find required key");
        }
    }

    private boolean requiredKeys(InstallerConfiguration conf) {
        return conf.hasKey(MANAGER_ADDR);
    }

    /**
     * Build config url for manager according to config
     *
     * example - http://127.0.0.1:8080/inlong/manager/openapi/installer/getConfig
     */
    private String buildStaticConfigUrl(String baseUrl) {
        return baseUrl + conf.get(INSTALLER_MANAGER_CONFIG_HTTP_PATH, DEFAULT_INSTALLER_MANAGER_CONFIG_HTTP_PATH);
    }

    /**
     * Request manager to get commands, make sure it is not throwing exceptions
     */
    public ConfigResult getConfig() {
        LOGGER.info("getConfig start");
        String resultStr = httpManager.doSentPost(staticConfigUrl, getFetchRequest(null));
        LOGGER.info("ConfigUrl {}", staticConfigUrl);
        JsonObject resultData = getResultData(resultStr);
        JsonElement dataElement = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        LOGGER.info("getConfig end");
        if (dataElement != null) {
            LOGGER.info("getConfig not null {}", resultData);
            return GSON.fromJson(dataElement.getAsJsonObject(), ConfigResult.class);
        } else {
            LOGGER.error("getConfig result does not contain the data field ");
            return null;
        }
    }

    /**
     * Form file command fetch request
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
     * Thread for config fetcher.
     *
     * @return runnable profile.
     */
    private Runnable configFetchThread() {
        return () -> {
            Thread.currentThread().setName("ManagerFetcher");
            while (isRunnable()) {
                try {
                    int configSleepTime = conf.getInt(AGENT_FETCHER_INTERVAL, DEFAULT_AGENT_FETCHER_INTERVAL);
                    ConfigResult config = getTestConfig();
                    if (config != null) {
                        manager.getModuleManager().submitConfig(config);
                    }
                    TimeUnit.SECONDS.sleep(AgentUtils.getRandomBySeed(configSleepTime));
                } catch (Throwable ex) {
                    LOGGER.warn("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private ConfigResult getTestConfig() {
        List<ModuleConfig> configs = new ArrayList<>();
        configs.add(getTestModuleConfig(1, "inlong-agent", "inlong-agent-md5-185454", "1.0", 1,
                "cd ~/inlong-agent/bin;sh agent.sh start", "cd ~/inlong-agent/bin;sh agent.sh stop",
                "ps aux | grep core.AgentMain | grep java | grep -v grep | awk '{print $2}'",
                "cd ~/inlong-agent/bin;sh agent.sh stop;rm -rf ~/inlong-agent/;mkdir ~/inlong-agent;cd /tmp;tar -xzvf agent-release-1.12.0-SNAPSHOT-bin.tar.gz -C ~/inlong-agent;cd ~/inlong-agent/bin;sh agent.sh start",
                "echo empty uninstall cmd", "agent-release-1.12.0-SNAPSHOT-bin.tar.gz",
                "https://inlong-anager.data.qq.com:8099/api/download?file=agent-release-1.12.0-SNAPSHOT-bin.tar.gz",
                "package-md5-190300"));
        configs.add(getTestModuleConfig(2, "inlong-agent-installer", "inlong-agent-installer-md5-190353", "1.0", 1,
                "ps aux | grep installer | grep -v grep | awk '{print $2}' | xargs kill -9 ;sleep 3;cd ~/inlong-agent-installer/;sh ./bin/monitor.sh",
                "",
                "ps aux | grep installer.Main | grep java | grep -v grep | awk '{print $2}'",
                "tar -xzvf installer-release-1.12.0-SNAPSHOT-bin.tar.gz -C ~/inlong-agent-installer;",
                "echo empty uninstall cmd", "installer-release-1.12.0-SNAPSHOT-bin.tar.gz",
                "https://inlong-anager.data.qq.com:8099/api/download?file=installer-release-1.12.0-SNAPSHOT-bin.tar.gz",
                "package-md5-191132"));
        return ConfigResult.builder().moduleList(configs).moduleNum(2).md5("config-result-md5-193603").build();
    }

    private ModuleConfig getTestModuleConfig(int id, String name, String md5, String version, Integer procNum,
            String startCmd, String stopCmd, String checkCmd, String installCmd, String uninstallCmd, String fileName,
            String downloadUrl,
            String packageMd5) {
        ModuleConfig moduleConfig = new ModuleConfig();
        moduleConfig.setId(id);
        moduleConfig.setName(name);
        moduleConfig.setVersion(version);
        moduleConfig.setMd5(md5);
        moduleConfig.setProcessesNum(procNum);
        moduleConfig.setStartCommand(startCmd);
        moduleConfig.setStopCommand(stopCmd);
        moduleConfig.setCheckCommand(checkCmd);
        moduleConfig.setInstallCommand(installCmd);
        moduleConfig.setUninstallCommand(uninstallCmd);
        PackageConfig packageConfig = new PackageConfig();
        packageConfig.setFileName(fileName);
        packageConfig.setDownloadUrl(downloadUrl);
        packageConfig.setMd5(packageMd5);
        moduleConfig.setPackageConfig(packageConfig);
        return moduleConfig;
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

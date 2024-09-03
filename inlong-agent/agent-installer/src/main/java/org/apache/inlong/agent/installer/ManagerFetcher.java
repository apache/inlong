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
import org.apache.inlong.common.pojo.agent.AgentResponseCode;
import org.apache.inlong.common.pojo.agent.installer.ConfigRequest;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_FETCHER_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_INSTALLER_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.INSTALLER_MANAGER_CONFIG_HTTP_PATH;
import static org.apache.inlong.agent.installer.ManagerResultFormatter.getResultData;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalUuid;

/**
 * Fetch command from Inlong-Manager
 */
public class ManagerFetcher extends AbstractDaemon implements ProfileFetcher {

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
    private String clusterTag;
    private String clusterName;

    public ManagerFetcher(Manager manager) {
        this.manager = manager;
        this.conf = InstallerConfiguration.getInstallerConf();
        httpManager = manager.getModuleManager().getHttpManager(conf);
        baseManagerUrl = httpManager.getBaseUrl();
        staticConfigUrl = buildStaticConfigUrl(baseManagerUrl);
        clusterTag = conf.get(AGENT_CLUSTER_TAG);
        clusterName = conf.get(AGENT_CLUSTER_NAME);
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
        String resultStr = httpManager.doSentPost(staticConfigUrl, getFetchRequest());
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
    public ConfigRequest getFetchRequest() {
        ConfigRequest request = new ConfigRequest();
        request.setClusterTag(clusterTag);
        request.setClusterName(clusterName);
        request.setLocalIp(localIp);
        request.setMd5(manager.getModuleManager().getCurrentMd5());
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
                    ConfigResult config = getConfig();
                    if (config != null && config.getCode().equals(AgentResponseCode.SUCCESS)
                            && manager.getModuleManager().getCurrentVersion() < config.getVersion()) {
                        manager.getModuleManager().submitConfig(config);
                    }
                } catch (Throwable ex) {
                    LOGGER.warn("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                } finally {
                    AgentUtils
                            .silenceSleepInSeconds(conf.getInt(AGENT_FETCHER_INTERVAL, DEFAULT_AGENT_FETCHER_INTERVAL));
                }
            }
        };
    }

    @Override
    public void start() throws Exception {
        // when agent start, check local ip and fetch manager ip list;
        localIp = conf.get(AGENT_LOCAL_IP);
        uuid = fetchLocalUuid();
        submitWorker(configFetchThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
    }
}

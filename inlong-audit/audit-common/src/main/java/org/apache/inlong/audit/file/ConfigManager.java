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

package org.apache.inlong.audit.file;

import org.apache.inlong.audit.consts.ConfigConstants;
import org.apache.inlong.audit.file.holder.PropertiesConfigHolder;
import org.apache.inlong.common.pojo.audit.AuditConfigRequest;
import org.apache.inlong.common.pojo.audit.MQInfo;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);

    private static final Map<String, ConfigHolder> holderMap =
            new ConcurrentHashMap<>();

    public static List<MQInfo> mqInfoList = new ArrayList<>();

    private static ConfigManager instance = null;

    private static String DEFAULT_CONFIG_PROPERTIES = "application.properties";

    static {
        instance = getInstance(DEFAULT_CONFIG_PROPERTIES, true);
        ReloadConfigWorker reloadProperties = new ReloadConfigWorker(instance);
        reloadProperties.setDaemon(true);
        reloadProperties.start();
    }

    public static ConfigManager getInstance() {
        return instance;
    }

    /**
     * get instance for manager
     *
     * @return
     */
    public static ConfigManager getInstance(String fileName, boolean needToCheckChanged) {
        synchronized (ConfigManager.class) {
            if (instance == null) {
                instance = new ConfigManager();
            }
            ConfigHolder holder = holderMap.get(fileName);
            if (holder == null) {
                holder = new PropertiesConfigHolder(fileName, needToCheckChanged);
                holder.loadFromFileToHolder();
                holderMap.putIfAbsent(fileName, holder);
            }
        }
        return instance;
    }

    public Map<String, String> getProperties(String fileName) {
        ConfigHolder holder = holderMap.get(fileName);
        if (holder != null) {
            return holder.getHolder();
        }
        return null;
    }

    public <T> T getValue(String key, T defaultValue, Function<String, T> parser) {
        ConfigHolder holder = holderMap.get(DEFAULT_CONFIG_PROPERTIES);
        if (holder == null) {
            return defaultValue;
        }
        Object value = holder.getHolder().get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return parser.apply((String) value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public String getValue(String key, String defaultValue) {
        return getValue(key, defaultValue, Function.identity());
    }

    public int getValue(String key, int defaultValue) {
        return getValue(key, defaultValue, Integer::parseInt);
    }

    private boolean updatePropertiesHolder(Map<String, String> result,
            String holderName, boolean addElseRemove) {
        if (StringUtils.isNotEmpty(holderName)) {
            PropertiesConfigHolder holder = (PropertiesConfigHolder) holderMap.get(holderName + ".properties");
            return updatePropertiesHolder(result, holder, true);
        }
        return true;
    }

    /**
     * update old maps, reload local files if changed.
     *
     * @param result - map pending to be added
     * @param holder - property holder
     * @param addElseRemove - if add(true) else remove(false)
     * @return true if changed else false.
     */
    private boolean updatePropertiesHolder(Map<String, String> result,
            PropertiesConfigHolder holder, boolean addElseRemove) {
        Map<String, String> tmpHolder = holder.forkHolder();
        boolean changed = false;
        for (Entry<String, String> entry : result.entrySet()) {
            String oldValue = addElseRemove
                    ? tmpHolder.put(entry.getKey(), entry.getValue())
                    : tmpHolder.remove(entry.getKey());
            // if addElseRemove is false, that means removing item, changed is true.
            if (oldValue == null || !oldValue.equals(entry.getValue()) || !addElseRemove) {
                changed = true;
            }
        }

        if (changed) {
            return holder.loadFromHolderToFile(tmpHolder);
        } else {
            return false;
        }
    }

    public List<MQInfo> getMqInfoList() {
        return mqInfoList;
    }

    public ConfigHolder getDefaultConfigHolder() {
        return holderMap.get(DEFAULT_CONFIG_PROPERTIES);
    }

    public ConfigHolder getConfigHolder(String fileName) {
        return holderMap.get(fileName);
    }

    /**
     * load worker
     */
    private static class ReloadConfigWorker extends Thread {

        private static final Logger LOG = LoggerFactory.getLogger(ReloadConfigWorker.class);
        private final ConfigManager configManager;
        private final CloseableHttpClient httpClient;
        private final Gson gson = new Gson();
        private boolean isRunning = true;

        public ReloadConfigWorker(ConfigManager managerInstance) {
            this.configManager = managerInstance;
            this.httpClient = constructHttpClient();
        }

        private synchronized CloseableHttpClient constructHttpClient() {
            long timeoutInMs = TimeUnit.MILLISECONDS.toMillis(50000);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout((int) timeoutInMs)
                    .setSocketTimeout((int) timeoutInMs).build();
            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
            httpClientBuilder.setDefaultRequestConfig(requestConfig);
            return httpClientBuilder.build();
        }

        public int getRandom(int min, int max) {
            return (int) (Math.random() * (max + 1 - min)) + min;
        }

        private long getSleepTime() {
            String sleepTimeInMsStr =
                    configManager.getProperties(DEFAULT_CONFIG_PROPERTIES).get(
                            "configCheckIntervalMs");
            long sleepTimeInMs = 10000;
            try {
                if (sleepTimeInMsStr != null) {
                    sleepTimeInMs = Long.parseLong(sleepTimeInMsStr);
                }
            } catch (Exception ignored) {
                LOG.info("ignored Exception ", ignored);
            }
            return sleepTimeInMs + getRandom(0, 5000);
        }

        public void close() {
            isRunning = false;
        }

        private void checkLocalFile() {
            for (ConfigHolder holder : holderMap.values()) {
                boolean isChanged = holder.checkAndUpdateHolder();
                if (isChanged) {
                    holder.executeCallbacks();
                }
            }
        }

        private boolean checkWithManager(String host, String clusterTag) {
            HttpPost httpPost = null;
            try {
                String url = "http://" + host + ConfigConstants.MANAGER_PATH + ConfigConstants.MANAGER_GET_CONFIG_PATH;
                LOG.info("start to request {} to get config info", url);
                httpPost = new HttpPost(url);
                httpPost.addHeader(HttpHeaders.CONNECTION, "close");

                // request body
                AuditConfigRequest request = new AuditConfigRequest();
                request.setClusterTag(clusterTag);
                StringEntity stringEntity = new StringEntity(gson.toJson(request));
                stringEntity.setContentType("application/json");
                httpPost.setEntity(stringEntity);

                // request with post
                LOG.info("start to request {} to get config info with params {}", url, request);
                CloseableHttpResponse response = httpClient.execute(httpPost);
                String returnStr = EntityUtils.toString(response.getEntity());
                // get groupId <-> topic and m value.

                RemoteConfigJson configJson = gson.fromJson(returnStr, RemoteConfigJson.class);
                if (configJson.isSuccess() && configJson.getData() != null) {
                    mqInfoList = configJson.getData().getMqInfoList();
                    if (mqInfoList == null || mqInfoList.isEmpty()) {
                        LOG.error("getConfig from manager: no available mq config");
                        return false;
                    }
                }
            } catch (Exception ex) {
                LOG.error("exception caught", ex);
                return false;
            } finally {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            }
            return true;
        }

        private void checkRemoteConfig() {

            try {
                String managerHosts = configManager.getProperties(DEFAULT_CONFIG_PROPERTIES).get("manager.hosts");
                String proxyClusterTag = configManager.getProperties(DEFAULT_CONFIG_PROPERTIES)
                        .get("default.mq.cluster.tag");
                LOG.info("manager url: {}", managerHosts);
                String[] hostList = StringUtils.split(managerHosts, ",");
                for (String host : hostList) {
                    if (checkWithManager(host, proxyClusterTag)) {
                        break;
                    }
                }
            } catch (Exception ex) {
                LOG.error("exception caught", ex);
            }
        }

        @Override
        public void run() {
            long count = 0;
            while (isRunning) {

                long sleepTimeInMs = getSleepTime();
                try {
                    checkLocalFile();
                    // wait for 30 seconds to update remote config
                    if (count % 3 == 0) {
                        checkRemoteConfig();
                        count = 0;
                    }
                    TimeUnit.MILLISECONDS.sleep(sleepTimeInMs);
                } catch (Exception ex) {
                    LOG.error("exception caught", ex);
                }
                count += 1;
            }
        }
    }
}

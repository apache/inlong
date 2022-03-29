/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.config;

import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.common.pojo.dataproxy.CacheClusterObject;
import org.apache.inlong.common.pojo.dataproxy.CacheClusterSetObject;
import org.apache.inlong.common.pojo.dataproxy.CacheTopicObject;
import org.apache.inlong.common.pojo.dataproxy.DataProxyCluster;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.common.pojo.dataproxy.IRepository;
import org.apache.inlong.common.pojo.dataproxy.InLongIdObject;
import org.apache.inlong.common.pojo.dataproxy.ProxyChannel;
import org.apache.inlong.common.pojo.dataproxy.ProxyClusterObject;
import org.apache.inlong.common.pojo.dataproxy.ProxySink;
import org.apache.inlong.common.pojo.dataproxy.ProxySource;
import org.apache.inlong.common.pojo.dataproxy.RepositoryTimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * RemoteConfigManager
 */
public class RemoteConfigManager implements IRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteConfigManager.class);
    public static final String KEY_CONFIG_CHECK_INTERVAL = "configCheckInterval";
    public static final String KEY_PROXY_CLUSTER_NAME = "proxy.cluster.name";
    public static final String KEY_SET_NAME = "set.name";
    public static final char FLUME_SEPARATOR = '.';
    private static volatile boolean isInit = false;
    private static RemoteConfigManager instance = null;

    private long reloadInterval;
    private Timer reloadTimer;

    //
    private IManagerIpListParser ipListParser;
    private CloseableHttpClient httpClient;
    private Gson gson = new Gson();
    private AtomicInteger managerIpListIndex = new AtomicInteger(0);
    // config
    private String dataProxyConfigMd5;
    private AtomicReference<DataProxyCluster> currentClusterConfigRef = new AtomicReference<>();
    // flume properties
    private Map<String, String> flumeProperties;
    // inlong id map
    private Map<String, InLongIdObject> inlongIdMap;

    private RemoteConfigManager() {
    }

    /**
     * get instance for manager
     * 
     * @return RemoteConfigManager
     */
    @SuppressWarnings("unchecked")
    public static RemoteConfigManager getInstance() {
        LOGGER.info("create repository for {}" + RemoteConfigManager.class.getSimpleName());
        if (isInit && instance != null) {
            return instance;
        }
        synchronized (RemoteConfigManager.class) {
            if (!isInit) {
                instance = new RemoteConfigManager();
                try {
                    String strReloadInterval = ConfigManager.getInstance().getCommonProperties()
                            .get(KEY_CONFIG_CHECK_INTERVAL);
                    instance.reloadInterval = NumberUtils.toLong(strReloadInterval, DEFAULT_HEARTBEAT_INTERVAL_MS);
                    //
                    String ipListParserType = ConfigManager.getInstance().getCommonProperties()
                            .get(IManagerIpListParser.KEY_MANAGER_TYPE);
                    Class<? extends IManagerIpListParser> ipListParserClass;
                    ipListParserClass = (Class<? extends IManagerIpListParser>) Class
                            .forName(ipListParserType);
                    instance.ipListParser = ipListParserClass.getDeclaredConstructor().newInstance();
                    //
                    SecureRandom random = new SecureRandom(String.valueOf(System.currentTimeMillis()).getBytes());
                    instance.managerIpListIndex.set(random.nextInt());
                    //
                    instance.httpClient = constructHttpClient();
                    //
                    instance.reload();
                    instance.setReloadTimer();
                    isInit = true;
                } catch (Throwable t) {
                    LOGGER.error(t.getMessage(), t);
                }
            }
        }
        return instance;
    }

    /**
     * reload
     */
    public void reload() {
        LOGGER.info("start to reload config.");
        String proxyClusterName = ConfigManager.getInstance().getCommonProperties().get(KEY_PROXY_CLUSTER_NAME);
        String setName = ConfigManager.getInstance().getCommonProperties().get(KEY_SET_NAME);
        if (StringUtils.isBlank(proxyClusterName) || StringUtils.isBlank(setName)) {
            return;
        }
        //
        this.ipListParser.setCommonProperties(ConfigManager.getInstance().getCommonProperties());
        List<String> managerIpList = this.ipListParser.getIpList();
        if (managerIpList == null || managerIpList.size() == 0) {
            return;
        }
        int managerIpSize = managerIpList.size();
        for (int i = 0; i < managerIpList.size(); i++) {
            String host = managerIpList.get(managerIpListIndex.getAndIncrement() % managerIpSize);
            if (this.reloadDataProxyConfig(proxyClusterName, setName, host)) {
                break;
            }
        }

        LOGGER.info("end to reload config.");
    }

    /**
     * setReloadTimer
     */
    private void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new RepositoryTimerTask<RemoteConfigManager>(this);
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    /**
     * reloadDataProxyConfig
     * 
     * @param  host
     * @return
     */
    private boolean reloadDataProxyConfig(String proxyClusterName, String setName, String host) {
        HttpGet httpGet = null;
        try {
            String url = "http://" + host + "/api/inlong/manager/openapi/dataproxy/getAllConfig?clusterName="
                    + proxyClusterName + "&setName=" + setName;
            if (StringUtils.isNotBlank(this.dataProxyConfigMd5)) {
                url += "&md5=" + this.dataProxyConfigMd5;
            }
            LOGGER.info("start to request {} to get config info", url);
            httpGet = new HttpGet(url);
            httpGet.addHeader(HttpHeaders.CONNECTION, "close");

            // request with get
            CloseableHttpResponse response = httpClient.execute(httpGet);
            String returnStr = EntityUtils.toString(response.getEntity());
            LOGGER.info("end to request {} to get config info:{}", url, returnStr);
            // get groupId <-> topic and m value.

            DataProxyConfigResponse proxyResponse = gson.fromJson(returnStr, DataProxyConfigResponse.class);
            if (!proxyResponse.isResult()) {
                LOGGER.info("Fail to get config info from url:{}, error code is {}", url, proxyResponse.getErrCode());
                return false;
            }
            if (proxyResponse.getErrCode() != DataProxyConfigResponse.SUCC) {
                LOGGER.info("get config info from url:{}, error code is {}", url, proxyResponse.getErrCode());
                return true;
            }

            this.dataProxyConfigMd5 = proxyResponse.getMd5();
            DataProxyCluster clusterObj = proxyResponse.getData();
            this.currentClusterConfigRef.set(clusterObj);
            // parse inlong id
            this.parseInlongIds();
            // generate flume properties
            this.generateFlumeProperties();
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
            return false;
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return true;
    }

    /**
     * constructHttpClient
     * 
     * @return
     */
    private static synchronized CloseableHttpClient constructHttpClient() {
        long timeoutInMs = TimeUnit.MILLISECONDS.toMillis(50000);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout((int) timeoutInMs)
                .setSocketTimeout((int) timeoutInMs).build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        return httpClientBuilder.build();
    }

    /**
     * getZone
     * 
     * @return
     */
    public String getZone() {
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        if (currentClusterConfig != null) {
            return currentClusterConfig.getProxyCluster().getZone();
        }
        return null;
    }

    /**
     * getProxyClusterName
     * 
     * @return
     */
    public String getProxyClusterName() {
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        if (currentClusterConfig != null) {
            return currentClusterConfig.getProxyCluster().getName();
        }
        return ConfigManager.getInstance().getCommonProperties().get(KEY_PROXY_CLUSTER_NAME);
    }

    /**
     * getProxyClusterName
     * 
     * @return
     */
    public String getSetName() {
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        if (currentClusterConfig != null) {
            return currentClusterConfig.getProxyCluster().getSetName();
        }
        return ConfigManager.getInstance().getCommonProperties().get(KEY_SET_NAME);
    }

    /**
     * parseInlongIds
     */
    private void parseInlongIds() {
        Map<String, InLongIdObject> newConfig = new HashMap<>();
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        ProxyClusterObject proxyClusterObject = currentClusterConfig.getProxyCluster();
        for (InLongIdObject obj : proxyClusterObject.getInlongIds()) {
            String inlongId = obj.getInlongId();
            newConfig.put(inlongId, obj);
        }
        this.inlongIdMap = newConfig;
    }

    /**
     * 
     * generateFlumeProperties
     */
    private void generateFlumeProperties() {
        Map<String, String> newConfig = new HashMap<>();
        // channels
        this.generateFlumeChannels(newConfig);
        // sinks
        this.generateFlumeSinks(newConfig);
        // sources
        this.generateFlumeSources(newConfig);
        //
        this.flumeProperties = newConfig;
    }

    /**
     * generateFlumeChannels
     * 
     * @param proxyClusterObject
     * @param newConfig
     */
    private void generateFlumeChannels(Map<String, String> newConfig) {
        StringBuilder builder = new StringBuilder();
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        ProxyClusterObject proxyClusterObject = currentClusterConfig.getProxyCluster();
        String proxyClusterName = proxyClusterObject.getName();
        // channels
        // ${proxyClusterName}.channels.${channelName}.type=xxx
        // ${proxyClusterName}.channels.${channelName}.${paramName}=${paramValue}
        for (ProxyChannel channel : proxyClusterObject.getChannels()) {
            builder.setLength(0);
            builder.append(proxyClusterName).append(".channels.").append(channel.getName()).append(FLUME_SEPARATOR);
            String prefix = builder.toString();
            builder.append("type");
            newConfig.put(builder.toString(), channel.getType());
            for (Entry<String, String> entry : channel.getParams().entrySet()) {
                builder.setLength(0);
                builder.append(prefix).append(entry.getKey());
                newConfig.put(builder.toString(), entry.getValue());
            }
        }
        // summary
        builder.setLength(0);
        builder.append(proxyClusterName).append(".channels");
        String key = builder.toString();
        builder.setLength(0);
        proxyClusterObject.getChannels().forEach((channel) -> {
            builder.append(channel.getName()).append(" ");
        });
        if (builder.length() > 0) {
            newConfig.put(key, builder.substring(0, builder.length() - 1));
        }
    }

    /**
     * generateFlumeSink
     * 
     * @param proxyClusterObject
     * @param newConfig
     */
    private void generateFlumeSinks(Map<String, String> newConfig) {
        StringBuilder builder = new StringBuilder();
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        ProxyClusterObject proxyClusterObject = currentClusterConfig.getProxyCluster();
        String proxyClusterName = proxyClusterObject.getName();
        // sinks
        // ${proxyClusterName}.sinks.${sinkName}.channel=xxx
        // ${proxyClusterName}.sinks.${sinkName}.type=xxx
        // ${proxyClusterName}.sinks.${sinkName}.${paramName}=${paramValue}
        for (ProxySink sink : proxyClusterObject.getSinks()) {
            builder.setLength(0);
            builder.append(proxyClusterName).append(".sinks.").append(sink.getName()).append(FLUME_SEPARATOR);
            String prefix = builder.toString();
            builder.setLength(0);
            builder.append(prefix).append("channel");
            newConfig.put(builder.toString(), sink.getChannel());
            builder.setLength(0);
            builder.append(prefix).append("type");
            newConfig.put(builder.toString(), sink.getType());
            for (Entry<String, String> entry : sink.getParams().entrySet()) {
                builder.setLength(0);
                builder.append(prefix).append(entry.getKey());
                newConfig.put(builder.toString(), entry.getValue());
            }
            // ${proxyClusterName}.sinks.${sinkName}.cache.type=Pulsar
            builder.setLength(0);
            builder.append(prefix).append("cache.type");
            CacheClusterSetObject cacheSetObject = currentClusterConfig.getCacheClusterSet();
            newConfig.put(builder.toString(), cacheSetObject.getType());
            // ${proxyClusterName}.sinks.${sinkName}.cache.topics.${topic}.partitionNum=xxx
            builder.setLength(0);
            builder.append(prefix).append("cache.topics").append(FLUME_SEPARATOR);
            String topicPrefix = builder.toString();
            for (CacheTopicObject topicObject : cacheSetObject.getTopics()) {
                builder.setLength(0);
                builder.append(topicPrefix).append(topicObject.getTopic()).append(".partitionNum");
                newConfig.put(builder.toString(), String.valueOf(topicObject.getPartitionNum()));
            }
            // ${proxyClusterName}.sinks.${sinkName}.cache.clusters.${cacheProxyName}.zone=xxx
            // ${proxyClusterName}.sinks.${sinkName}.cache.clusters.${cacheProxyName}.${paramName}=${paramValue}
            for (CacheClusterObject cacheClusterObject : cacheSetObject.getCacheClusters()) {
                builder.setLength(0);
                builder.append(prefix).append("cache.clusters.").append(cacheClusterObject.getName())
                        .append(FLUME_SEPARATOR);
                String cachePrefix = builder.toString();
                builder.append("zone");
                newConfig.put(builder.toString(), cacheClusterObject.getZone());
                for (Entry<String, String> entry : sink.getParams().entrySet()) {
                    builder.setLength(0);
                    builder.append(cachePrefix).append(entry.getKey());
                    newConfig.put(builder.toString(), entry.getValue());
                }
            }
        }
        // summary
        builder.setLength(0);
        builder.append(proxyClusterName).append(".sinks");
        String key = builder.toString();
        builder.setLength(0);
        proxyClusterObject.getSinks().forEach((sink) -> {
            builder.append(sink.getName()).append(" ");
        });
        if (builder.length() > 0) {
            newConfig.put(key, builder.substring(0, builder.length() - 1));
        }
    }

    /**
     * generateFlumeSources
     * 
     * @param proxyClusterObject
     * @param newConfig
     */
    private void generateFlumeSources(Map<String, String> newConfig) {
        StringBuilder builder = new StringBuilder();
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        ProxyClusterObject proxyClusterObject = currentClusterConfig.getProxyCluster();
        String proxyClusterName = proxyClusterObject.getName();
        // sources
        // ${proxyClusterName}.sources.${sourceName}.channels=xxx xxx xxx
        // ${proxyClusterName}.sources.${sourceName}.type=xxx
        // ${proxyClusterName}.sources.${sourceName}.selector.type=xxx
        // ${proxyClusterName}.sources.${sourceName}.${paramName}=${paramValue}
        for (ProxySource source : proxyClusterObject.getSources()) {
            builder.setLength(0);
            builder.append(proxyClusterName).append(".sources.").append(source.getName()).append(FLUME_SEPARATOR);
            String prefix = builder.toString();
            builder.setLength(0);
            builder.append(prefix).append("channels");
            String channelsKey = builder.toString();
            builder.setLength(0);
            for (String channel : source.getChannels()) {
                builder.append(channel).append(" ");
            }
            String channelsValue = builder.toString().trim();
            newConfig.put(channelsKey, channelsValue);
            builder.setLength(0);
            builder.append(prefix).append("type");
            newConfig.put(builder.toString(), source.getType());
            builder.setLength(0);
            builder.append(prefix).append("selector.type");
            newConfig.put(builder.toString(), source.getSelectorType());
            for (Entry<String, String> entry : source.getParams().entrySet()) {
                builder.setLength(0);
                builder.append(prefix).append(entry.getKey());
                newConfig.put(builder.toString(), entry.getValue());
            }
        }
        // summary
        builder.setLength(0);
        builder.append(proxyClusterName).append(".sources");
        String key = builder.toString();
        builder.setLength(0);
        proxyClusterObject.getSources().forEach((source) -> {
            builder.append(source.getName()).append(" ");
        });
        if (builder.length() > 0) {
            newConfig.put(key, builder.substring(0, builder.length() - 1));
        }
    }

    /**
     * getFlumeProperties
     * 
     * @return
     */
    public Map<String, String> getFlumeProperties() {
        return flumeProperties;
    }

    /**
     * getInlongIdMap
     * 
     * @return
     */
    public Map<String, InLongIdObject> getInlongIdMap() {
        return inlongIdMap;
    }

    /**
     * 
     * getCurrentClusterConfig
     * 
     * @return
     */
    public DataProxyCluster getCurrentClusterConfig() {
        DataProxyCluster currentClusterConfig = currentClusterConfigRef.get();
        return currentClusterConfig;
    }

    /**
     * get currentClusterConfigRef
     * 
     * @return the currentClusterConfigRef
     */
    public AtomicReference<DataProxyCluster> getCurrentClusterConfigRef() {
        return currentClusterConfigRef;
    }

}

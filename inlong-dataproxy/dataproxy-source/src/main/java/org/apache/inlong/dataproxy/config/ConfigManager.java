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

package org.apache.inlong.dataproxy.config;

import org.apache.inlong.common.constant.Constants;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.enums.InlongCompressType;
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.heartbeat.AddressInfo;
import org.apache.inlong.common.pojo.dataproxy.CacheClusterObject;
import org.apache.inlong.common.pojo.dataproxy.CacheClusterSetObject;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigRequest;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.common.pojo.dataproxy.InLongIdObject;
import org.apache.inlong.common.pojo.dataproxy.ProxyClusterObject;
import org.apache.inlong.dataproxy.config.holder.BlackListConfigHolder;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.config.holder.GroupIdNumConfigHolder;
import org.apache.inlong.dataproxy.config.holder.MetaConfigHolder;
import org.apache.inlong.dataproxy.config.holder.SourceReportConfigHolder;
import org.apache.inlong.dataproxy.config.holder.WeightConfigHolder;
import org.apache.inlong.dataproxy.config.holder.WhiteListConfigHolder;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.CacheType;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.config.pojo.InLongMetaConfig;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.utils.HttpUtils;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Config manager class.
 */
public class ConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);

    public static final Map<ConfigHolder, Long> CONFIG_HOLDER_MAP = new ConcurrentHashMap<>();
    // whether handshake manager ok
    public static final AtomicBoolean handshakeManagerOk = new AtomicBoolean(false);
    private static volatile boolean isInit = false;
    private static ConfigManager instance = null;
    // node weight configure
    private final WeightConfigHolder weightConfigHolder = new WeightConfigHolder();
    // black list configure
    private final BlackListConfigHolder blacklistConfigHolder = new BlackListConfigHolder();
    // whitelist configure
    private final WhiteListConfigHolder whitelistConfigHolder = new WhiteListConfigHolder();
    // group id num 2 name configure
    private final GroupIdNumConfigHolder groupIdConfig = new GroupIdNumConfigHolder();
    // mq configure and topic configure holder
    private final MetaConfigHolder metaConfigHolder = new MetaConfigHolder();
    // source report configure holder
    private final SourceReportConfigHolder sourceReportConfigHolder = new SourceReportConfigHolder();
    // mq clusters ready
    private volatile boolean mqClusterReady = false;

    /**
     * get instance for config manager
     */
    public static ConfigManager getInstance() {
        if (isInit && instance != null) {
            return instance;
        }
        synchronized (ConfigManager.class) {
            if (!isInit) {
                instance = new ConfigManager();
                for (ConfigHolder holder : CONFIG_HOLDER_MAP.keySet()) {
                    holder.loadFromFileToHolder();
                }
                ReloadConfigWorker reloadProperties = ReloadConfigWorker.create(instance);
                reloadProperties.setDaemon(true);
                reloadProperties.start();
                isInit = true;
            }
        }
        return instance;
    }

    // get node weight configure
    public double getCpuWeight() {
        return weightConfigHolder.getCachedCpuWeight();
    }

    public double getNetInWeight() {
        return weightConfigHolder.getCachedNetInWeight();
    }

    public double getNetOutWeight() {
        return weightConfigHolder.getCachedNetOutWeight();
    }

    public double getTcpWeight() {
        return weightConfigHolder.getCachedTcpWeight();
    }

    public double getCpuThresholdWeight() {
        return weightConfigHolder.getCachedCpuThreshold();
    }

    /**
     * get source topic by groupId and streamId
     */
    public String getTopicName(String groupId, String streamId) {
        return metaConfigHolder.getSrcBaseTopicName(groupId, streamId);
    }

    /**
     * get sink topic configure by groupId and streamId
     */
    public IdTopicConfig getSinkIdTopicConfig(String groupId, String streamId) {
        return metaConfigHolder.getSinkIdTopicConfig(groupId, streamId);
    }

    public String getMetaConfigMD5() {
        return metaConfigHolder.getConfigMd5();
    }

    public boolean updateMetaConfigInfo(InLongMetaConfig metaConfig) {
        return metaConfigHolder.updateConfigMap(metaConfig);
    }

    // register meta-config callback
    public void regMetaConfigChgCallback(ConfigUpdateCallback callback) {
        metaConfigHolder.addUpdateCallback(callback);
    }

    public List<CacheClusterConfig> getCachedCLusterConfig() {
        return metaConfigHolder.forkCachedCLusterConfig();
    }

    public Set<String> getAllTopicNames() {
        return metaConfigHolder.getAllTopicName();
    }

    // get groupId num 2 name info
    public boolean isEnableNum2NameTrans(String groupIdNum) {
        return groupIdConfig.isEnableNum2NameTrans(groupIdNum);
    }

    public boolean isGroupIdNumConfigEmpty() {
        return groupIdConfig.isGroupIdNumConfigEmpty();
    }

    public boolean isStreamIdNumConfigEmpty() {
        return groupIdConfig.isStreamIdNumConfigEmpty();
    }

    public String getGroupIdNameByNum(String groupIdNum) {
        return groupIdConfig.getGroupIdNameByNum(groupIdNum);
    }

    public String getStreamIdNameByIdNum(String groupIdNum, String streamIdNum) {
        return groupIdConfig.getStreamIdNameByIdNum(groupIdNum, streamIdNum);
    }

    public ConcurrentHashMap<String, String> getGroupIdNumMap() {
        return groupIdConfig.getGroupIdNumMap();
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, String>> getStreamIdNumMap() {
        return groupIdConfig.getStreamIdNumMap();
    }

    // get blacklist whitelist configure info
    public void regIPVisitConfigChgCallback(ConfigUpdateCallback callback) {
        blacklistConfigHolder.addUpdateCallback(callback);
        whitelistConfigHolder.addUpdateCallback(callback);
    }

    public boolean needChkIllegalIP() {
        return (blacklistConfigHolder.needCheckBlacklist()
                || whitelistConfigHolder.needCheckWhitelist());
    }

    public boolean isIllegalIP(String strRemoteIP) {
        return strRemoteIP == null
                || blacklistConfigHolder.isIllegalIP(strRemoteIP)
                || whitelistConfigHolder.isIllegalIP(strRemoteIP);
    }

    public void addSourceReportInfo(String sourceIp,
            String sourcePort, String rptSrcType, String protocolType) {
        sourceReportConfigHolder.addSourceInfo(sourceIp, sourcePort, rptSrcType, protocolType);
    }

    public Map<String, AddressInfo> getSrcAddressInfos() {
        return sourceReportConfigHolder.getSrcAddressInfos();
    }

    public boolean isMqClusterReady() {
        return mqClusterReady;
    }

    public void setMqClusterReady() {
        mqClusterReady = true;
    }

    /**
     * load worker
     */
    public static class ReloadConfigWorker extends Thread {

        private final ConfigManager configManager;
        private final CloseableHttpClient httpClient;
        private final Gson gson = new Gson();
        private boolean isRunning = true;
        private final AtomicInteger managerIpListIndex = new AtomicInteger(0);

        private ReloadConfigWorker(ConfigManager managerInstance) {
            this.configManager = managerInstance;
            this.httpClient = constructHttpClient();
            SecureRandom random = new SecureRandom(String.valueOf(System.currentTimeMillis()).getBytes());
            managerIpListIndex.set(random.nextInt());
        }

        public static ReloadConfigWorker create(ConfigManager managerInstance) {
            return new ReloadConfigWorker(managerInstance);
        }

        @Override
        public void run() {
            long count = 0;
            long startTime;
            long wstTime;
            LOG.info("Reload-Config Worker started!");
            while (isRunning) {
                startTime = System.currentTimeMillis();
                try {
                    // check and load local configure files
                    for (ConfigHolder holder : CONFIG_HOLDER_MAP.keySet()) {
                        if (holder.checkAndUpdateHolder()) {
                            holder.executeCallbacks();
                        }
                    }
                    // connect to manager for updating remote config
                    if (count % 3 == 0) {
                        checkRemoteConfig();
                    }
                    // check processing time
                    wstTime = System.currentTimeMillis() - startTime;
                    if (wstTime > 60000L) {
                        LOG.warn("Reload-Config Worker process wast({}) over 60000 millis", wstTime);
                    }
                    // sleep for next check
                    TimeUnit.MILLISECONDS.sleep(
                            CommonConfigHolder.getInstance().getMetaConfigSyncInvlMs() + getRandom(0, 5000));
                } catch (InterruptedException ex1) {
                    LOG.error("Reload-Config Worker encounters an interrupt exception, break processing", ex1);
                    break;
                } catch (Throwable ex2) {
                    LOG.error("Reload-Config Worker encounters exception, continue process", ex2);
                } finally {
                    count++;
                }
            }
            LOG.info("Reload-Config Worker existed!");
        }

        public void close() {
            isRunning = false;
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

        private int getRandom(int min, int max) {
            return (int) (Math.random() * (max + 1 - min)) + min;
        }

        private void checkRemoteConfig() {
            List<String> managerIpList = CommonConfigHolder.getInstance().getManagerHosts();
            int managerIpSize = managerIpList.size();
            for (int i = 0; i < managerIpList.size(); i++) {
                String host = managerIpList.get(Math.abs(managerIpListIndex.getAndIncrement()) % managerIpSize);
                if (this.reloadDataProxyConfig(CommonConfigHolder.getInstance().getClusterName(),
                        CommonConfigHolder.getInstance().getClusterTag(), host)) {
                    break;
                }
            }
        }

        /**
         * reloadDataProxyConfig
         */
        private boolean reloadDataProxyConfig(String clusterName, String clusterTag, String host) {
            String url = null;
            HttpPost httpPost = null;
            try {
                url = "http://" + host + ConfigConstants.MANAGER_PATH
                        + ConfigConstants.MANAGER_GET_ALL_CONFIG_PATH;
                httpPost = HttpUtils.getHttPost(url);
                // request body
                DataProxyConfigRequest request = new DataProxyConfigRequest();
                request.setClusterName(clusterName);
                request.setClusterTag(clusterTag);
                if (StringUtils.isNotBlank(configManager.getMetaConfigMD5())) {
                    request.setMd5(configManager.getMetaConfigMD5());
                }
                httpPost.setEntity(HttpUtils.getEntity(request));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sync meta: start to get config, to:{}, params: {}, headers: {}",
                            url, request, httpPost.getAllHeaders());
                }
                // request with post
                long startTime = System.currentTimeMillis();
                CloseableHttpResponse response = httpClient.execute(httpPost);
                String returnStr = EntityUtils.toString(response.getEntity());
                long dltTime = System.currentTimeMillis() - startTime;
                if (dltTime >= CommonConfigHolder.getInstance().getMetaConfigWastAlarmMs()) {
                    LOG.warn("Sync meta: end to get config, WAIST {} ms, over alarm: {} ms, from:{}",
                            dltTime, CommonConfigHolder.getInstance().getMetaConfigWastAlarmMs(), url);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sync meta: end to get config, WAIST {} ms, from:{}, result:{}",
                                dltTime, url, returnStr);
                    }
                }
                if (response.getStatusLine().getStatusCode() != 200) {
                    LOG.error(
                            "Sync meta: return failure, errCode {}, from:{}, params:{}, headers:{}, response:{}",
                            response.getStatusLine().getStatusCode(), url, request, httpPost.getAllHeaders(),
                            returnStr);
                    return false;
                }
                // get groupId <-> topic and m value.
                DataProxyConfigResponse proxyResponse;
                try {
                    proxyResponse = gson.fromJson(returnStr, DataProxyConfigResponse.class);
                } catch (Throwable e) {
                    LOG.error("Sync meta: exception thrown while parsing config, from:{}, params:{}, response:{}",
                            url, request, returnStr, e);
                    return false;
                }
                // check required fields
                ImmutablePair<Boolean, String> validResult = validRequiredFields(proxyResponse);
                if (!validResult.getLeft()) {
                    if (proxyResponse.getErrCode() != DataProxyConfigResponse.NOUPDATE) {
                        LOG.error("Sync meta: {}, from:{}, params:{}, return:{}",
                                validResult.getRight(), url, request, returnStr);
                    }
                    return true;
                }
                // get mq cluster info
                ImmutablePair<CacheType, Map<String, CacheClusterConfig>> clusterInfo =
                        buildCacheClusterConfig(proxyResponse.getData().getCacheClusterSet());
                if (clusterInfo.getLeft() == CacheType.N) {
                    LOG.error("Sync meta: unsupported mq type {}, from:{}, params:{}, return:{}",
                            clusterInfo.getLeft(), url, request, returnStr);
                    return true;
                }
                if (clusterInfo.getRight().isEmpty()) {
                    LOG.error("Sync meta: cacheClusters is empty, from:{}, params:{}, return:{}",
                            url, request, returnStr);
                    return true;
                }
                // get ID to Topic configure
                Map<String, IdTopicConfig> idTopicConfigMap = buildCacheTopicConfig(
                        clusterInfo.getLeft(), proxyResponse.getData().getProxyCluster());
                InLongMetaConfig inLongMetaConfig = new InLongMetaConfig(proxyResponse.getMd5(),
                        clusterInfo.getLeft(), clusterInfo.getRight(), idTopicConfigMap);
                // update meta configure
                configManager.updateMetaConfigInfo(inLongMetaConfig);
                // update handshake to manager status
                if (ConfigManager.handshakeManagerOk.get()) {
                    LOG.info("Sync meta: sync config success, from:{}", url);
                } else {
                    ConfigManager.handshakeManagerOk.set(true);
                    LOG.info("Sync meta: sync config success, handshake manager ok, from:{}", url);
                }
                return true;
            } catch (Throwable ex) {
                LOG.error("Sync meta: process throw exception, from:{}", url, ex);
                return false;
            } finally {
                if (httpPost != null) {
                    httpPost.releaseConnection();
                }
            }
        }

        /**
         * check required fields status
         *
         * @param response  response from Manager
         *
         * @return   check result
         */
        public ImmutablePair<Boolean, String> validRequiredFields(DataProxyConfigResponse response) {
            if (response == null) {
                return ImmutablePair.of(false, "parse result is null");
            } else if (!response.isResult()) {
                return ImmutablePair.of(false, "result is NOT true");
            } else if (response.getErrCode() != DataProxyConfigResponse.SUCC) {
                return ImmutablePair.of(false, "errCode is "
                        + response.getErrCode() + ", NOT success");
            } else if (response.getMd5() == null) {
                return ImmutablePair.of(false, "md5 field is null");
            } else if (response.getData() == null) {
                return ImmutablePair.of(false, "data field is null");
            } else if (response.getData().getProxyCluster() == null) {
                return ImmutablePair.of(false, "proxyCluster field is null");
            } else if (response.getData().getCacheClusterSet() == null) {
                return ImmutablePair.of(false, "cacheClusterSet field is null");
            } else if (response.getData().getProxyCluster().getInlongIds() == null) {
                return ImmutablePair.of(false, "inlongIds field is null");
            } else if (response.getData().getCacheClusterSet().getCacheClusters() == null) {
                return ImmutablePair.of(false, "cacheClusters field is null");
            }
            return ImmutablePair.of(true, "ok");
        }

        /**
         * build cluster config based on cluster set object
         *
         * @param clusterSetObject  mq cluster set obect
         *
         * @return   mq type and cluster set configure
         */
        private ImmutablePair<CacheType, Map<String, CacheClusterConfig>> buildCacheClusterConfig(
                CacheClusterSetObject clusterSetObject) {
            CacheType mqType = CacheType.convert(clusterSetObject.getType());
            Map<String, CacheClusterConfig> result = new HashMap<>();
            for (CacheClusterObject clusterObject : clusterSetObject.getCacheClusters()) {
                if (clusterObject == null || StringUtils.isBlank(clusterObject.getName())) {
                    continue;
                }
                CacheClusterConfig config = new CacheClusterConfig();
                config.setClusterName(clusterObject.getName());
                config.setToken(clusterObject.getToken());
                config.getParams().putAll(clusterObject.getParams());
                result.put(config.getClusterName(), config);
            }
            return ImmutablePair.of(mqType, result);
        }

        /**
         * build id2field config based on id2Topic configure
         *
         * @param mqType  mq cluster type
         * @param proxyClusterObject  cluster object info
         *
         * @return   ID to Topic configures
         */
        private Map<String, IdTopicConfig> buildCacheTopicConfig(
                CacheType mqType, ProxyClusterObject proxyClusterObject) {
            Map<String, IdTopicConfig> tmpTopicConfigMap = new HashMap<>();
            List<InLongIdObject> inLongIds = proxyClusterObject.getInlongIds();
            if (inLongIds.isEmpty()) {
                return tmpTopicConfigMap;
            }
            int index;
            String[] idItems;
            String groupId;
            String streamId;
            String topicName;
            String tenant;
            String nameSpace;
            for (InLongIdObject idObject : inLongIds) {
                if (idObject == null
                        || StringUtils.isBlank(idObject.getInlongId())
                        || StringUtils.isBlank(idObject.getTopic())) {
                    continue;
                }
                // parse inlong id
                idItems = idObject.getInlongId().split("\\.");
                if (idItems.length == 2) {
                    if (StringUtils.isBlank(idItems[0])) {
                        continue;
                    }
                    groupId = idItems[0].trim();
                    streamId = idItems[1].trim();
                } else {
                    groupId = idObject.getInlongId().trim();
                    streamId = "";
                }
                topicName = idObject.getTopic().trim();
                // change full topic name "pulsar-xxx/test/base_topic_name" to
                // base topic name "base_topic_name"
                index = topicName.lastIndexOf('/');
                if (index >= 0) {
                    topicName = topicName.substring(index + 1).trim();
                }
                tenant = idObject.getParams().getOrDefault(ConfigConstants.KEY_TENANT, "");
                nameSpace = idObject.getParams().getOrDefault(ConfigConstants.KEY_NAMESPACE, "");
                if (StringUtils.isBlank(idObject.getTopic())) {
                    // namespace field must exist and value not be empty,
                    // otherwise it is an illegal configuration item.
                    continue;
                }
                if (mqType.equals(CacheType.TUBE)) {
                    topicName = nameSpace;
                } else if (mqType.equals(CacheType.KAFKA)) {
                    if (topicName.equals(streamId)) {
                        topicName = String.format(Constants.DEFAULT_KAFKA_TOPIC_FORMAT, nameSpace, topicName);
                    }
                }
                IdTopicConfig tmpConfig = new IdTopicConfig();
                tmpConfig.setInlongGroupIdAndStreamId(groupId, streamId);
                tmpConfig.setTenantAndNameSpace(tenant, nameSpace);
                tmpConfig.setTopicName(topicName);
                tmpConfig.setParams(idObject.getParams());
                tmpConfig.setDataType(DataTypeEnum.convert(
                        idObject.getParams().getOrDefault("dataType", DataTypeEnum.TEXT.getType())));
                tmpConfig.setFieldDelimiter(idObject.getParams().getOrDefault("fieldDelimiter", "|"));
                tmpConfig.setFileDelimiter(idObject.getParams().getOrDefault("fileDelimiter", "\n"));
                tmpConfig.setUseExtendedFields(Boolean.valueOf(
                        idObject.getParams().getOrDefault("useExtendedFields", "false")));
                tmpConfig.setMsgWrapType(getPbWrapType(idObject));
                tmpConfig.setV1CompressType(getPbCompressType(idObject));
                tmpTopicConfigMap.put(tmpConfig.getUid(), tmpConfig);
                // add only groupId object for tube
                if (mqType.equals(CacheType.TUBE)
                        && !tmpConfig.getUid().equals(tmpConfig.getInlongGroupId())
                        && tmpTopicConfigMap.get(tmpConfig.getInlongGroupId()) == null) {
                    IdTopicConfig tmpConfig2 = new IdTopicConfig();
                    tmpConfig2.setInlongGroupIdAndStreamId(groupId, "");
                    tmpConfig2.setTenantAndNameSpace(tenant, nameSpace);
                    tmpConfig2.setTopicName(topicName);
                    tmpConfig2.setDataType(tmpConfig.getDataType());
                    tmpConfig2.setFieldDelimiter(tmpConfig.getFieldDelimiter());
                    tmpConfig2.setFileDelimiter(tmpConfig.getFileDelimiter());
                    tmpConfig2.setParams(new HashMap<>(tmpConfig.getParams()));
                    tmpConfig2.setUseExtendedFields(tmpConfig.isUseExtendedFields());
                    tmpConfig2.setMsgWrapType(tmpConfig.getMsgWrapType());
                    tmpConfig2.setV1CompressType(tmpConfig.getV1CompressType());
                    tmpTopicConfigMap.put(tmpConfig2.getUid(), tmpConfig2);
                }
            }
            return tmpTopicConfigMap;
        }

        private MessageWrapType getPbWrapType(InLongIdObject idObject) {
            String strWrapType = idObject.getParams().get("wrapType");
            if (StringUtils.isBlank(strWrapType)) {
                return MessageWrapType.UNKNOWN;
            } else {
                return MessageWrapType.forType(strWrapType);
            }
        }

        private InlongCompressType getPbCompressType(InLongIdObject idObject) {
            String strCompressType = idObject.getParams().get("inlongCompressType");
            if (StringUtils.isBlank(strCompressType)) {
                return CommonConfigHolder.getInstance().getDefV1MsgCompressType();
            } else {
                InlongCompressType msgCompType = InlongCompressType.forType(strCompressType);
                if (msgCompType == InlongCompressType.UNKNOWN) {
                    return CommonConfigHolder.getInstance().getDefV1MsgCompressType();
                } else {
                    return msgCompType;
                }
            }
        }
    }
}

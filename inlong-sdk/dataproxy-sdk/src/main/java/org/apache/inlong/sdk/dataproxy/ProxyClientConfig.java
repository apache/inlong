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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.metric.MetricConfig;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class ProxyClientConfig {

    private String managerIP = "";
    private int managerPort = 8099;
    private boolean visitManagerByHttp = true;
    private boolean onlyUseLocalProxyConfig = false;
    private int managerConnTimeoutMs = ConfigConstants.VAL_DEF_CONNECT_TIMEOUT_MS;
    private int managerSocketTimeoutMs = ConfigConstants.VAL_DEF_SOCKET_TIMEOUT_MS;
    private long managerConfigSyncInrMs =
            ConfigConstants.VAL_DEF_CONFIG_SYNC_INTERVAL_MIN * ConfigConstants.VAL_UNIT_MIN_TO_MS;
    private int configSyncMaxRetryIfFail = ConfigConstants.VAL_DEF_RETRY_IF_CONFIG_SYNC_FAIL;
    private String configStoreBasePath = System.getProperty("user.dir");
    // max expired time for config cache.
    private long configCacheExpiredMs = ConfigConstants.VAL_DEF_CACHE_CONFIG_EXPIRED_MS;
    // max expired time for config query failure status
    private long configFailStatusExpiredMs = ConfigConstants.VAL_DEF_CONFIG_FAIL_STATUS_EXPIRED_MS;
    // nodes force choose interval ms
    private long forceReChooseInrMs = ConfigConstants.VAL_DEF_FORCE_CHOOSE_INR_MS;
    private boolean enableAuthentication = false;
    private String authSecretId = "";
    private String authSecretKey = "";
    private String inlongGroupId;
    private String regionName = ConfigConstants.VAL_DEF_REGION_NAME;
    private int aliveConnections = ConfigConstants.VAL_DEF_ALIVE_CONNECTIONS;
    // data encrypt info
    private boolean enableDataEncrypt = false;
    private String rsaPubKeyUrl = "";
    private String userName = "";

    private int syncThreadPoolSize;
    private int asyncCallbackSize;

    private String tlsServerCertFilePathAndName;
    private String tlsServerKey;
    private String tlsVersion = "TLSv1.2";
    private int maxTimeoutCnt = ConfigConstants.MAX_TIMEOUT_CNT;
    private String protocolType;

    // metric configure
    private MetricConfig metricConfig = new MetricConfig();

    // connect timeout in milliseconds
    private long connectTimeoutMs = ConfigConstants.VAL_DEF_CONNECT_TIMEOUT_MS;
    // request timeout in milliseconds
    private long requestTimeoutMs = ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS;
    // connect close wait period in milliseconds
    private long conCloseWaitPeriodMs =
            ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS + ConfigConstants.VAL_DEF_CONNECT_CLOSE_DELAY_MS;
    // client reconnect wait period in ms
    private long reConnectWaitMs = ConfigConstants.VAL_DEF_RECONNECT_WAIT_MS;
    // socket receive buffer
    private int recvBufferSize = ConfigConstants.DEFAULT_RECEIVE_BUFFER_SIZE;
    // socket send buffer
    private int sendBufferSize = ConfigConstants.DEFAULT_SEND_BUFFER_SIZE;
    // max message count per connection
    private long maxMsgInFlightPerConn = ConfigConstants.MAX_INFLIGHT_MSG_COUNT_PER_CONNECTION;

    // configuration for http client
    // whether discard old metric when cache is full.
    private boolean discardOldMessage = false;
    private int proxyHttpUpdateIntervalMinutes;
    // thread number for async sending data.
    private int asyncWorkerNumber = 3;
    // interval for async worker in microseconds.
    private int asyncWorkerInterval = 500;
    private boolean cleanHttpCacheWhenClosing = false;

    private int ioThreadNum = Runtime.getRuntime().availableProcessors();
    private boolean enableBusyWait = false;

    private int senderMaxAttempt = ConfigConstants.DEFAULT_SENDER_MAX_ATTEMPT;

    /* pay attention to the last url parameter ip */
    public ProxyClientConfig(boolean visitManagerByHttp, String managerIp,
            int managerPort, String inlongGroupId, String authSecretId, String authSecretKey) throws ProxySdkException {
        if (StringUtils.isBlank(managerIp)) {
            throw new ProxySdkException("managerIp is Blank!");
        }
        if (managerPort <= 0) {
            throw new ProxySdkException("managerPort <= 0!");
        }
        if (StringUtils.isBlank(inlongGroupId)) {
            throw new ProxySdkException("groupId is blank!");
        }
        this.inlongGroupId = inlongGroupId.trim();
        this.visitManagerByHttp = visitManagerByHttp;
        this.managerPort = managerPort;
        this.managerIP = managerIp;
        this.syncThreadPoolSize = ConfigConstants.SYNC_THREAD_POOL_SIZE;
        this.asyncCallbackSize = ConfigConstants.ASYNC_CALLBACK_SIZE;
        this.proxyHttpUpdateIntervalMinutes = ConfigConstants.PROXY_HTTP_UPDATE_INTERVAL_MINUTES;
        this.authSecretId = authSecretId;
        this.authSecretKey = authSecretKey;
    }

    /* pay attention to the last url parameter ip */
    public ProxyClientConfig(String managerAddress,
            String inlongGroupId, String authSecretId, String authSecretKey) throws ProxySdkException {
        checkAndParseAddress(managerAddress);
        if (StringUtils.isBlank(inlongGroupId)) {
            throw new ProxySdkException("groupId is blank!");
        }
        this.inlongGroupId = inlongGroupId.trim();
        this.syncThreadPoolSize = ConfigConstants.SYNC_THREAD_POOL_SIZE;
        this.asyncCallbackSize = ConfigConstants.ASYNC_CALLBACK_SIZE;
        this.proxyHttpUpdateIntervalMinutes = ConfigConstants.PROXY_HTTP_UPDATE_INTERVAL_MINUTES;
        this.authSecretId = authSecretId;
        this.authSecretKey = authSecretKey;
    }

    public String getManagerIP() {
        return managerIP;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public boolean isVisitManagerByHttp() {
        return visitManagerByHttp;
    }

    public boolean isOnlyUseLocalProxyConfig() {
        return onlyUseLocalProxyConfig;
    }

    public void setOnlyUseLocalProxyConfig(boolean onlyUseLocalProxyConfig) {
        this.onlyUseLocalProxyConfig = onlyUseLocalProxyConfig;
    }

    public boolean isEnableAuthentication() {
        return this.enableAuthentication;
    }

    public String getAuthSecretId() {
        return authSecretId;
    }

    public String getAuthSecretKey() {
        return authSecretKey;
    }

    public void setAuthenticationInfo(boolean needAuthentication, String secretId, String secretKey) {
        this.enableAuthentication = needAuthentication;
        if (!this.enableAuthentication) {
            return;
        }
        if (StringUtils.isBlank(secretId)) {
            throw new IllegalArgumentException("secretId is Blank!");
        }
        if (StringUtils.isBlank(secretKey)) {
            throw new IllegalArgumentException("secretKey is Blank!");
        }
        this.authSecretId = secretId.trim();
        this.authSecretKey = secretKey.trim();
    }

    public long getManagerConfigSyncInrMs() {
        return managerConfigSyncInrMs;
    }

    public void setManagerConfigSyncInrMin(int managerConfigSyncInrMin) {
        int tmpValue =
                Math.min(ConfigConstants.VAL_MAX_CONFIG_SYNC_INTERVAL_MIN,
                        Math.max(ConfigConstants.VAL_MIN_CONFIG_SYNC_INTERVAL_MIN, managerConfigSyncInrMin));
        this.managerConfigSyncInrMs = tmpValue * ConfigConstants.VAL_UNIT_MIN_TO_MS;
    }

    public int getManagerConnTimeoutMs() {
        return managerConnTimeoutMs;
    }

    public void setManagerConnTimeoutMs(int managerConnTimeoutMs) {
        this.managerConnTimeoutMs =
                Math.min(ConfigConstants.VAL_MAX_CONNECT_TIMEOUT_MS,
                        Math.max(ConfigConstants.VAL_MIN_CONNECT_TIMEOUT_MS, managerConnTimeoutMs));
    }

    public int getManagerSocketTimeoutMs() {
        return managerSocketTimeoutMs;
    }

    public void setManagerSocketTimeoutMs(int managerSocketTimeoutMs) {
        this.managerSocketTimeoutMs =
                Math.min(ConfigConstants.VAL_MAX_SOCKET_TIMEOUT_MS,
                        Math.max(ConfigConstants.VAL_MIN_SOCKET_TIMEOUT_MS, managerSocketTimeoutMs));
    }

    public int getConfigSyncMaxRetryIfFail() {
        return configSyncMaxRetryIfFail;
    }

    public void setConfigSyncMaxRetryIfFail(int configSyncMaxRetryIfFail) {
        this.configSyncMaxRetryIfFail =
                Math.min(configSyncMaxRetryIfFail, ConfigConstants.VAL_MAX_RETRY_IF_CONFIG_SYNC_FAIL);
    }

    public String getConfigStoreBasePath() {
        return configStoreBasePath;
    }

    public void setConfigStoreBasePath(String configStoreBasePath) {
        if (StringUtils.isBlank(configStoreBasePath)) {
            return;
        }
        this.configStoreBasePath = configStoreBasePath.trim();
    }

    public long getConfigCacheExpiredMs() {
        return configCacheExpiredMs;
    }

    public void setConfigCacheExpiredMs(long configCacheExpiredMs) {
        this.configCacheExpiredMs = configCacheExpiredMs;
    }

    public long getConfigFailStatusExpiredMs() {
        return configFailStatusExpiredMs;
    }

    public void setConfigFailStatusExpiredMs(long configFailStatusExpiredMs) {
        this.configFailStatusExpiredMs =
                Math.min(configFailStatusExpiredMs, ConfigConstants.VAL_MAX_CONFIG_FAIL_STATUS_EXPIRED_MS);
    }

    public long getForceReChooseInrMs() {
        return forceReChooseInrMs;
    }

    public void setForceReChooseInrMs(long forceReChooseInrMs) {
        this.forceReChooseInrMs =
                Math.max(ConfigConstants.VAL_MIN_FORCE_CHOOSE_INR_MS, forceReChooseInrMs);
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        if (StringUtils.isNotBlank(regionName)) {
            this.regionName = regionName.trim();
        }
    }

    public int getAliveConnections() {
        return this.aliveConnections;
    }

    public void setAliveConnections(int aliveConnections) {
        this.aliveConnections =
                Math.max(ConfigConstants.VAL_MIN_ALIVE_CONNECTIONS, aliveConnections);
    }

    public boolean isEnableDataEncrypt() {
        return enableDataEncrypt;
    }

    public String getRsaPubKeyUrl() {
        return rsaPubKeyUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void enableDataEncrypt(boolean needDataEncrypt, String userName, String rsaPubKeyUrl) {
        this.enableDataEncrypt = needDataEncrypt;
        if (!this.enableDataEncrypt) {
            return;
        }
        if (StringUtils.isBlank(userName)) {
            throw new IllegalArgumentException("userName is Blank!");
        }
        if (StringUtils.isBlank(rsaPubKeyUrl)) {
            throw new IllegalArgumentException("rsaPubKeyUrl is Blank!");
        }
        this.userName = userName.trim();
        this.rsaPubKeyUrl = rsaPubKeyUrl.trim();
    }

    public String getTlsServerCertFilePathAndName() {
        return tlsServerCertFilePathAndName;
    }

    public String getTlsServerKey() {
        return tlsServerKey;
    }

    public int getSyncThreadPoolSize() {
        return syncThreadPoolSize;
    }

    public void setSyncThreadPoolSize(int syncThreadPoolSize) {
        if (syncThreadPoolSize > ConfigConstants.MAX_SYNC_THREAD_POOL_SIZE) {
            throw new IllegalArgumentException("");
        }
        this.syncThreadPoolSize = syncThreadPoolSize;
    }

    public int getTotalAsyncCallbackSize() {
        return asyncCallbackSize;
    }

    public void setTotalAsyncCallbackSize(int asyncCallbackSize) {
        this.asyncCallbackSize = asyncCallbackSize;
    }

    public int getMaxTimeoutCnt() {
        return maxTimeoutCnt;
    }

    public void setMaxTimeoutCnt(int maxTimeoutCnt) {
        if (maxTimeoutCnt < 0) {
            throw new IllegalArgumentException("maxTimeoutCnt must bigger than 0");
        }
        this.maxTimeoutCnt = maxTimeoutCnt;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(long connectTimeoutMs) {
        if (connectTimeoutMs >= ConfigConstants.VAL_MIN_CONNECT_TIMEOUT_MS) {
            this.connectTimeoutMs = connectTimeoutMs;
        }
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        if (requestTimeoutMs >= ConfigConstants.VAL_MIN_REQUEST_TIMEOUT_MS) {
            this.requestTimeoutMs = requestTimeoutMs;
            this.conCloseWaitPeriodMs =
                    this.requestTimeoutMs + ConfigConstants.VAL_DEF_CONNECT_CLOSE_DELAY_MS;
        }
    }

    public long getConCloseWaitPeriodMs() {
        return conCloseWaitPeriodMs;
    }

    public void setConCloseWaitPeriodMs(long conCloseWaitPeriodMs) {
        if (conCloseWaitPeriodMs >= 0) {
            this.conCloseWaitPeriodMs = conCloseWaitPeriodMs;
        }
    }

    public long getReConnectWaitMs() {
        return reConnectWaitMs;
    }

    public void setReConnectWaitMs(long reConnectWaitMs) {
        if (reConnectWaitMs > ConfigConstants.VAL_MAX_RECONNECT_WAIT_MS) {
            this.reConnectWaitMs = ConfigConstants.VAL_MAX_RECONNECT_WAIT_MS;
        }
    }

    public int getRecvBufferSize() {
        return recvBufferSize;
    }

    public void setRecvBufferSize(int recvBufferSize) {
        if (recvBufferSize > 0 && recvBufferSize < Integer.MAX_VALUE) {
            this.recvBufferSize = recvBufferSize;
        }
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize > 0 && sendBufferSize < Integer.MAX_VALUE) {
            this.sendBufferSize = sendBufferSize;
        }
    }

    public long getMaxMsgInFlightPerConn() {
        return maxMsgInFlightPerConn;
    }

    public void setMaxMsgInFlightPerConn(long maxMsgInFlightPerConn) {
        this.maxMsgInFlightPerConn = maxMsgInFlightPerConn;
    }

    public void setHttpsInfo(String tlsServerCertFilePathAndName, String tlsServerKey) {
        if (StringUtils.isBlank(tlsServerCertFilePathAndName)) {
            throw new IllegalArgumentException("tlsServerCertFilePathAndName is Blank!");
        }
        if (StringUtils.isBlank(tlsServerKey)) {
            throw new IllegalArgumentException("tlsServerKey is Blank!");
        }
        this.tlsServerKey = tlsServerKey;
        this.tlsServerCertFilePathAndName = tlsServerCertFilePathAndName;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }

    public void setTlsVersion(String tlsVersion) {
        this.tlsVersion = tlsVersion;
    }

    public int getProxyHttpUpdateIntervalMinutes() {
        return proxyHttpUpdateIntervalMinutes;
    }

    public void setProxyHttpUpdateIntervalMinutes(int proxyHttpUpdateIntervalMinutes) {
        this.proxyHttpUpdateIntervalMinutes = proxyHttpUpdateIntervalMinutes;
    }

    public boolean isDiscardOldMessage() {
        return discardOldMessage;
    }

    public void setDiscardOldMessage(boolean discardOldMessage) {
        this.discardOldMessage = discardOldMessage;
    }

    public int getAsyncWorkerNumber() {
        return asyncWorkerNumber;
    }

    public void setAsyncWorkerNumber(int asyncWorkerNumber) {
        this.asyncWorkerNumber = asyncWorkerNumber;
    }

    public int getAsyncWorkerInterval() {
        return asyncWorkerInterval;
    }

    public void setAsyncWorkerInterval(int asyncWorkerInterval) {
        this.asyncWorkerInterval = asyncWorkerInterval;
    }

    public boolean isCleanHttpCacheWhenClosing() {
        return cleanHttpCacheWhenClosing;
    }

    public void setCleanHttpCacheWhenClosing(boolean cleanHttpCacheWhenClosing) {
        this.cleanHttpCacheWhenClosing = cleanHttpCacheWhenClosing;
    }

    public MetricConfig getMetricConfig() {
        return metricConfig;
    }

    public boolean isEnableMetric() {
        return metricConfig.isEnableMetric();
    }

    public void setMetricConfig(MetricConfig metricConfig) {
        if (metricConfig == null) {
            return;
        }
        this.metricConfig = metricConfig;
    }

    public int getIoThreadNum() {
        return ioThreadNum;
    }

    public void setIoThreadNum(int ioThreadNum) {
        this.ioThreadNum = ioThreadNum;
    }

    public boolean isEnableBusyWait() {
        return enableBusyWait;
    }

    public void setEnableBusyWait(boolean enableBusyWait) {
        this.enableBusyWait = enableBusyWait;
    }

    public int getSenderMaxAttempt() {
        return senderMaxAttempt;
    }

    public void setSenderAttempt(int senderMaxAttempt) {
        this.senderMaxAttempt = senderMaxAttempt;
    }

    private void checkAndParseAddress(String managerAddress) throws ProxySdkException {
        if (StringUtils.isBlank(managerAddress)
                || (!managerAddress.startsWith(ConfigConstants.HTTP)
                        && !managerAddress.startsWith(ConfigConstants.HTTPS))) {
            throw new ProxySdkException("managerAddress is blank or missing http/https protocol");
        }
        String hostPortInfo;
        if (managerAddress.startsWith(ConfigConstants.HTTPS)) {
            this.visitManagerByHttp = false;
            hostPortInfo = managerAddress.substring(ConfigConstants.HTTPS.length());
        } else {
            hostPortInfo = managerAddress.substring(ConfigConstants.HTTP.length());
        }
        if (StringUtils.isBlank(hostPortInfo)) {
            throw new ProxySdkException("managerAddress must include host:port info!");
        }
        String[] fields = hostPortInfo.split(":");
        if (fields.length == 1) {
            throw new ProxySdkException("managerAddress must include port info!");
        } else if (fields.length > 2) {
            throw new ProxySdkException("managerAddress must only include host:port info!");
        }
        if (StringUtils.isBlank(fields[0])) {
            throw new ProxySdkException("managerAddress's host is blank!");
        }
        this.managerIP = fields[0].trim();
        if (StringUtils.isBlank(fields[1])) {
            throw new ProxySdkException("managerAddress's port is blank!");
        }
        try {
            this.managerPort = Integer.parseInt(fields[1]);
        } catch (Throwable ex) {
            throw new ProxySdkException("managerAddress's port must be number!");
        }
    }
}

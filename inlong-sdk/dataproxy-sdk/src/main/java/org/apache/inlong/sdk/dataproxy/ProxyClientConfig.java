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

import org.apache.inlong.sdk.dataproxy.metric.MetricConfig;
import org.apache.inlong.sdk.dataproxy.network.IpUtils;
import org.apache.inlong.sdk.dataproxy.network.ProxysdkException;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class ProxyClientConfig {

    private int aliveConnections;
    private int syncThreadPoolSize;
    private int asyncCallbackSize;
    private int managerPort = 8099;
    private String managerIP = "";
    private String managerAddress;

    private String managerIpLocalPath = System.getProperty("user.dir") + "/.inlong/.managerIps";
    private String managerUrl = "";
    private int proxyUpdateIntervalMinutes;
    private int proxyUpdateMaxRetry;
    private String inlongGroupId;
    private boolean requestByHttp = true;
    private boolean isNeedDataEncry = false;
    private boolean needAuthentication = false;
    private String userName = "";
    private String secretKey = "";
    private String rsaPubKeyUrl = "";
    private String confStoreBasePath = System.getProperty("user.dir") + "/.inlong/";
    private String tlsServerCertFilePathAndName;
    private String tlsServerKey;
    private String tlsVersion = "TLSv1.2";
    private int maxTimeoutCnt = ConfigConstants.MAX_TIMEOUT_CNT;
    private String authSecretId;
    private String authSecretKey;
    private String protocolType;

    private boolean enableSaveManagerVIps = false;
    // metric configure
    private MetricConfig metricConfig = new MetricConfig();

    private int managerConnectionTimeout = 10000;
    // http socket timeout in milliseconds
    private int managerSocketTimeout = 30 * 1000;

    private boolean readProxyIPFromLocal = false;

    // connect timeout in milliseconds
    private long connectTimeoutMs = ConfigConstants.VAL_DEF_CONNECT_TIMEOUT_MS;
    // request timeout in milliseconds
    private long requestTimeoutMs = ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS;
    // connect close wait period in milliseconds
    private long conCloseWaitPeriodMs =
            ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS + ConfigConstants.VAL_DEF_CONNECT_CLOSE_DELAY_MS;

    // configuration for http client
    // whether discard old metric when cache is full.
    private boolean discardOldMessage = false;
    private int proxyHttpUpdateIntervalMinutes;
    // thread number for async sending data.
    private int asyncWorkerNumber = 3;
    // interval for async worker in microseconds.
    private int asyncWorkerInterval = 500;
    private boolean cleanHttpCacheWhenClosing = false;
    // max cache time for proxy config.
    private long maxProxyCacheTimeInMs = 30 * 60 * 1000;

    private int ioThreadNum = Runtime.getRuntime().availableProcessors();
    private boolean enableBusyWait = false;

    private int virtualNode;

    private LoadBalance loadBalance;

    private int maxRetry;
    private int senderMaxAttempt = ConfigConstants.DEFAULT_SENDER_MAX_ATTEMPT;

    /* pay attention to the last url parameter ip */
    public ProxyClientConfig(String localHost, boolean requestByHttp, String managerIp,
            int managerPort, String inlongGroupId, String authSecretId, String authSecretKey,
            LoadBalance loadBalance, int virtualNode, int maxRetry) throws ProxysdkException {
        if (StringUtils.isBlank(localHost)) {
            throw new ProxysdkException("localHost is blank!");
        }
        if (StringUtils.isBlank(managerIp)) {
            throw new IllegalArgumentException("managerIp is Blank!");
        }
        if (StringUtils.isBlank(inlongGroupId)) {
            throw new ProxysdkException("groupId is blank!");
        }
        this.inlongGroupId = inlongGroupId;
        this.requestByHttp = requestByHttp;
        this.managerPort = managerPort;
        this.managerIP = managerIp;
        this.managerAddress = getManagerAddress(managerIp, managerPort, requestByHttp);
        this.managerUrl =
                getManagerUrl(managerAddress, inlongGroupId);
        IpUtils.validLocalIp(localHost);
        this.aliveConnections = ConfigConstants.ALIVE_CONNECTIONS;
        this.syncThreadPoolSize = ConfigConstants.SYNC_THREAD_POOL_SIZE;
        this.asyncCallbackSize = ConfigConstants.ASYNC_CALLBACK_SIZE;
        this.proxyUpdateIntervalMinutes = ConfigConstants.PROXY_UPDATE_INTERVAL_MINUTES;
        this.proxyHttpUpdateIntervalMinutes = ConfigConstants.PROXY_HTTP_UPDATE_INTERVAL_MINUTES;
        this.proxyUpdateMaxRetry = ConfigConstants.PROXY_UPDATE_MAX_RETRY;
        this.authSecretId = authSecretId;
        this.authSecretKey = authSecretKey;
        this.loadBalance = loadBalance;
        this.virtualNode = virtualNode;
        this.maxRetry = maxRetry;
    }

    /* pay attention to the last url parameter ip */
    public ProxyClientConfig(String managerAddress, String inlongGroupId, String authSecretId, String authSecretKey,
            LoadBalance loadBalance, int virtualNode, int maxRetry) throws ProxysdkException {
        if (StringUtils.isBlank(managerAddress) || (!managerAddress.startsWith(ConfigConstants.HTTP)
                && !managerAddress.startsWith(ConfigConstants.HTTPS))) {
            throw new ProxysdkException("managerAddress is blank or missing http/https protocol ");
        }
        if (StringUtils.isBlank(inlongGroupId)) {
            throw new ProxysdkException("groupId is blank!");
        }
        if (managerAddress.startsWith(ConfigConstants.HTTPS)) {
            this.requestByHttp = false;
        }
        this.managerAddress = managerAddress;
        this.managerUrl = getManagerUrl(managerAddress, inlongGroupId);
        this.inlongGroupId = inlongGroupId;
        this.aliveConnections = ConfigConstants.ALIVE_CONNECTIONS;
        this.syncThreadPoolSize = ConfigConstants.SYNC_THREAD_POOL_SIZE;
        this.asyncCallbackSize = ConfigConstants.ASYNC_CALLBACK_SIZE;
        this.proxyUpdateIntervalMinutes = ConfigConstants.PROXY_UPDATE_INTERVAL_MINUTES;
        this.proxyHttpUpdateIntervalMinutes = ConfigConstants.PROXY_HTTP_UPDATE_INTERVAL_MINUTES;
        this.proxyUpdateMaxRetry = ConfigConstants.PROXY_UPDATE_MAX_RETRY;
        this.authSecretId = authSecretId;
        this.authSecretKey = authSecretKey;
        this.loadBalance = loadBalance;
        this.virtualNode = virtualNode;
        this.maxRetry = maxRetry;
    }

    private String getManagerUrl(String managerAddress, String inlongGroupId) {
        return managerAddress + ConfigConstants.MANAGER_DATAPROXY_API + inlongGroupId;
    }

    private String getManagerAddress(String managerIp, int managerPort, boolean requestByHttp) {
        String protocolType = ConfigConstants.HTTPS;
        if (requestByHttp) {
            protocolType = ConfigConstants.HTTP;
        }
        return protocolType + managerIp + ":" + managerPort;
    }

    public ProxyClientConfig(String localHost, boolean requestByHttp, String managerIp, int managerPort,
            String inlongGroupId, String authSecretId, String authSecretKey) throws ProxysdkException {
        this(localHost, requestByHttp, managerIp, managerPort, inlongGroupId, authSecretId, authSecretKey,
                ConfigConstants.DEFAULT_LOAD_BALANCE, ConfigConstants.DEFAULT_VIRTUAL_NODE,
                ConfigConstants.DEFAULT_RANDOM_MAX_RETRY);
    }

    public ProxyClientConfig(String managerAddress, String inlongGroupId, String authSecretId, String authSecretKey)
            throws ProxysdkException {
        this(managerAddress, inlongGroupId, authSecretId, authSecretKey,
                ConfigConstants.DEFAULT_LOAD_BALANCE, ConfigConstants.DEFAULT_VIRTUAL_NODE,
                ConfigConstants.DEFAULT_RANDOM_MAX_RETRY);
    }

    public String getTlsServerCertFilePathAndName() {
        return tlsServerCertFilePathAndName;
    }

    public String getTlsServerKey() {
        return tlsServerKey;
    }

    public boolean isRequestByHttp() {
        return requestByHttp;
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public String getManagerIP() {
        return managerIP;
    }

    public String getManagerIpLocalPath() {
        return managerIpLocalPath;
    }

    public void setManagerIpLocalPath(String managerIpLocalPath) throws ProxysdkException {
        if (StringUtils.isEmpty(managerIpLocalPath)) {
            throw new ProxysdkException("managerIpLocalPath is empty.");
        }
        if (managerIpLocalPath.charAt(managerIpLocalPath.length() - 1) == '/') {
            managerIpLocalPath = managerIpLocalPath.substring(0, managerIpLocalPath.length() - 1);
        }
        this.managerIpLocalPath = managerIpLocalPath + "/.managerIps";
    }

    public boolean isEnableSaveManagerVIps() {
        return enableSaveManagerVIps;
    }

    public void setEnableSaveManagerVIps(boolean enable) {
        this.enableSaveManagerVIps = enable;
    }

    public String getConfStoreBasePath() {
        return confStoreBasePath;
    }

    public void setConfStoreBasePath(String confStoreBasePath) {
        this.confStoreBasePath = confStoreBasePath;
    }

    public int getAliveConnections() {
        return this.aliveConnections;
    }

    public void setAliveConnections(int aliveConnections) {
        this.aliveConnections = aliveConnections;
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

    public String getManagerUrl() {
        return managerUrl;
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

    public int getProxyUpdateIntervalMinutes() {
        return proxyUpdateIntervalMinutes;
    }

    public void setProxyUpdateIntervalMinutes(int proxyUpdateIntervalMinutes) {
        this.proxyUpdateIntervalMinutes = proxyUpdateIntervalMinutes;
    }

    public int getProxyUpdateMaxRetry() {
        return proxyUpdateMaxRetry;
    }

    public void setProxyUpdateMaxRetry(int proxyUpdateMaxRetry) {
        this.proxyUpdateMaxRetry = proxyUpdateMaxRetry;
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

    public String getRsaPubKeyUrl() {
        return rsaPubKeyUrl;
    }

    public boolean isNeedDataEncry() {
        return isNeedDataEncry;
    }

    public boolean isNeedAuthentication() {
        return this.needAuthentication;
    }

    public void setAuthenticationInfo(boolean needAuthentication, boolean needDataEncry,
            final String userName, final String secretKey) {
        this.needAuthentication = needAuthentication;
        this.isNeedDataEncry = needDataEncry;
        if (this.needAuthentication || this.isNeedDataEncry) {
            if (StringUtils.isBlank(userName)) {
                throw new IllegalArgumentException("userName is Blank!");
            }
            if (StringUtils.isBlank(secretKey)) {
                throw new IllegalArgumentException("secretKey is Blank!");
            }
        }
        this.userName = userName.trim();
        this.secretKey = secretKey.trim();
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

    public String getUserName() {
        return userName;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isReadProxyIPFromLocal() {
        return readProxyIPFromLocal;
    }

    public void setReadProxyIPFromLocal(boolean readProxyIPFromLocal) {
        this.readProxyIPFromLocal = readProxyIPFromLocal;
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

    public int getManagerSocketTimeout() {
        return managerSocketTimeout;
    }

    public void setManagerSocketTimeout(int managerSocketTimeout) {
        this.managerSocketTimeout = managerSocketTimeout;
    }

    public boolean isCleanHttpCacheWhenClosing() {
        return cleanHttpCacheWhenClosing;
    }

    public void setCleanHttpCacheWhenClosing(boolean cleanHttpCacheWhenClosing) {
        this.cleanHttpCacheWhenClosing = cleanHttpCacheWhenClosing;
    }

    public long getMaxProxyCacheTimeInMs() {
        return maxProxyCacheTimeInMs;
    }

    public void setMaxProxyCacheTimeInMs(long maxProxyCacheTimeInMs) {
        this.maxProxyCacheTimeInMs = maxProxyCacheTimeInMs;
    }

    public int getManagerConnectionTimeout() {
        return managerConnectionTimeout;
    }

    public void setManagerConnectionTimeout(int managerConnectionTimeout) {
        this.managerConnectionTimeout = managerConnectionTimeout;
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

    public int getVirtualNode() {
        return virtualNode;
    }

    public void setVirtualNode(int virtualNode) {
        this.virtualNode = virtualNode;
    }

    public LoadBalance getLoadBalance() {
        return loadBalance;
    }

    public void setLoadBalance(LoadBalance loadBalance) {
        this.loadBalance = loadBalance;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }
    public int getSenderMaxAttempt() {
        return senderMaxAttempt;
    }

    public void setSenderAttempt(int senderMaxAttempt) {
        this.senderMaxAttempt = senderMaxAttempt;
    }
}

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

package org.apache.inlong.sdk.dataproxy.common;

import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.metric.MetricConfig;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Configuration settings related to Manager metadata synchronization:
 * 1. Manager address, visit protocol, authentication info
 * 2. requested groupId, username info
 * 3. meta cache setting info
 * 4. other setting info
 */
public class ProxyClientConfig implements Cloneable {

    protected static final Logger logger = LoggerFactory.getLogger(ProxyClientConfig.class);
    // whether to access the manager via https
    private boolean visitMgrByHttps = false;
    private String tlsVersion = "TLSv1.2";
    // manager ip
    private String managerIP;
    // manager port
    private int managerPort;
    // whether enable authentication
    private boolean enableMgrAuthz = false;
    // auth secret id
    private String mgrAuthSecretId = "";
    // auth secret key
    private String mgrAuthSecretKey = "";
    // sdk report group Id
    private final String inlongGroupId;
    // sdk report protocol
    private final String dataRptProtocol;
    // sdk report region name
    private String regionName = SdkConsts.VAL_DEF_REGION_NAME;
    // group meta config key
    private String groupMetaConfigKey;
    // manager socket timeout ms
    private int mgrSocketTimeoutMs = SdkConsts.VAL_DEF_MGR_SOCKET_TIMEOUT_MS;
    // manager connect timeout ms
    private int mgrConnTimeoutMs = SdkConsts.VAL_DEF_TCP_CONNECT_TIMEOUT_MS;
    // whether to start using local proxy configure
    private boolean onlyUseLocalProxyConfig = false;
    // max retry count if meta query
    private int metaQryMaxRetryIfFail = SdkConsts.VAL_DEF_RETRY_IF_CONFIG_SYNC_FAIL;
    // meta sync interval ms
    private long mgrMetaSyncInrMs =
            SdkConsts.VAL_DEF_CONFIG_SYNC_INTERVAL_MIN * SdkConsts.VAL_UNIT_MIN_TO_MS;
    // max retry count if meta query failure
    private int metaQueryMaxRetryIfFail = SdkConsts.VAL_DEF_RETRY_IF_CONFIG_QUERY_FAIL;
    // query wait ms if meta query failure
    private int metaQueryWaitMsIfFail = SdkConsts.VAL_DEF_WAIT_MS_IF_CONFIG_QUERY_FAIL;
    // max retry count if meta sync failure
    private int metaSyncMaxRetryIfFail = SdkConsts.VAL_DEF_RETRY_IF_CONFIG_SYNC_FAIL;
    // sync wait ms if meta sync failure
    private int metaSyncWaitMsIfFail = SdkConsts.VAL_DEF_WAIT_MS_IF_CONFIG_SYNC_FAIL;
    // meta cache storage base path
    private String metaStoreBasePath = System.getProperty("user.dir");
    // max expired time for meta cache.
    private long metaCacheExpiredMs = SdkConsts.VAL_DEF_CACHE_CONFIG_EXPIRED_MS;
    // max expired time for meta query failure status
    private long metaQryFailCacheExpiredMs = SdkConsts.VAL_DEF_CONFIG_FAIL_STATUS_EXPIRED_MS;
    // maximum number of concurrent nodes
    private int aliveConnections = SdkConsts.VAL_DEF_ALIVE_CONNECTIONS;
    // node forced selection interval ms
    private long forceReChooseInrMs = SdkConsts.VAL_DEF_FORCE_CHOOSE_INR_MS;
    // metric setting
    private MetricConfig metricConfig = new MetricConfig();
    // report setting
    private boolean enableReportAuthz = false;
    private boolean enableReportEncrypt = false;
    private String rptRsaPubKeyUrl = "";
    private String rptUserName = "";
    private String rptSecretKey = "";

    protected ProxyClientConfig(boolean visitMgrByHttps, String managerIP, int managerPort,
            String groupId, ReportProtocol rptProtocol, String regionName) throws ProxySdkException {
        this.visitMgrByHttps = visitMgrByHttps;
        if (StringUtils.isBlank(managerIP)) {
            throw new ProxySdkException("managerIP is Blank!");
        }
        this.managerIP = managerIP.trim();
        if (managerPort <= 0) {
            throw new ProxySdkException("managerPort <= 0!");
        }
        this.managerPort = managerPort;
        if (StringUtils.isBlank(groupId)) {
            throw new ProxySdkException("groupId is Blank!");
        }
        this.inlongGroupId = groupId.trim();
        this.dataRptProtocol = rptProtocol.name();
        this.setRegionName(regionName);
        this.groupMetaConfigKey = ProxyUtils.buildGroupIdConfigKey(
                this.dataRptProtocol, this.regionName, this.inlongGroupId);
    }

    protected ProxyClientConfig(String managerAddress,
            String groupId, ReportProtocol rptProtocol, String regionName) throws ProxySdkException {
        this.checkAndParseAddress(managerAddress);
        if (StringUtils.isBlank(groupId)) {
            throw new ProxySdkException("groupId is Blank!");
        }
        this.inlongGroupId = groupId.trim();
        this.dataRptProtocol = rptProtocol.name();
        this.setRegionName(regionName);
        this.groupMetaConfigKey = ProxyUtils.buildGroupIdConfigKey(
                this.dataRptProtocol, this.regionName, this.inlongGroupId);
    }

    public void setMgrAuthzInfo(boolean needMgrAuthz,
            String mgrAuthSecretId, String mgrAuthSecretKey) throws ProxySdkException {
        this.enableMgrAuthz = needMgrAuthz;
        if (!this.enableMgrAuthz) {
            return;
        }
        if (StringUtils.isBlank(mgrAuthSecretId)) {
            throw new ProxySdkException("mgrAuthSecretId is Blank!");
        }
        if (StringUtils.isBlank(mgrAuthSecretKey)) {
            throw new ProxySdkException("mgrAuthSecretKey is Blank!");
        }
        this.mgrAuthSecretId = mgrAuthSecretId.trim();
        this.mgrAuthSecretKey = mgrAuthSecretKey.trim();
    }

    public void setRptAuthzAndEncryptInfo(Boolean enableRptAuthz, Boolean enableRptEncrypt,
            String rptUserName, String rptSecretKey, String rptRsaPubKeyUrl) throws ProxySdkException {
        if (enableRptAuthz != null) {
            this.enableReportAuthz = enableRptAuthz;
        }
        if (enableRptEncrypt != null) {
            this.enableReportEncrypt = enableRptEncrypt;
        }
        if (this.enableReportAuthz) {
            if (StringUtils.isBlank(rptUserName)) {
                throw new ProxySdkException("rptUserName is Blank!");
            }
            if (StringUtils.isBlank(rptSecretKey)) {
                throw new ProxySdkException("rptSecretKey is Blank!");
            }
            this.rptUserName = rptUserName.trim();
            this.rptSecretKey = rptSecretKey.trim();
        }
        if (this.enableReportEncrypt) {
            if (StringUtils.isBlank(rptUserName)) {
                throw new ProxySdkException("rptUserName is Blank!");
            }
            if (StringUtils.isBlank(rptRsaPubKeyUrl)) {
                throw new ProxySdkException("rptRsaPubKeyUrl is Blank!");
            }
            this.rptUserName = rptUserName.trim();
            this.rptRsaPubKeyUrl = rptRsaPubKeyUrl.trim();
        }
    }

    public boolean isVisitMgrByHttps() {
        return visitMgrByHttps;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }

    public void setTlsVersion(String tlsVersion) {
        if (StringUtils.isNotBlank(tlsVersion)) {
            this.tlsVersion = tlsVersion.trim();
        }
    }

    public String getManagerIP() {
        return managerIP;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public boolean isEnableMgrAuthz() {
        return enableMgrAuthz;
    }

    public String getMgrAuthSecretId() {
        return mgrAuthSecretId;
    }

    public String getMgrAuthSecretKey() {
        return mgrAuthSecretKey;
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public String getDataRptProtocol() {
        return dataRptProtocol;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getGroupMetaConfigKey() {
        return groupMetaConfigKey;
    }

    public void setRegionName(String regionName) {
        if (StringUtils.isNotBlank(regionName)) {
            this.regionName = regionName.trim().toLowerCase();
            this.groupMetaConfigKey = ProxyUtils.buildGroupIdConfigKey(
                    this.dataRptProtocol, this.regionName, this.inlongGroupId);
        }
    }

    public int getMgrSocketTimeoutMs() {
        return mgrSocketTimeoutMs;
    }

    public void setMgrSocketTimeoutMs(int mgrSocketTimeoutMs) {
        this.mgrSocketTimeoutMs =
                Math.min(SdkConsts.VAL_MAX_SOCKET_TIMEOUT_MS,
                        Math.max(SdkConsts.VAL_MIN_SOCKET_TIMEOUT_MS, mgrSocketTimeoutMs));
    }

    public int getMgrConnTimeoutMs() {
        return mgrConnTimeoutMs;
    }

    public void setMgrConnTimeoutMs(int mgrConnTimeoutMs) {
        this.mgrConnTimeoutMs =
                Math.min(SdkConsts.VAL_MAX_CONNECT_TIMEOUT_MS,
                        Math.max(SdkConsts.VAL_MIN_CONNECT_TIMEOUT_MS, mgrConnTimeoutMs));
    }

    public boolean isOnlyUseLocalProxyConfig() {
        return onlyUseLocalProxyConfig;
    }

    public void setOnlyUseLocalProxyConfig(boolean onlyUseLocalProxyConfig) {
        this.onlyUseLocalProxyConfig = onlyUseLocalProxyConfig;
    }

    public long getMgrMetaSyncInrMs() {
        return mgrMetaSyncInrMs;
    }

    public void setMgrMetaSyncInrMin(int mgrMetaSyncInrMin) {
        int tmpValue =
                Math.min(SdkConsts.VAL_MAX_CONFIG_SYNC_INTERVAL_MIN,
                        Math.max(SdkConsts.VAL_MIN_CONFIG_SYNC_INTERVAL_MIN, mgrMetaSyncInrMin));
        this.mgrMetaSyncInrMs = tmpValue * SdkConsts.VAL_UNIT_MIN_TO_MS;
    }

    public int getMetaQueryMaxRetryIfFail() {
        return metaQueryMaxRetryIfFail;
    }

    public void setMetaQueryMaxRetryIfFail(int metaQueryMaxRetryIfFail) {
        this.metaQueryMaxRetryIfFail = metaQueryMaxRetryIfFail;
    }

    public int getMetaQueryWaitMsIfFail() {
        return metaQueryWaitMsIfFail;
    }

    public void setMetaQueryWaitMsIfFail(int metaQueryWaitMsIfFail) {
        this.metaQueryWaitMsIfFail = Math.min(SdkConsts.VAL_MAX_WAIT_MS_IF_CONFIG_REQ_FAIL,
                Math.max(SdkConsts.VAL_MIN_WAIT_MS_IF_CONFIG_REQ_FAIL, metaQueryWaitMsIfFail));
    }

    public int getMetaSyncMaxRetryIfFail() {
        return metaSyncMaxRetryIfFail;
    }

    public void setMetaSyncMaxRetryIfFail(int metaSyncMaxRetryIfFail) {
        this.metaSyncMaxRetryIfFail =
                Math.min(metaSyncMaxRetryIfFail, SdkConsts.VAL_MAX_RETRY_IF_CONFIG_SYNC_FAIL);
    }

    public int getMetaSyncWaitMsIfFail() {
        return metaSyncWaitMsIfFail;
    }

    public void setMetaSyncWaitMsIfFail(int metaSyncWaitMsIfFail) {
        this.metaSyncWaitMsIfFail = Math.min(SdkConsts.VAL_MAX_WAIT_MS_IF_CONFIG_REQ_FAIL,
                Math.max(SdkConsts.VAL_MIN_WAIT_MS_IF_CONFIG_REQ_FAIL, metaSyncWaitMsIfFail));

    }

    public String getMetaStoreBasePath() {
        return metaStoreBasePath;
    }

    public void setMetaStoreBasePath(String metaStoreBasePath) {
        if (StringUtils.isBlank(metaStoreBasePath)) {
            return;
        }
        this.metaStoreBasePath = metaStoreBasePath.trim();
    }

    public long getMetaCacheExpiredMs() {
        return metaCacheExpiredMs;
    }

    public void setMetaCacheExpiredMs(long metaCacheExpiredMs) {
        this.metaCacheExpiredMs = metaCacheExpiredMs;
    }

    public long getMetaQryFailCacheExpiredMs() {
        return metaQryFailCacheExpiredMs;
    }

    public void setMetaQryFailCacheExpiredMs(long metaQryFailCacheExpiredMs) {
        this.metaQryFailCacheExpiredMs =
                Math.min(metaQryFailCacheExpiredMs, SdkConsts.VAL_MAX_CONFIG_FAIL_STATUS_EXPIRED_MS);
    }

    public int getAliveConnections() {
        return aliveConnections;
    }

    public void setAliveConnections(int aliveConnections) {
        this.aliveConnections =
                Math.max(SdkConsts.VAL_MIN_ALIVE_CONNECTIONS, aliveConnections);
    }

    public long getForceReChooseInrMs() {
        return forceReChooseInrMs;
    }

    public void setForceReChooseInrMs(long forceReChooseInrMs) {
        this.forceReChooseInrMs =
                Math.max(SdkConsts.VAL_MIN_FORCE_CHOOSE_INR_MS, forceReChooseInrMs);
    }

    public boolean isEnableReportAuthz() {
        return enableReportAuthz;
    }

    public boolean isEnableReportEncrypt() {
        return enableReportEncrypt;
    }

    public String getRptRsaPubKeyUrl() {
        return rptRsaPubKeyUrl;
    }

    public String getRptUserName() {
        return rptUserName;
    }

    public String getRptSecretKey() {
        return rptSecretKey;
    }

    public boolean isEnableMetric() {
        return metricConfig.isEnableMetric();
    }

    public void setMetricConfig(MetricConfig metricConfig) {
        if (metricConfig == null) {
            throw new IllegalArgumentException("metricConfig is null");
        }
        this.metricConfig = metricConfig;
    }

    public MetricConfig getMetricConfig() {
        return metricConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProxyClientConfig that = (ProxyClientConfig) o;
        return visitMgrByHttps == that.visitMgrByHttps
                && managerPort == that.managerPort
                && enableMgrAuthz == that.enableMgrAuthz
                && mgrSocketTimeoutMs == that.mgrSocketTimeoutMs
                && mgrConnTimeoutMs == that.mgrConnTimeoutMs
                && onlyUseLocalProxyConfig == that.onlyUseLocalProxyConfig
                && mgrMetaSyncInrMs == that.mgrMetaSyncInrMs
                && metaSyncMaxRetryIfFail == that.metaSyncMaxRetryIfFail
                && metaCacheExpiredMs == that.metaCacheExpiredMs
                && metaQryFailCacheExpiredMs == that.metaQryFailCacheExpiredMs
                && aliveConnections == that.aliveConnections
                && forceReChooseInrMs == that.forceReChooseInrMs
                && enableReportAuthz == that.enableReportAuthz
                && enableReportEncrypt == that.enableReportEncrypt
                && Objects.equals(tlsVersion, that.tlsVersion)
                && Objects.equals(managerIP, that.managerIP)
                && Objects.equals(mgrAuthSecretId, that.mgrAuthSecretId)
                && Objects.equals(mgrAuthSecretKey, that.mgrAuthSecretKey)
                && Objects.equals(inlongGroupId, that.inlongGroupId)
                && Objects.equals(dataRptProtocol, that.dataRptProtocol)
                && Objects.equals(regionName, that.regionName)
                && Objects.equals(groupMetaConfigKey, that.groupMetaConfigKey)
                && Objects.equals(metaStoreBasePath, that.metaStoreBasePath)
                && Objects.equals(metricConfig, that.metricConfig)
                && Objects.equals(rptRsaPubKeyUrl, that.rptRsaPubKeyUrl)
                && Objects.equals(rptUserName, that.rptUserName)
                && Objects.equals(rptSecretKey, that.rptSecretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(visitMgrByHttps, tlsVersion,
                managerIP, managerPort, enableMgrAuthz,
                mgrAuthSecretId, mgrAuthSecretKey, inlongGroupId,
                dataRptProtocol, regionName, groupMetaConfigKey,
                mgrSocketTimeoutMs, mgrConnTimeoutMs, onlyUseLocalProxyConfig,
                mgrMetaSyncInrMs, metaSyncMaxRetryIfFail, metaStoreBasePath,
                metaCacheExpiredMs, metaQryFailCacheExpiredMs, aliveConnections,
                forceReChooseInrMs, metricConfig, enableReportAuthz, enableReportEncrypt,
                rptRsaPubKeyUrl, rptUserName, rptSecretKey);
    }

    @Override
    public ProxyClientConfig clone() {
        try {
            return (ProxyClientConfig) super.clone();
        } catch (Throwable ex) {
            logger.warn("Failed to clone ManagerConfig", ex);
            return null;
        }
    }

    protected String getSetting(StringBuilder strBuff) {
        strBuff.append(", visitMgrByHttps=").append(visitMgrByHttps)
                .append(", tlsVersion='").append(tlsVersion)
                .append("', managerIP='").append(managerIP)
                .append("', managerPort=").append(managerPort)
                .append(", enableMgrAuthz=").append(enableMgrAuthz)
                .append(", mgrAuthSecretId='").append(mgrAuthSecretId)
                .append("', mgrAuthSecretKey='").append(mgrAuthSecretKey)
                .append("', inlongGroupId='").append(inlongGroupId)
                .append("', dataRptProtocol='").append(dataRptProtocol)
                .append("', regionName='").append(regionName)
                .append("', groupMetaConfigKey='").append(groupMetaConfigKey)
                .append("', mgrSocketTimeoutMs=").append(mgrSocketTimeoutMs)
                .append(", mgrConnTimeoutMs=").append(mgrConnTimeoutMs)
                .append(", onlyUseLocalProxyConfig=").append(onlyUseLocalProxyConfig)
                .append(", mgrMetaSyncInrMs=").append(mgrMetaSyncInrMs)
                .append(", metaSyncMaxRetryIfFail=").append(metaSyncMaxRetryIfFail)
                .append(", metaStoreBasePath='").append(metaStoreBasePath)
                .append("', metaCacheExpiredMs=").append(metaCacheExpiredMs)
                .append(", metaQryFailCacheExpiredMs=").append(metaQryFailCacheExpiredMs)
                .append(", aliveConnections=").append(aliveConnections)
                .append(", forceReChooseInrMs=").append(forceReChooseInrMs)
                .append(", enableReportAuthz=").append(enableReportAuthz)
                .append(", enableReportEncrypt=").append(enableReportEncrypt)
                .append(", rptRsaPubKeyUrl='").append(rptRsaPubKeyUrl)
                .append("', rptUserName='").append(rptUserName)
                .append("', rptSecretKey='").append(rptSecretKey)
                .append("', metricConfig=");
        metricConfig.getSetting(strBuff);
        return strBuff.append("}").toString();
    }

    private void checkAndParseAddress(String managerAddress) throws ProxySdkException {
        if (StringUtils.isBlank(managerAddress)
                || (!managerAddress.startsWith(SdkConsts.PREFIX_HTTP)
                        && !managerAddress.startsWith(SdkConsts.PREFIX_HTTPS))) {
            throw new ProxySdkException("managerAddress is blank or missing http/https protocol");
        }
        String hostPortInfo;
        if (managerAddress.startsWith(SdkConsts.PREFIX_HTTPS)) {
            this.visitMgrByHttps = true;
            hostPortInfo = managerAddress.substring(SdkConsts.PREFIX_HTTPS.length());
        } else {
            hostPortInfo = managerAddress.substring(SdkConsts.PREFIX_HTTP.length());
        }
        if (StringUtils.isBlank(hostPortInfo)) {
            throw new ProxySdkException("managerAddress must include host:port info!");
        }
        // parse ip and port
        String[] fields = hostPortInfo.split(":");
        if (fields.length == 1) {
            throw new ProxySdkException("managerAddress must include port info!");
        } else if (fields.length > 2) {
            throw new ProxySdkException("managerAddress must only include host:port info!");
        }
        // get manager ip
        if (StringUtils.isBlank(fields[0])) {
            throw new ProxySdkException("managerAddress's host is blank!");
        }
        this.managerIP = fields[0].trim();
        // get manager port
        if (StringUtils.isBlank(fields[1])) {
            throw new ProxySdkException("managerAddress's port is blank!");
        }
        int tmValue;
        try {
            tmValue = Integer.parseInt(fields[1].trim());
        } catch (Throwable ex) {
            throw new ProxySdkException("managerAddress's port must be number!");
        }
        if (tmValue <= 0) {
            throw new ProxySdkException("managerAddress's port must be > 0!");
        }
        this.managerPort = tmValue;
    }
}

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

import org.apache.inlong.common.enums.InlongCompressType;
import org.apache.inlong.dataproxy.sink.common.DefaultEventHandler;
import org.apache.inlong.dataproxy.sink.mq.AllCacheClusterSelector;
import org.apache.inlong.dataproxy.utils.AddressUtils;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * common.properties Configure Holder
 */
public class CommonConfigHolder {

    private static final Logger LOG = LoggerFactory.getLogger(CommonConfigHolder.class);
    // configure file name
    private static final String COMMON_CONFIG_FILE_NAME = "common.properties";
    // list split separator
    private static final String VAL_CONFIG_ITEMS_SEPARATOR = ",|\\s+";
    // **** allowed keys and default value, begin
    // proxy node id
    @Deprecated
    private static final String KEY_PROXY_NODE_ID = "nodeId";
    private static final String KEY_PROXY_NODE_IDV2 = "proxy.node.id";
    private static final String VAL_DEF_PROXY_NODE_ID = "127.0.0.1";
    // cluster tag
    private static final String KEY_PROXY_CLUSTER_TAG = "proxy.cluster.tag";
    private static final String VAL_DEF_CLUSTER_TAG = "default_cluster";
    // cluster name
    private static final String KEY_PROXY_CLUSTER_NAME = "proxy.cluster.name";
    private static final String VAL_DEF_CLUSTER_NAME = "default_dataproxy";
    // cluster incharges
    @Deprecated
    private static final String KEY_PROXY_CLUSTER_INCHARGES = "proxy.cluster.inCharges";
    private static final String KEY_PROXY_CLUSTER_INCHARGESV2 = "proxy.cluster.incharges";
    private static final String VAL_DEF_CLUSTER_INCHARGES = "admin";
    // cluster exttag,
    @Deprecated
    private static final String KEY_PROXY_CLUSTER_EXT_TAG = "proxy.cluster.extTag";
    private static final String KEY_PROXY_CLUSTER_EXT_TAGV2 = "proxy.cluster.ext.tag";
    // predefined format of ext tag: {key}={value}
    private static final String VAL_DEF_CLUSTER_EXT_TAG = "default=true";
    // manager hosts
    private static final String KEY_MANAGER_HOSTS = "manager.hosts";
    private static final String KEY_MANAGER_HOST_PORT_SEPARATOR = ":";
    // manager auth secret id
    @Deprecated
    private static final String KEY_MANAGER_AUTH_SECRET_ID = "manager.auth.secretId";
    private static final String KEY_MANAGER_AUTH_SECRET_IDV2 = "manager.auth.secret.id";
    // manager auth secret key
    @Deprecated
    private static final String KEY_MANAGER_AUTH_SECRET_KEY = "manager.auth.secretKey";
    private static final String KEY_MANAGER_AUTH_SECRET_KEYV2 = "manager.auth.secret.key";
    // configure file check interval
    @Deprecated
    private static final String KEY_CONFIG_CHECK_INTERVAL_MS = "configCheckInterval";
    private static final String KEY_META_CONFIG_SYNC_INTERVAL_MS = "meta.config.sync.interval.ms";
    private static final long VAL_DEF_CONFIG_SYNC_INTERVAL_MS = 60000L;
    private static final long VAL_MIN_CONFIG_SYNC_INTERVAL_MS = 10000L;
    // max allowed wait duration
    private static final String KEY_META_CONFIG_SYNC_WAST_ALARM_MS = "meta.config.sync.wast.alarm.ms";
    private static final long VAL_DEF_META_CONFIG_SYNC_WAST_ALARM_MS = 40000L;
    // whether to startup using the local metadata.json file without connecting to the Manager
    @Deprecated
    private static final String KEY_ENABLE_STARTUP_USING_LOCAL_META_FILE =
            "startup.using.local.meta.file.enable";
    private static final String KEY_ENABLE_STARTUP_USING_LOCAL_META_FILEV2 =
            "meta.config.startup.using.local.file.enable";
    private static final boolean VAL_DEF_ENABLE_STARTUP_USING_LOCAL_META_FILE = false;
    // whether enable file metric, optional field.
    private static final String KEY_ENABLE_FILE_METRIC = "file.metric.enable";
    private static final boolean VAL_DEF_ENABLE_FILE_METRIC = true;
    // file metric statistic interval (second)
    private static final String KEY_FILE_METRIC_STAT_INTERVAL_SEC = "file.metric.stat.interval.sec";
    private static final int VAL_DEF_FILE_METRIC_STAT_INVL_SEC = 60;
    private static final int VAL_MIN_FILE_METRIC_STAT_INVL_SEC = 0;
    // file metric max statistic key count
    private static final String KEY_FILE_METRIC_MAX_CACHE_CNT = "file.metric.max.cache.cnt";
    private static final int VAL_DEF_FILE_METRIC_MAX_CACHE_CNT = 1000000;
    private static final int VAL_MIN_FILE_METRIC_MAX_CACHE_CNT = 0;
    // source metric statistic name
    private static final String KEY_FILE_METRIC_SOURCE_OUTPUT_NAME = "file.metric.source.output.name";
    private static final String VAL_DEF_FILE_METRIC_SOURCE_OUTPUT_NAME = "Source";
    // sink metric statistic name
    private static final String KEY_FILE_METRIC_SINK_OUTPUT_NAME = "file.metric.sink.output.name";
    private static final String VAL_DEF_FILE_METRIC_SINK_OUTPUT_NAME = "Sink";
    // event metric statistic name
    private static final String KEY_FILE_METRIC_EVENT_OUTPUT_NAME = "file.metric.event.output.name";
    private static final String VAL_DEF_FILE_METRIC_EVENT_OUTPUT_NAME = "Stats";
    // prometheus http port
    @Deprecated
    private static final String KEY_PROMETHEUS_HTTP_PORT = "prometheusHttpPort";
    private static final String KEY_PROMETHEUS_HTTP_PORTV2 = "online.metric.prometheus.http.port";
    private static final int VAL_DEF_PROMETHEUS_HTTP_PORT = 8080;
    // Audit fields
    private static final String KEY_ENABLE_AUDIT = "audit.enable";
    private static final boolean VAL_DEF_ENABLE_AUDIT = true;
    private static final String KEY_AUDIT_PROXYS_DISCOVERY_MANAGER_ENABLE = "audit.proxys.discovery.manager.enable";
    private static final boolean VAL_DEF_AUDIT_PROXYS_DISCOVERY_MANAGER_ENABLE = false;
    private static final String KEY_AUDIT_PROXYS = "audit.proxys";
    @Deprecated
    private static final String KEY_AUDIT_FILE_PATH = "audit.filePath";
    private static final String KEY_AUDIT_FILE_PATHV2 = "audit.file.path";
    private static final String VAL_DEF_AUDIT_FILE_PATH = "/data/inlong/audit/";
    @Deprecated
    private static final String KEY_AUDIT_MAX_CACHE_ROWS = "audit.maxCacheRows";
    private static final String KEY_AUDIT_MAX_CACHE_ROWSV2 = "audit.max.cache.rows";
    private static final int VAL_DEF_AUDIT_MAX_CACHE_ROWS = 2000000;
    @Deprecated
    private static final String KEY_AUDIT_FORMAT_INTERVAL_MS = "auditFormatInterval";
    private static final String KEY_AUDIT_TIME_FORMAT_INTERVAL = "audit.time.format.intvl.ms";
    private static final long VAL_DEF_AUDIT_FORMAT_INTERVAL_MS = 60000L;

    // v1 msg whether response by sink
    private static final String KEY_V1MSG_RESPONSE_BY_SINK = "isResponseAfterSave";
    private static final String KEY_V1MSG_RESPONSE_BY_SINKV2 = "proxy.v1msg.response.by.sink.enable";
    private static final boolean VAL_DEF_V1MSG_RESPONSE_BY_SINK = false;
    // v1 msg sent compress type
    @Deprecated
    private static final String KEY_V1MSG_SENT_COMPRESS_TYPE = "compressType";
    private static final String KEY_V1MSG_SENT_COMPRESS_TYPEV2 = "proxy.v1msg.compress.type";
    private static final InlongCompressType VAL_DEF_V1MSG_COMPRESS_TYPE = InlongCompressType.INLONG_SNAPPY;
    // Same as KEY_MAX_RESPONSE_TIMEOUT_MS = "maxResponseTimeoutMs";
    private static final String KEY_MAX_RAS_TIMEOUT_MS = "maxRASTimeoutMs";
    private static final long VAL_DEF_MAX_RAS_TIMEOUT_MS = 10000L;

    // max buffer queue size in KB
    @Deprecated
    private static final String KEY_DEF_BUFFERQUEUE_SIZE_KB = "maxBufferQueueSizeKb";
    private static final String KEY_DEF_BUFFERQUEUE_SIZE_KBV2 = "proxy.def.buffer.queue.size.KB";
    private static final int VAL_DEF_BUFFERQUEUE_SIZE_KB = 128 * 1024;
    // whether to retry after the message send failure
    @Deprecated
    private static final String KEY_ENABLE_SEND_RETRY_AFTER_FAILURE = "send.retry.after.failure";
    private static final String KEY_ENABLE_SEND_RETRY_AFTER_FAILUREV2 = "msg.send.failure.retry.enable";
    private static final boolean VAL_DEF_ENABLE_SEND_RETRY_AFTER_FAILURE = true;
    // max retry count
    private static final String KEY_MAX_RETRIES_AFTER_FAILURE = "max.retries.after.failure";
    private static final String KEY_MAX_RETRIES_AFTER_FAILUREV2 = "msg.max.retries";
    private static final int VAL_DEF_MAX_RETRIES_AFTER_FAILURE = -1;
    // whether to accept messages without mapping between groupId/streamId and topic
    private static final String KEY_ENABLE_UNCONFIGURED_TOPIC_ACCEPT = "id2topic.unconfigured.accept.enable";
    private static final boolean VAL_DEF_ENABLE_UNCONFIGURED_TOPIC_ACCEPT = false;
    // default topics configure key, multiple topic settings are separated by "\\s+".
    private static final String KEY_UNCONFIGURED_TOPIC_DEFAULT_TOPICS = "id2topic.unconfigured.default.topics";
    // whether enable whitelist, optional field.
    @Deprecated
    private static final String KEY_ENABLE_WHITELIST = "proxy.enable.whitelist";
    private static final String KEY_ENABLE_WHITELISTV2 = "proxy.visit.whitelist.enable";
    private static final boolean VAL_DEF_ENABLE_WHITELIST = false;

    // event handler
    private static final String KEY_EVENT_HANDLER = "eventHandler";
    private static final String VAL_DEF_EVENT_HANDLER = DefaultEventHandler.class.getName();
    // cache cluster selector
    @Deprecated
    private static final String KEY_CACHE_CLUSTER_SELECTOR = "cacheClusterSelector";
    private static final String KEY_CACHE_CLUSTER_SELECTORV2 = "proxy.mq.cluster.selector";
    private static final String VAL_DEF_CACHE_CLUSTER_SELECTOR = AllCacheClusterSelector.class.getName();

    // **** allowed keys and default value, end

    // class instance
    private static CommonConfigHolder instance = null;
    private static volatile boolean isInit = false;
    private Map<String, String> props;
    // pre-read field values
    // node setting
    private String clusterTag = VAL_DEF_CLUSTER_TAG;
    private String clusterName = VAL_DEF_CLUSTER_NAME;
    private String clusterIncharges = VAL_DEF_CLUSTER_INCHARGES;
    private String clusterExtTag = VAL_DEF_CLUSTER_EXT_TAG;
    // manager setting
    private final List<String> managerIpList = new ArrayList<>();
    private String managerAuthSecretId = "";
    private String managerAuthSecretKey = "";
    private boolean enableStartupUsingLocalMetaFile = VAL_DEF_ENABLE_STARTUP_USING_LOCAL_META_FILE;
    private long metaConfigSyncInvlMs = VAL_DEF_CONFIG_SYNC_INTERVAL_MS;
    private long metaConfigWastAlarmMs = VAL_DEF_META_CONFIG_SYNC_WAST_ALARM_MS;
    private boolean enableAudit = VAL_DEF_ENABLE_AUDIT;
    private boolean enableAuditProxysDiscoveryFromManager = VAL_DEF_AUDIT_PROXYS_DISCOVERY_MANAGER_ENABLE;
    private final HashSet<String> auditProxys = new HashSet<>();
    private String auditFilePath = VAL_DEF_AUDIT_FILE_PATH;
    private int auditMaxCacheRows = VAL_DEF_AUDIT_MAX_CACHE_ROWS;
    private long auditFormatInvlMs = VAL_DEF_AUDIT_FORMAT_INTERVAL_MS;
    private boolean enableUnConfigTopicAccept = VAL_DEF_ENABLE_UNCONFIGURED_TOPIC_ACCEPT;
    private final List<String> defaultTopics = new ArrayList<>();
    private boolean enableFileMetric = VAL_DEF_ENABLE_FILE_METRIC;
    private int fileMetricStatInvlSec = VAL_DEF_FILE_METRIC_STAT_INVL_SEC;
    private int fileMetricStatCacheCnt = VAL_DEF_FILE_METRIC_MAX_CACHE_CNT;
    private String fileMetricSourceOutName = VAL_DEF_FILE_METRIC_SOURCE_OUTPUT_NAME;
    private String fileMetricSinkOutName = VAL_DEF_FILE_METRIC_SINK_OUTPUT_NAME;
    private String fileMetricEventOutName = VAL_DEF_FILE_METRIC_EVENT_OUTPUT_NAME;
    private InlongCompressType defV1MsgCompressType = VAL_DEF_V1MSG_COMPRESS_TYPE;
    private boolean defV1MsgResponseBySink = VAL_DEF_V1MSG_RESPONSE_BY_SINK;
    private long maxResAfterSaveTimeout = VAL_DEF_MAX_RAS_TIMEOUT_MS;
    private boolean enableWhiteList = VAL_DEF_ENABLE_WHITELIST;
    private int defBufferQueueSizeKB = VAL_DEF_BUFFERQUEUE_SIZE_KB;
    private String eventHandler = VAL_DEF_EVENT_HANDLER;
    private String cacheClusterSelector = VAL_DEF_CACHE_CLUSTER_SELECTOR;
    private String proxyNodeId = VAL_DEF_PROXY_NODE_ID;
    private int prometheusHttpPort = VAL_DEF_PROMETHEUS_HTTP_PORT;
    private boolean sendRetryAfterFailure = VAL_DEF_ENABLE_SEND_RETRY_AFTER_FAILURE;
    private int maxRetriesAfterFailure = VAL_DEF_MAX_RETRIES_AFTER_FAILURE;

    /**
     * get instance for common.properties config manager
     */
    public static CommonConfigHolder getInstance() {
        if (isInit && instance != null) {
            return instance;
        }
        synchronized (CommonConfigHolder.class) {
            if (!isInit) {
                instance = new CommonConfigHolder();
                if (instance.loadConfigFile()) {
                    instance.preReadFields();
                    LOG.info("{} load and read result is: {}", COMMON_CONFIG_FILE_NAME, instance.toString());
                }
                isInit = true;
            }
        }
        return instance;
    }

    /**
     * Get the original attribute map
     *
     * Notice: only the non-pre-read fields need to be searched from the attribute map,
     *         the pre-read fields MUST be got according to the methods in the class.
     */
    public Map<String, String> getProperties() {
        return this.props;
    }

    /**
     * getStringFromContext
     *
     * @param context
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getStringFromContext(Context context, String key, String defaultValue) {
        String value = context.getString(key);
        value = (value != null) ? value : getInstance().getProperties().getOrDefault(key, defaultValue);
        return value;
    }

    public String getClusterTag() {
        return clusterTag;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public String getClusterIncharges() {
        return clusterIncharges;
    }

    public String getClusterExtTag() {
        return clusterExtTag;
    }

    public long getMetaConfigSyncInvlMs() {
        return metaConfigSyncInvlMs;
    }

    public long getMetaConfigWastAlarmMs() {
        return metaConfigWastAlarmMs;
    }

    public boolean isEnableUnConfigTopicAccept() {
        return enableUnConfigTopicAccept;
    }

    public List<String> getDefTopics() {
        return defaultTopics;
    }

    public String getRandDefTopics() {
        if (defaultTopics.isEmpty()) {
            return null;
        }
        SecureRandom rand = new SecureRandom();
        return defaultTopics.get(rand.nextInt(defaultTopics.size()));
    }

    public boolean isEnableWhiteList() {
        return this.enableWhiteList;
    }

    public List<String> getManagerHosts() {
        return this.managerIpList;
    }

    public String getManagerAuthSecretId() {
        return managerAuthSecretId;
    }

    public String getManagerAuthSecretKey() {
        return managerAuthSecretKey;
    }

    public boolean isEnableAudit() {
        return enableAudit;
    }

    public boolean isEnableAuditProxysDiscoveryFromManager() {
        return enableAuditProxysDiscoveryFromManager;
    }

    public boolean isEnableFileMetric() {
        return enableFileMetric;
    }

    public int getFileMetricStatInvlSec() {
        return fileMetricStatInvlSec;
    }

    public int getFileMetricStatCacheCnt() {
        return fileMetricStatCacheCnt;
    }

    public HashSet<String> getAuditProxys() {
        return auditProxys;
    }

    public String getAuditFilePath() {
        return auditFilePath;
    }

    public int getAuditMaxCacheRows() {
        return auditMaxCacheRows;
    }

    public long getAuditFormatInvlMs() {
        return auditFormatInvlMs;
    }

    public boolean isDefV1MsgResponseBySink() {
        return defV1MsgResponseBySink;
    }

    public long getMaxResAfterSaveTimeout() {
        return maxResAfterSaveTimeout;
    }

    public int getDefBufferQueueSizeKB() {
        return defBufferQueueSizeKB;
    }

    public boolean isEnableStartupUsingLocalMetaFile() {
        return enableStartupUsingLocalMetaFile;
    }

    public String getEventHandler() {
        return eventHandler;
    }

    public String getCacheClusterSelector() {
        return cacheClusterSelector;
    }

    public int getPrometheusHttpPort() {
        return prometheusHttpPort;
    }

    public String getProxyNodeId() {
        return proxyNodeId;
    }

    public InlongCompressType getDefV1MsgCompressType() {
        return defV1MsgCompressType;
    }

    public String getFileMetricSourceOutName() {
        return fileMetricSourceOutName;
    }

    public String getFileMetricSinkOutName() {
        return fileMetricSinkOutName;
    }

    public String getFileMetricEventOutName() {
        return fileMetricEventOutName;
    }

    public boolean isSendRetryAfterFailure() {
        return sendRetryAfterFailure;
    }

    public int getMaxRetriesAfterFailure() {
        return maxRetriesAfterFailure;
    }

    private void preReadFields() {
        String tmpValue;
        // read cluster tag
        tmpValue = this.props.get(KEY_PROXY_CLUSTER_TAG);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.clusterTag = tmpValue.trim();
        }
        // read cluster name
        tmpValue = this.props.get(KEY_PROXY_CLUSTER_NAME);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.clusterName = tmpValue.trim();
        }
        // read cluster incharges
        tmpValue = compatGetValue(this.props,
                KEY_PROXY_CLUSTER_INCHARGESV2, KEY_PROXY_CLUSTER_INCHARGES);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.clusterIncharges = tmpValue.trim();
        }
        tmpValue = compatGetValue(this.props,
                KEY_PROXY_CLUSTER_EXT_TAGV2, KEY_PROXY_CLUSTER_EXT_TAG);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.clusterExtTag = tmpValue.trim();
        }
        // read the manager setting
        this.preReadManagerSetting();
        // read the configuration related to the default topics
        this.preReadDefTopicSetting();
        // read enable whitelist
        tmpValue = compatGetValue(this.props,
                KEY_ENABLE_WHITELISTV2, KEY_ENABLE_WHITELIST);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.enableWhiteList = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read max response after save timeout
        tmpValue = this.props.get(KEY_MAX_RAS_TIMEOUT_MS);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.maxResAfterSaveTimeout = NumberUtils.toLong(tmpValue.trim(), VAL_DEF_MAX_RAS_TIMEOUT_MS);
        }
        // read default buffer queue size
        tmpValue = compatGetValue(this.props,
                KEY_DEF_BUFFERQUEUE_SIZE_KBV2, KEY_DEF_BUFFERQUEUE_SIZE_KB);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.defBufferQueueSizeKB = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_BUFFERQUEUE_SIZE_KB);
        }
        // read cache cluster selector
        tmpValue = compatGetValue(this.props,
                KEY_CACHE_CLUSTER_SELECTORV2, KEY_CACHE_CLUSTER_SELECTOR);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.cacheClusterSelector = tmpValue.trim();
        }
        // read event handler
        tmpValue = this.props.get(KEY_EVENT_HANDLER);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.eventHandler = tmpValue.trim();
        }
        // read proxy node id
        tmpValue = compatGetValue(this.props,
                KEY_PROXY_NODE_IDV2, KEY_PROXY_NODE_ID);
        if (StringUtils.isBlank(tmpValue)) {
            this.proxyNodeId = AddressUtils.getSelfHost();
        } else {
            this.proxyNodeId = tmpValue.trim();
        }
        // read prometheus Http Port
        tmpValue = compatGetValue(this.props,
                KEY_PROMETHEUS_HTTP_PORTV2, KEY_PROMETHEUS_HTTP_PORT);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.prometheusHttpPort = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_PROMETHEUS_HTTP_PORT);
        }
        // read whether retry send message after sent failure
        tmpValue = compatGetValue(this.props,
                KEY_ENABLE_SEND_RETRY_AFTER_FAILUREV2, KEY_ENABLE_SEND_RETRY_AFTER_FAILURE);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.sendRetryAfterFailure = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read max retry count
        tmpValue = compatGetValue(this.props,
                KEY_MAX_RETRIES_AFTER_FAILUREV2, KEY_MAX_RETRIES_AFTER_FAILURE);
        if (StringUtils.isNotBlank(tmpValue)) {
            int retries = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_MAX_RETRIES_AFTER_FAILURE);
            if (retries >= 0) {
                this.maxRetriesAfterFailure = retries;
            }
        }
        // Pre-read the fields related to Audit
        this.preReadAuditSetting();
        // Pre-read the fields related to File metric
        this.preReadMetricSetting();
        // pre-read v1msg default setting
        this.preReadV1MsgSetting();
    }

    private void preReadManagerSetting() {
        String tmpValue;
        // read manager hosts
        String managerHosts = this.props.get(KEY_MANAGER_HOSTS);
        if (StringUtils.isBlank(managerHosts)) {
            LOG.error("Value of {} is required in {}, exit!", KEY_MANAGER_HOSTS, COMMON_CONFIG_FILE_NAME);
            System.exit(2);
        }
        managerHosts = managerHosts.trim();
        String[] hostPort;
        String[] hostPortList = managerHosts.split(VAL_CONFIG_ITEMS_SEPARATOR);
        for (String tmpItem : hostPortList) {
            if (StringUtils.isBlank(tmpItem)) {
                continue;
            }
            hostPort = tmpItem.split(KEY_MANAGER_HOST_PORT_SEPARATOR);
            if (hostPort.length != 2
                    || StringUtils.isBlank(hostPort[0])
                    || StringUtils.isBlank(hostPort[1])) {
                continue;
            }
            managerIpList.add(hostPort[0].trim() + KEY_MANAGER_HOST_PORT_SEPARATOR + hostPort[1].trim());
        }
        if (managerIpList.isEmpty()) {
            LOG.error("Invalid value {} in configure item {}, exit!", managerHosts, KEY_MANAGER_HOSTS);
            System.exit(2);
        }
        // read manager auth secret id
        tmpValue = compatGetValue(this.props,
                KEY_MANAGER_AUTH_SECRET_IDV2, KEY_MANAGER_AUTH_SECRET_ID);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.managerAuthSecretId = tmpValue.trim();
        }
        // read manager auth secret key
        tmpValue = compatGetValue(this.props,
                KEY_MANAGER_AUTH_SECRET_KEYV2, KEY_MANAGER_AUTH_SECRET_KEY);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.managerAuthSecretKey = tmpValue.trim();
        }
        // read enable startup using local meta file
        tmpValue = compatGetValue(this.props,
                KEY_ENABLE_STARTUP_USING_LOCAL_META_FILEV2, KEY_ENABLE_STARTUP_USING_LOCAL_META_FILE);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.enableStartupUsingLocalMetaFile = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read configure sync interval
        tmpValue = compatGetValue(this.props,
                KEY_META_CONFIG_SYNC_INTERVAL_MS, KEY_CONFIG_CHECK_INTERVAL_MS);
        if (StringUtils.isNotEmpty(tmpValue)) {
            long tmpSyncInvMs = NumberUtils.toLong(
                    tmpValue.trim(), VAL_DEF_CONFIG_SYNC_INTERVAL_MS);
            if (tmpSyncInvMs >= VAL_MIN_CONFIG_SYNC_INTERVAL_MS) {
                this.metaConfigSyncInvlMs = tmpSyncInvMs;
            }
        }
        // read configure sync wast alarm ms
        tmpValue = this.props.get(KEY_META_CONFIG_SYNC_WAST_ALARM_MS);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.metaConfigWastAlarmMs = NumberUtils.toLong(
                    tmpValue.trim(), VAL_DEF_META_CONFIG_SYNC_WAST_ALARM_MS);
        }
    }

    private void preReadDefTopicSetting() {
        String tmpValue;
        // read whether accept msg without id2topic configure
        tmpValue = this.props.get(KEY_ENABLE_UNCONFIGURED_TOPIC_ACCEPT);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.enableUnConfigTopicAccept = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read default topics
        tmpValue = this.props.get(KEY_UNCONFIGURED_TOPIC_DEFAULT_TOPICS);
        if (StringUtils.isNotBlank(tmpValue)) {
            String[] topicItems = tmpValue.split(VAL_CONFIG_ITEMS_SEPARATOR);
            for (String item : topicItems) {
                if (StringUtils.isBlank(item)) {
                    continue;
                }
                this.defaultTopics.add(item.trim());
            }
            LOG.info("Configured {}, size is {}, value is {}",
                    KEY_UNCONFIGURED_TOPIC_DEFAULT_TOPICS, defaultTopics.size(), defaultTopics);
        }
        // check whether configure default topics
        if (this.enableUnConfigTopicAccept && this.defaultTopics.isEmpty()) {
            LOG.error("Required {} field value is blank in {} for {} is true, exit!",
                    KEY_UNCONFIGURED_TOPIC_DEFAULT_TOPICS, COMMON_CONFIG_FILE_NAME,
                    KEY_ENABLE_UNCONFIGURED_TOPIC_ACCEPT);
            System.exit(2);
        }
    }

    private void preReadMetricSetting() {
        String tmpValue;
        // read whether enable file metric
        tmpValue = this.props.get(KEY_ENABLE_FILE_METRIC);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.enableFileMetric = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read file metric statistic interval
        tmpValue = this.props.get(KEY_FILE_METRIC_STAT_INTERVAL_SEC);
        if (StringUtils.isNotEmpty(tmpValue)) {
            int statInvl = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_FILE_METRIC_STAT_INVL_SEC);
            if (statInvl >= VAL_MIN_FILE_METRIC_STAT_INVL_SEC) {
                this.fileMetricStatInvlSec = statInvl;
            }
        }
        // read file metric statistic max cache count
        tmpValue = this.props.get(KEY_FILE_METRIC_MAX_CACHE_CNT);
        if (StringUtils.isNotEmpty(tmpValue)) {
            int maxCacheCnt = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_FILE_METRIC_MAX_CACHE_CNT);
            if (maxCacheCnt >= VAL_MIN_FILE_METRIC_MAX_CACHE_CNT) {
                this.fileMetricStatCacheCnt = maxCacheCnt;
            }
        }
        // read source file statistic output name
        tmpValue = this.props.get(KEY_FILE_METRIC_SOURCE_OUTPUT_NAME);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.fileMetricSourceOutName = tmpValue.trim();
        }
        // read sink file statistic output name
        tmpValue = this.props.get(KEY_FILE_METRIC_SINK_OUTPUT_NAME);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.fileMetricSinkOutName = tmpValue.trim();
        }
        // read event file statistic output name
        tmpValue = this.props.get(KEY_FILE_METRIC_EVENT_OUTPUT_NAME);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.fileMetricEventOutName = tmpValue.trim();
        }
    }

    private void preReadAuditSetting() {
        String tmpValue;
        // read whether enable audit
        tmpValue = this.props.get(KEY_ENABLE_AUDIT);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.enableAudit = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read whether discovery audit proxys from manager
        tmpValue = this.props.get(KEY_AUDIT_PROXYS_DISCOVERY_MANAGER_ENABLE);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.enableAuditProxysDiscoveryFromManager = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read audit proxys
        tmpValue = this.props.get(KEY_AUDIT_PROXYS);
        if (StringUtils.isNotBlank(tmpValue)) {
            String[] ipPorts = tmpValue.split(VAL_CONFIG_ITEMS_SEPARATOR);
            for (String tmpIPPort : ipPorts) {
                if (StringUtils.isBlank(tmpIPPort)) {
                    continue;
                }
                this.auditProxys.add(tmpIPPort.trim());
            }
        }
        // check auditProxys configure
        if (this.enableAudit) {
            if (!this.enableAuditProxysDiscoveryFromManager && this.auditProxys.isEmpty()) {
                LOG.error("{}'s {} must be configured when {} is true and {} is false, exist!",
                        COMMON_CONFIG_FILE_NAME, KEY_AUDIT_PROXYS, KEY_ENABLE_AUDIT,
                        KEY_AUDIT_PROXYS_DISCOVERY_MANAGER_ENABLE);
                System.exit(2);
            }
        }
        // read audit file path
        tmpValue = compatGetValue(this.props,
                KEY_AUDIT_FILE_PATHV2, KEY_AUDIT_FILE_PATH);
        if (StringUtils.isNotBlank(tmpValue)) {
            this.auditFilePath = tmpValue.trim();
        }
        // read audit max cache rows
        tmpValue = compatGetValue(this.props,
                KEY_AUDIT_MAX_CACHE_ROWSV2, KEY_AUDIT_MAX_CACHE_ROWS);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.auditMaxCacheRows = NumberUtils.toInt(
                    tmpValue.trim(), VAL_DEF_AUDIT_MAX_CACHE_ROWS);
        }
        // read audit format interval
        tmpValue = compatGetValue(this.props,
                KEY_AUDIT_TIME_FORMAT_INTERVAL, KEY_AUDIT_FORMAT_INTERVAL_MS);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.auditFormatInvlMs = NumberUtils.toLong(
                    tmpValue.trim(), VAL_DEF_AUDIT_FORMAT_INTERVAL_MS);
        }
    }

    private void preReadV1MsgSetting() {
        String tmpValue;
        // read whether response v1 message by sink
        tmpValue = compatGetValue(this.props,
                KEY_V1MSG_RESPONSE_BY_SINKV2, KEY_V1MSG_RESPONSE_BY_SINK);
        if (StringUtils.isNotEmpty(tmpValue)) {
            this.defV1MsgResponseBySink = "TRUE".equalsIgnoreCase(tmpValue.trim());
        }
        // read v1 msg compress type
        tmpValue = compatGetValue(this.props,
                KEY_V1MSG_SENT_COMPRESS_TYPEV2, KEY_V1MSG_SENT_COMPRESS_TYPE);
        if (StringUtils.isNotBlank(tmpValue)) {
            InlongCompressType tmpCompType = InlongCompressType.forType(tmpValue.trim());
            if (tmpCompType == InlongCompressType.UNKNOWN) {
                LOG.error("{}'s {}({}) must be in allowed range [{}], exist!", COMMON_CONFIG_FILE_NAME,
                        KEY_V1MSG_SENT_COMPRESS_TYPEV2, tmpValue, InlongCompressType.allowedCompressTypes);
                System.exit(2);
            }
            this.defV1MsgCompressType = tmpCompType;
        }
    }

    private boolean loadConfigFile() {
        InputStream inStream = null;
        try {
            URL url = getClass().getClassLoader().getResource(COMMON_CONFIG_FILE_NAME);
            inStream = url != null ? url.openStream() : null;
            if (inStream == null) {
                LOG.error("Fail to open {} as the input stream is null, exit!",
                        COMMON_CONFIG_FILE_NAME);
                System.exit(1);
                return false;
            }
            String strKey;
            String strVal;
            Properties tmpProps = new Properties();
            tmpProps.load(inStream);
            props = new HashMap<>(tmpProps.size());
            for (Map.Entry<Object, Object> entry : tmpProps.entrySet()) {
                if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                strKey = (String) entry.getKey();
                strVal = (String) entry.getValue();
                if (StringUtils.isBlank(strKey) || StringUtils.isBlank(strVal)) {
                    continue;
                }
                props.put(strKey.trim(), strVal.trim());
            }
            LOG.info("Read success from {}, content is {}", COMMON_CONFIG_FILE_NAME, props);
        } catch (Throwable e) {
            LOG.error("Fail to load properties from {}, exit!",
                    COMMON_CONFIG_FILE_NAME, e);
            System.exit(1);
            return false;
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOG.error("Fail to InputStream.close() for file {}, exit!",
                            COMMON_CONFIG_FILE_NAME, e);
                    System.exit(1);
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("props", props)
                .append("clusterTag", clusterTag)
                .append("clusterName", clusterName)
                .append("clusterIncharges", clusterIncharges)
                .append("clusterExtTag", clusterExtTag)
                .append("managerIpList", managerIpList)
                .append("managerAuthSecretId", managerAuthSecretId)
                .append("managerAuthSecretKey", managerAuthSecretKey)
                .append("enableStartupUsingLocalMetaFile", enableStartupUsingLocalMetaFile)
                .append("metaConfigSyncInvlMs", metaConfigSyncInvlMs)
                .append("metaConfigWastAlarmMs", metaConfigWastAlarmMs)
                .append("enableAudit", enableAudit)
                .append("enableAuditProxysDiscoveryFromManager",
                        enableAuditProxysDiscoveryFromManager)
                .append("auditProxys", auditProxys)
                .append("auditFilePath", auditFilePath)
                .append("auditMaxCacheRows", auditMaxCacheRows)
                .append("auditFormatInvlMs", auditFormatInvlMs)
                .append("enableUnConfigTopicAccept", enableUnConfigTopicAccept)
                .append("defaultTopics", defaultTopics)
                .append("enableFileMetric", enableFileMetric)
                .append("fileMetricStatInvlSec", fileMetricStatInvlSec)
                .append("fileMetricStatCacheCnt", fileMetricStatCacheCnt)
                .append("fileMetricSourceOutName", fileMetricSourceOutName)
                .append("fileMetricSinkOutName", fileMetricSinkOutName)
                .append("fileMetricEventOutName", fileMetricEventOutName)
                .append("defV1MsgCompressType", defV1MsgCompressType)
                .append("defV1MsgResponseBySink", defV1MsgResponseBySink)
                .append("maxResAfterSaveTimeout", maxResAfterSaveTimeout)
                .append("enableWhiteList", enableWhiteList)
                .append("defBufferQueueSizeKB", defBufferQueueSizeKB)
                .append("eventHandler", eventHandler)
                .append("cacheClusterSelector", cacheClusterSelector)
                .append("proxyNodeId", proxyNodeId)
                .append("prometheusHttpPort", prometheusHttpPort)
                .append("sendRetryAfterFailure", sendRetryAfterFailure)
                .append("maxRetriesAfterFailure", maxRetriesAfterFailure)
                .toString();
    }

    private String compatGetValue(Map<String, String> attrs, String newKey, String depKey) {
        String tmpValue = attrs.get(newKey);
        if (StringUtils.isBlank(tmpValue)) {
            tmpValue = attrs.get(depKey);
            if (StringUtils.isNotEmpty(tmpValue)) {
                LOG.warn("** Deprecated configure key {}, replaced by {} **", depKey, newKey);
            }
        }
        return tmpValue;
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.master;

import com.google.gson.GsonBuilder;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TServerConstants;
import org.apache.tubemq.corebase.config.Configuration;
import org.apache.tubemq.corebase.config.ConfigurationUtils;
import org.apache.tubemq.corebase.config.TLSConfig;
import org.apache.tubemq.corebase.config.TlsConfItems;
import org.apache.tubemq.corebase.config.constants.MasterCfgConst;
import org.apache.tubemq.corebase.config.constants.MasterReplicaCfgConst;
import org.apache.tubemq.corebase.utils.AddressUtils;
import org.apache.tubemq.corebase.utils.MixedUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corerpc.RpcConstants;
import org.apache.tubemq.server.common.fileconfig.AbstractFileConfig;
import org.apache.tubemq.server.common.fileconfig.MasterReplicationConfig;
import org.apache.tubemq.server.common.fileconfig.ZKConfig;
import org.ini4j.Ini;
import org.ini4j.Profile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic config for master service
 */
public class MasterConfig extends AbstractFileConfig {
    private static final Logger logger = LoggerFactory.getLogger(MasterConfig.class);

    private String hostName;
    private int port;
    private int webPort = MasterCfgConst.DEFAULT_MASTER_WEB_PORT;
    private MasterReplicationConfig replicationConfig = new MasterReplicationConfig();
    private Configuration tlsConfiguration;
    private ZKConfig zkConfig;
    private int consumerBalancePeriodMs = MasterCfgConst.DEFAULT_CONSUMER_BALANCE_PERIOD_MS;
    private int firstBalanceDelayAfterStartMs = MasterCfgConst.DEFAULT_FIRST_BALANCE_DELAY_AFTER_START_MS;
    private int consumerHeartbeatTimeoutMs = MasterCfgConst.DEFAULT_CONSUMER_HEARTBEAT_TIMEOUT_MS;
    private int producerHeartbeatTimeoutMs = MasterCfgConst.DEFAULT_PRODUCER_HEARTBEAT_TIMEOUT_MS;
    private int brokerHeartbeatTimeoutMs = MasterCfgConst.DEFAULT_BROKER_HEARTBEAT_TIMEOUT_MS;
    private long rpcReadTimeoutMs = RpcConstants.CFG_RPC_READ_TIMEOUT_DEFAULT_MS;
    private long nettyWriteBufferHighWaterMark = MasterCfgConst.DEFAULT_MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK;
    private long nettyWriteBufferLowWaterMark = MasterCfgConst.DEFAULT_MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK;
    private long onlineOnlyReadToRWPeriodMs = MasterCfgConst.DEFAULT_ONLINE_ONLY_READ_TO_RW_PERIOD_MS;
    private long offlineOnlyReadToRWPeriodMs = MasterCfgConst.DEFAULT_OFFLINE_ONLY_READ_TO_RW_PERIOD_MS;
    private long stepChgWaitPeriodMs = MasterCfgConst.DEFAULT_STEP_CHG_WAIT_PERIOD_MS;
    private String confModAuthToken = MasterCfgConst.DEFAULT_CONF_MOD_AUTH_TOKEN;
    private String webResourcePath = MasterCfgConst.DEFAULT_WEB_RESOURCE_PATH;
    private String metaDataPath = MasterCfgConst.DEFAULT_META_DATA_PATH;
    private int maxGroupBrokerConsumeRate = MasterCfgConst.DEFAULT_MAX_GROUP_BROKER_CONSUME_RATE;
    private int maxGroupRebalanceWaitPeriod = MasterCfgConst.DEFAULT_MAX_GROUP_REBALANCE_WAIT_PERIOD;
    private int maxAutoForbiddenCnt = MasterCfgConst.DEFAULT_MAX_AUTO_FORBIDDEN_CNT;
    private long socketSendBuffer = MasterCfgConst.DEFAULT_MASTER_SOCKET_SEND_BUFFER;
    private long socketRecvBuffer = MasterCfgConst.DEFAULT_MASTER_SOCKET_RECV_BUFFER;
    private boolean startOffsetResetCheck = MasterCfgConst.DEFAULT_START_OFFSET_RESET_CHECK;
    private int rowLockWaitDurMs =
        TServerConstants.CFG_ROWLOCK_DEFAULT_DURATION;
    private boolean startVisitTokenCheck = MasterCfgConst.DEFAULT_START_VISIT_TOKEN_CHECK;
    private boolean startProduceAuthenticate = MasterCfgConst.DEFAULT_START_PRODUCE_AUTHENTICATE;
    private boolean startProduceAuthorize = MasterCfgConst.DEFAULT_START_PRODUCE_AUTHORIZE;
    private boolean startConsumeAuthenticate = MasterCfgConst.DEFAULT_START_CONSUME_AUTHENTICATE;
    private boolean startConsumeAuthorize = MasterCfgConst.DEFAULT_START_CONSUME_AUTHORIZE;
    private long visitTokenValidPeriodMs = MasterCfgConst.DEFAULT_VISIT_TOKEN_VALID_PERIOD_MS;
    private boolean needBrokerVisitAuth = MasterCfgConst.DEFAULT_NEED_BROKER_VISIT_AUTH;
    private boolean useWebProxy = MasterCfgConst.DEFAULT_USE_WEB_PROXY;
    private String visitName = MasterCfgConst.DEFAULT_MASTER_VISIT_NAME;
    private String visitPassword = MasterCfgConst.DEFAULT_MASTER_VISIT_PASSWORD;
    private long authValidTimeStampPeriodMs = TBaseConstants.CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL;
    private int rebalanceParallel = MasterCfgConst.DEFAULT_REBALANCE_PARALLEL;

    /**
     * getters
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Is Transport Layer Security enabled ?
     *
     * @return true if enabled
     */
    public boolean isTlsEnable() {
        return this.tlsConfiguration.get(TlsConfItems.TLS_ENABLE);
    }

    public int getPort() {
        return port;
    }

    public int getWebPort() {
        return webPort;
    }

    public long getOfflineOnlyReadToRWPeriodMs() {
        return this.offlineOnlyReadToRWPeriodMs;
    }

    public String getConfModAuthToken() {
        return this.confModAuthToken;
    }

    public long getOnlineOnlyReadToRWPeriodMs() {
        return this.onlineOnlyReadToRWPeriodMs;
    }

    public long getStepChgWaitPeriodMs() {
        return this.stepChgWaitPeriodMs;
    }

    public long getRpcReadTimeoutMs() {
        return this.rpcReadTimeoutMs;
    }

    public long getNettyWriteBufferHighWaterMark() {
        return this.nettyWriteBufferHighWaterMark;
    }

    public long getNettyWriteBufferLowWaterMark() {
        return this.nettyWriteBufferLowWaterMark;
    }

    public int getConsumerBalancePeriodMs() {
        return consumerBalancePeriodMs;
    }

    public int getFirstBalanceDelayAfterStartMs() {
        return firstBalanceDelayAfterStartMs;
    }

    public String getWebResourcePath() {
        return webResourcePath;
    }

    public String getMetaDataPath() {
        return metaDataPath;
    }

    /**
     * Setter
     *
     * @param webResourcePath TODO: Have no usage, could be removed?
     */
    public void setWebResourcePath(String webResourcePath) {
        this.webResourcePath = webResourcePath;
    }

    public int getConsumerHeartbeatTimeoutMs() {
        return consumerHeartbeatTimeoutMs;
    }

    public int getProducerHeartbeatTimeoutMs() {
        return producerHeartbeatTimeoutMs;
    }

    public int getBrokerHeartbeatTimeoutMs() {
        return brokerHeartbeatTimeoutMs;
    }

    public int getMaxGroupBrokerConsumeRate() {
        return maxGroupBrokerConsumeRate;
    }

    public boolean isStartOffsetResetCheck() {
        return startOffsetResetCheck;
    }

    public int getMaxGroupRebalanceWaitPeriod() {
        return maxGroupRebalanceWaitPeriod;
    }

    public int getRowLockWaitDurMs() {
        return rowLockWaitDurMs;
    }

    public int getMaxAutoForbiddenCnt() {
        return maxAutoForbiddenCnt;
    }

    public MasterReplicationConfig getReplicationConfig() {
        return this.replicationConfig;
    }

    /**
     * @deprecated Use {@link #getTlsConfiguration()}
     * @return TLSConfig.
     */
    @Deprecated
    public TLSConfig getTlsConfig() {
        return TLSConfig.fromConfiguration(tlsConfiguration);
    }

    public Configuration getTlsConfiguration() {
        return tlsConfiguration;
    }

    public void setTlsConfiguration(Configuration newConfiguration) {
        ConfigurationUtils.updateTlsConfiguration(this.tlsConfiguration, newConfiguration);
    }

    /**
     * use {@link #setTlsConfiguration(Configuration)} instead of this method.
     * @param tlsConfig
     */
    @Deprecated
    public void setTlsConfiguration(TLSConfig tlsConfig) {
        setTlsConfiguration(TLSConfig.convertToConfiguration(tlsConfig));
    }

    public ZKConfig getZkConfig() {
        return zkConfig;
    }

    public boolean isStartVisitTokenCheck() {
        return startVisitTokenCheck;
    }

    public long getVisitTokenValidPeriodMs() {
        return visitTokenValidPeriodMs;
    }

    public boolean isStartProduceAuthenticate() {
        return startProduceAuthenticate;
    }

    public boolean isStartProduceAuthorize() {
        return startProduceAuthorize;
    }

    public boolean isNeedBrokerVisitAuth() {
        return needBrokerVisitAuth;
    }

    public boolean isStartConsumeAuthenticate() {
        return startConsumeAuthenticate;
    }

    public boolean isStartConsumeAuthorize() {
        return startConsumeAuthorize;
    }

    public boolean isUseWebProxy() {
        return useWebProxy;
    }

    public long getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public long getSocketRecvBuffer() {
        return socketRecvBuffer;
    }

    public String getVisitName() {
        return visitName;
    }

    public String getVisitPassword() {
        return visitPassword;
    }

    public long getAuthValidTimeStampPeriodMs() {
        return authValidTimeStampPeriodMs;
    }

    public int getRebalanceParallel() {
        return rebalanceParallel;
    }

    /**
     * Load file section attributes
     *
     * @param iniConf
     */
    @Override
    protected void loadFileSectAttributes(final Ini iniConf) {
        this.loadSystemConf(iniConf);
        this.loadReplicationSectConf(iniConf);
        this.tlsConfiguration = this.loadTlsSectConfiguration(iniConf, TBaseConstants.META_DEFAULT_MASTER_TLS_PORT);
        this.zkConfig = loadZKeeperSectConf(iniConf);
    }

    /**
     * Load system config
     *
     * @param iniConf
     */
    // #lizard forgives
    private void loadSystemConf(final Ini iniConf) {
        final Profile.Section masterConf = iniConf.get(MasterCfgConst.SECT_TOKEN_MASTER);
        if (masterConf == null) {
            throw new IllegalArgumentException(new StringBuilder(256)
                    .append(MasterCfgConst.SECT_TOKEN_MASTER).append(" configure section is required!").toString());
        }
        Set<String> configKeySet = masterConf.keySet();
        if (configKeySet.isEmpty()) { /* Should have a least one config item */
            throw new IllegalArgumentException(new StringBuilder(256)
                    .append("Empty configure item in ").append(MasterCfgConst.SECT_TOKEN_MASTER)
                    .append(" section!").toString());
        }

        // port
        this.port = this.getInt(masterConf, MasterCfgConst.MASTER_PORT,
                TBaseConstants.META_DEFAULT_MASTER_PORT);

        // hostname
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_HOST_NAME))) {
            this.hostName = masterConf.get(MasterCfgConst.MASTER_HOST_NAME).trim();
        } else {
            try {
                this.hostName = AddressUtils.getIPV4LocalAddress();
            } catch (Throwable e) {
                throw new IllegalArgumentException(new StringBuilder(256)
                    .append("Get default master hostName failure : ")
                    .append(e.getMessage()).toString());
            }
        }
        // web port
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_WEB_PORT))) {
            this.webPort = this.getInt(masterConf, MasterCfgConst.MASTER_WEB_PORT);
        }

        // web resource path
        if (TStringUtils.isBlank(masterConf.get(MasterCfgConst.WEB_RESOURCE_PATH))) {
            throw new IllegalArgumentException(new StringBuilder(256)
                .append("webResourcePath is null or Blank in ").append(MasterCfgConst.SECT_TOKEN_MASTER)
                .append(" section!").toString());
        }
        this.webResourcePath = masterConf.get(MasterCfgConst.WEB_RESOURCE_PATH).trim();

        // meta data path
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.META_DATA_PATH))) {
            this.metaDataPath = masterConf.get(MasterCfgConst.META_DATA_PATH).trim();
        }

        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.CONSUMER_BALANCE_PERIOD_MS))) {
            this.consumerBalancePeriodMs =
                this.getInt(masterConf, MasterCfgConst.CONSUMER_BALANCE_PERIOD_MS);
        }

        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.FIRST_BALANCE_DELAY_AFTER_START_MS))) {
            this.firstBalanceDelayAfterStartMs =
                this.getInt(masterConf, MasterCfgConst.FIRST_BALANCE_DELAY_AFTER_START_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.CONSUMER_HEARTBEAT_TIMEOUT_MS))) {
            this.consumerHeartbeatTimeoutMs =
                this.getInt(masterConf, MasterCfgConst.CONSUMER_HEARTBEAT_TIMEOUT_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.PRODUCER_HEARTBEAT_TIMEOUT_MS))) {
            this.producerHeartbeatTimeoutMs =
                this.getInt(masterConf, MasterCfgConst.PRODUCER_HEARTBEAT_TIMEOUT_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.BROKER_HEARTBEAT_TIMEOUT_MS))) {
            this.brokerHeartbeatTimeoutMs =
                this.getInt(masterConf, MasterCfgConst.BROKER_HEARTBEAT_TIMEOUT_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_SOCKET_SEND_BUFFER))) {
            this.socketSendBuffer = this.getLong(masterConf, MasterCfgConst.MASTER_SOCKET_SEND_BUFFER);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_SOCKET_RECV_BUFFER))) {
            this.socketRecvBuffer = this.getLong(masterConf, MasterCfgConst.MASTER_SOCKET_RECV_BUFFER);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_RPC_READ_TIMEOUT_MS))) {
            this.rpcReadTimeoutMs =
                this.getLong(masterConf, MasterCfgConst.MASTER_RPC_READ_TIMEOUT_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK))) {
            this.nettyWriteBufferHighWaterMark =
                this.getLong(masterConf, MasterCfgConst.MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK))) {
            this.nettyWriteBufferLowWaterMark =
                this.getLong(masterConf, MasterCfgConst.MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.ONLINE_ONLY_READ_TO_RW_PERIOD_MS))) {
            this.onlineOnlyReadToRWPeriodMs =
                this.getLong(masterConf, MasterCfgConst.ONLINE_ONLY_READ_TO_RW_PERIOD_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.STEP_CHG_WAIT_PERIOD_MS))) {
            this.stepChgWaitPeriodMs =
                this.getLong(masterConf, MasterCfgConst.STEP_CHG_WAIT_PERIOD_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.OFFLINE_ONLY_READ_TO_RW_PERIOD_MS))) {
            this.offlineOnlyReadToRWPeriodMs =
                this.getLong(masterConf, MasterCfgConst.OFFLINE_ONLY_READ_TO_RW_PERIOD_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.CONF_MOD_AUTH_TOKEN))) {
            String tmpAuthToken = masterConf.get(MasterCfgConst.CONF_MOD_AUTH_TOKEN).trim();
            if (tmpAuthToken.length() > TServerConstants.CFG_MODAUTHTOKEN_MAX_LENGTH) {
                throw new IllegalArgumentException(
                        "Invalid value: the length of confModAuthToken's value > "
                                + TServerConstants.CFG_MODAUTHTOKEN_MAX_LENGTH);
            }
            this.confModAuthToken = tmpAuthToken;
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MAX_GROUP_BROKER_CONSUME_RATE))) {
            this.maxGroupBrokerConsumeRate =
                this.getInt(masterConf, MasterCfgConst.MAX_GROUP_BROKER_CONSUME_RATE);
            if (this.maxGroupBrokerConsumeRate <= 0) {
                throw new IllegalArgumentException(
                        "Invalid value: maxGroupBrokerConsumeRate's value must > 0 !");
            }
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MAX_GROUP_REBALANCE_WAIT_PERIOD))) {
            this.maxGroupRebalanceWaitPeriod =
                this.getInt(masterConf, MasterCfgConst.MAX_GROUP_REBALANCE_WAIT_PERIOD);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_OFFSET_RESET_CHECK))) {
            this.startOffsetResetCheck =
                this.getBoolean(masterConf, MasterCfgConst.START_OFFSET_RESET_CHECK);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MASTER_ROW_LOCK_WAIT_DUR_MS))) {
            this.rowLockWaitDurMs =
                this.getInt(masterConf, MasterCfgConst.MASTER_ROW_LOCK_WAIT_DUR_MS);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.MAX_AUTO_FORBIDDEN_CNT))) {
            this.maxAutoForbiddenCnt =
                this.getInt(masterConf, MasterCfgConst.MAX_AUTO_FORBIDDEN_CNT);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.VISIT_TOKEN_VALID_PERIOD_MS))) {
            long tmpPeriodMs = this.getLong(masterConf, MasterCfgConst.VISIT_TOKEN_VALID_PERIOD_MS);
            if (tmpPeriodMs < 3 * 60 * 1000) { /* Min value is 3 min */
                tmpPeriodMs = 3 * 60 * 1000;
            }
            this.visitTokenValidPeriodMs = tmpPeriodMs;
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.AUTH_VALID_TIMESTAMP_PERIOD_MS))) {
            long tmpPeriodMs = this.getLong(masterConf, MasterCfgConst.AUTH_VALID_TIMESTAMP_PERIOD_MS);
            // must between 5,000 ms and 120,000 ms
            this.authValidTimeStampPeriodMs =
                    tmpPeriodMs < 5000 ? 5000 : tmpPeriodMs > 120000 ? 120000 : tmpPeriodMs;
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_VISIT_TOKEN_CHECK))) {
            this.startVisitTokenCheck = this.getBoolean(masterConf, MasterCfgConst.START_VISIT_TOKEN_CHECK);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_PRODUCE_AUTHENTICATE))) {
            this.startProduceAuthenticate = this.getBoolean(masterConf, MasterCfgConst.START_PRODUCE_AUTHENTICATE);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_PRODUCE_AUTHORIZE))) {
            this.startProduceAuthorize = this.getBoolean(masterConf, MasterCfgConst.START_PRODUCE_AUTHORIZE);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.USE_WEB_PROXY))) {
            this.useWebProxy = this.getBoolean(masterConf, MasterCfgConst.USE_WEB_PROXY);
        }
        if (!this.startProduceAuthenticate && this.startProduceAuthorize) {
            throw new IllegalArgumentException(
                    "startProduceAuthenticate must set true if startProduceAuthorize is true!");
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_CONSUME_AUTHENTICATE))) {
            this.startConsumeAuthenticate = this.getBoolean(masterConf, MasterCfgConst.START_CONSUME_AUTHENTICATE);
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.START_CONSUME_AUTHORIZE))) {
            this.startConsumeAuthorize = this.getBoolean(masterConf, MasterCfgConst.START_CONSUME_AUTHORIZE);
        }
        if (!this.startConsumeAuthenticate && this.startConsumeAuthorize) {
            throw new IllegalArgumentException(
                    "startConsumeAuthenticate must set true if startConsumeAuthorize is true!");
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.NEED_BROKER_VISIT_AUTH))) {
            this.needBrokerVisitAuth = this.getBoolean(masterConf, MasterCfgConst.NEED_BROKER_VISIT_AUTH);
        }
        if (this.needBrokerVisitAuth) {
            if (TStringUtils.isBlank(masterConf.get(MasterCfgConst.MASTER_VISIT_NAME))) {
                throw new IllegalArgumentException(new StringBuilder(256)
                        .append("visitName is null or Blank in ").append(MasterCfgConst.SECT_TOKEN_BROKER)
                        .append(" section!").toString());
            }
            if (TStringUtils.isBlank(masterConf.get(MasterCfgConst.MASTER_VISIT_PASSWORD))) {
                throw new IllegalArgumentException(new StringBuilder(256)
                        .append("visitPassword is null or Blank in ").append(MasterCfgConst.SECT_TOKEN_BROKER)
                        .append(" section!").toString());
            }
            this.visitName = masterConf.get(MasterCfgConst.MASTER_VISIT_NAME).trim();
            this.visitPassword = masterConf.get(MasterCfgConst.MASTER_VISIT_PASSWORD).trim();
        }
        if (TStringUtils.isNotBlank(masterConf.get(MasterCfgConst.REBALANCE_PARALLEL))) {
            int tmpParallel = this.getInt(masterConf, MasterCfgConst.REBALANCE_PARALLEL);
            this.rebalanceParallel = MixedUtils.mid(tmpParallel, 1, 20);
        }
    }

    /**
     * Deprecated: Load Berkeley DB store section config
     * Just keep `loadBdbStoreSectConf` for backward compatibility
     *
     * @param iniConf
     */
    private boolean loadBdbStoreSectConf(final Ini iniConf) {
        final Profile.Section bdbSect = iniConf.get(MasterCfgConst.SECT_TOKEN_BDB);
        if (bdbSect == null) {
            return false;
        }
        Set<String> configKeySet = bdbSect.keySet();
        if (configKeySet.isEmpty()) {
            throw new IllegalArgumentException(new StringBuilder(256)
                    .append("Empty configure item in ").append(MasterCfgConst.SECT_TOKEN_BDB)
                    .append(" section!").toString());
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_REP_GROUP_NAME))) {
            getSimilarConfigField(MasterCfgConst.SECT_TOKEN_BDB, configKeySet, MasterCfgConst.BDB_REP_GROUP_NAME);
        } else {
            replicationConfig.setRepGroupName(bdbSect.get(MasterCfgConst.BDB_REP_GROUP_NAME).trim());
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_NODE_NAME))) {
            getSimilarConfigField(MasterCfgConst.SECT_TOKEN_BDB, configKeySet, MasterCfgConst.BDB_NODE_NAME);
        } else {
            replicationConfig.setRepNodeName(bdbSect.get(MasterCfgConst.BDB_NODE_NAME).trim());
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_NODE_PORT))) {
            replicationConfig.setRepNodePort(9001);
        } else {
            replicationConfig.setRepNodePort(getInt(bdbSect, MasterCfgConst.BDB_NODE_PORT));
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_ENV_HOME))) {
            getSimilarConfigField(MasterCfgConst.SECT_TOKEN_BDB, configKeySet, MasterCfgConst.BDB_ENV_HOME);
        } else {
            this.metaDataPath = bdbSect.get(MasterCfgConst.BDB_ENV_HOME).trim();
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_HELPER_HOST))) {
            getSimilarConfigField(MasterCfgConst.SECT_TOKEN_BDB, configKeySet, MasterCfgConst.BDB_HELPER_HOST);
        } else {
            replicationConfig.setRepHelperHost(bdbSect.get(MasterCfgConst.BDB_HELPER_HOST).trim());
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_LOCAL_SYNC))) {
            replicationConfig.setMetaLocalSyncPolicy(1);
        } else {
            replicationConfig.setMetaLocalSyncPolicy(getInt(bdbSect, MasterCfgConst.BDB_LOCAL_SYNC));
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_REPLICA_SYNC))) {
            replicationConfig.setMetaReplicaSyncPolicy(3);
        } else {
            replicationConfig.setMetaReplicaSyncPolicy(getInt(bdbSect, MasterCfgConst.BDB_REPLICA_SYNC));
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_REPLICA_ACK))) {
            replicationConfig.setRepReplicaAckPolicy(1);
        } else {
            replicationConfig.setRepReplicaAckPolicy(getInt(bdbSect, MasterCfgConst.BDB_REPLICA_ACK));
        }
        if (TStringUtils.isBlank(bdbSect.get(MasterCfgConst.BDB_STATUS_CHECK_TIMEOUT_MS))) {
            replicationConfig.setRepStatusCheckTimeoutMs(10000);
        } else {
            replicationConfig.setRepStatusCheckTimeoutMs(getLong(bdbSect, MasterCfgConst.BDB_STATUS_CHECK_TIMEOUT_MS));
        }

        return true;
    }

    /**
     * Load Replication section config
     *
     * @param iniConf
     */
    private void loadReplicationSectConf(final Ini iniConf) {
        final Profile.Section repSect = iniConf.get(MasterCfgConst.SECT_TOKEN_REPLICATION);
        if (repSect == null) {
            if (!this.loadBdbStoreSectConf(iniConf)) { // read [bdbStore] for backward compatibility
                throw new IllegalArgumentException(new StringBuilder(256)
                    .append(MasterCfgConst.SECT_TOKEN_REPLICATION).append(" configure section is required!")
                    .toString());
            }
            logger.warn("[bdbStore] section is deprecated. " +
                "Please config in [replication] section.");
            return;
        }
        Set<String> configKeySet = repSect.keySet();
        if (configKeySet.isEmpty()) {
            throw new IllegalArgumentException(new StringBuilder(256)
                    .append("Empty configure item in ").append(MasterCfgConst.SECT_TOKEN_REPLICATION)
                    .append(" section!").toString());
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.REP_GROUP_NAME))) {
            replicationConfig.setRepGroupName(repSect.get(MasterReplicaCfgConst.REP_GROUP_NAME).trim());
        }
        if (TStringUtils.isBlank(repSect.get(MasterReplicaCfgConst.REP_NODE_NAME))) {
            getSimilarConfigField(MasterReplicaCfgConst.SECT_TOKEN_REPLICATION, configKeySet,
                    MasterReplicaCfgConst.REP_NODE_NAME);
        } else {
            replicationConfig.setRepNodeName(repSect.get(MasterReplicaCfgConst.REP_NODE_NAME).trim());
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.REP_NODE_PORT))) {
            replicationConfig.setRepNodePort(getInt(repSect, MasterReplicaCfgConst.REP_NODE_PORT));
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.REP_HELPER_HOST))) {
            replicationConfig.setRepHelperHost(repSect.get(MasterReplicaCfgConst.REP_HELPER_HOST).trim());
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.META_LOCAL_SYNC_POLICY))) {
            replicationConfig.setMetaLocalSyncPolicy(getInt(repSect, MasterReplicaCfgConst.META_LOCAL_SYNC_POLICY));
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.META_REPLICA_SYNC_POLICY))) {
            replicationConfig.setMetaReplicaSyncPolicy(getInt(repSect, MasterReplicaCfgConst.META_REPLICA_SYNC_POLICY));
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.REP_REPLICA_ACK_POLICY))) {
            replicationConfig.setRepReplicaAckPolicy(getInt(repSect, MasterReplicaCfgConst.REP_REPLICA_ACK_POLICY));
        }
        if (TStringUtils.isNotBlank(repSect.get(MasterReplicaCfgConst.REP_STATUS_CHECK_TIMEOUT_MS))) {
            replicationConfig.setRepStatusCheckTimeoutMs(getLong(repSect,
                    MasterReplicaCfgConst.REP_STATUS_CHECK_TIMEOUT_MS));
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(super.toString())
                .append("hostName", hostName)
                .append("port", port)
                .append("webPort", webPort)
                .append("consumerBalancePeriodMs", consumerBalancePeriodMs)
                .append("firstBalanceDelayAfterStartMs", firstBalanceDelayAfterStartMs)
                .append("consumerHeartbeatTimeoutMs", consumerHeartbeatTimeoutMs)
                .append("producerHeartbeatTimeoutMs", producerHeartbeatTimeoutMs)
                .append("brokerHeartbeatTimeoutMs", brokerHeartbeatTimeoutMs)
                .append("rpcReadTimeoutMs", rpcReadTimeoutMs)
                .append("nettyWriteBufferHighWaterMark", nettyWriteBufferHighWaterMark)
                .append("nettyWriteBufferLowWaterMark", nettyWriteBufferLowWaterMark)
                .append("onlineOnlyReadToRWPeriodMs", onlineOnlyReadToRWPeriodMs)
                .append("offlineOnlyReadToRWPeriodMs", offlineOnlyReadToRWPeriodMs)
                .append("stepChgWaitPeriodMs", stepChgWaitPeriodMs)
                .append("confModAuthToken", confModAuthToken)
                .append("webResourcePath", webResourcePath)
                .append("maxGroupBrokerConsumeRate", maxGroupBrokerConsumeRate)
                .append("maxGroupRebalanceWaitPeriod", maxGroupRebalanceWaitPeriod)
                .append("maxAutoForbiddenCnt", maxAutoForbiddenCnt)
                .append("startOffsetResetCheck", startOffsetResetCheck)
                .append("rowLockWaitDurMs", rowLockWaitDurMs)
                .append("needBrokerVisitAuth", needBrokerVisitAuth)
                .append("useWebProxy", useWebProxy)
                .append("visitName", visitName)
                .append("visitPassword", visitPassword)
                .append("rebalanceParallel", rebalanceParallel)
                .append(",").append(replicationConfig.toString())
                .append(",")
                .append("TLSConfig", new GsonBuilder().disableHtmlEscaping().create().toJson(tlsConfiguration))
                .append(",").append(zkConfig.toString())
                .append("}").toString();
    }
}

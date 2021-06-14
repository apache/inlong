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

package org.apache.tubemq.server.broker;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TServerConstants;
import org.apache.tubemq.corebase.config.Configuration;
import org.apache.tubemq.corebase.config.ConfigurationUtils;
import org.apache.tubemq.corebase.config.TLSConfig;
import org.apache.tubemq.corebase.config.TlsConfItems;
import org.apache.tubemq.corebase.config.constants.BrokerCfgConst;
import org.apache.tubemq.corebase.utils.AddressUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.tubemq.server.common.fileconfig.AbstractFileConfig;
import org.apache.tubemq.server.common.fileconfig.ZKConfig;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.abs;

/***
 * Config of broker. Read from broker.ini config file.
 */
public class BrokerConfig extends AbstractFileConfig {
    static final long serialVersionUID = -1L;
    private static final Logger logger = LoggerFactory.getLogger(BrokerConfig.class);
    // broker id
    private int brokerId = BrokerCfgConst.DEFAULT_BROKER_ID;
    // default NetworkInterface
    private String defEthName = BrokerCfgConst.DEFAULT_DEF_ETH_NAME;
    // broker hostname
    private String hostName;
    // broker port
    private int port = TBaseConstants.META_DEFAULT_BROKER_PORT;
    // broker web service port
    private int webPort = BrokerCfgConst.DEFAULT_BROKER_WEB_PORT;
    // master service address
    private String masterAddressList;
    private String primaryPath;
    // tcp write service thread count
    private int tcpWriteServiceThread = BrokerCfgConst.DEFAULT_TCP_WRITE_SERVICE_THREAD;
    // tcp read service thread count
    private int tcpReadServiceThread = BrokerCfgConst.DEFAULT_TCP_READ_SERVICE_THREAD;
    // tls write service thread count
    private int tlsWriteServiceThread = BrokerCfgConst.DEFAULT_TLS_WRITE_SERVICE_THREAD;
    // tls read service thread count
    private int tlsReadServiceThread = BrokerCfgConst.DEFAULT_TLS_READ_SERVICE_THREAD;
    private long defaultDeduceReadSize = BrokerCfgConst.DEFAULT_DEFAULT_DEDUCE_READ_SIZE;
    private long defaultDoubleDeduceReadSize = this.defaultDeduceReadSize * 2;
    // max data segment size
    private int maxSegmentSize = BrokerCfgConst.DEFAULT_MAX_SEGMENT_SIZE;
    // max index segment size
    private int maxIndexSegmentSize = 700000 * DataStoreUtils.STORE_INDEX_HEAD_LEN;
    // transfer size
    private int transferSize = BrokerCfgConst.DEFAULT_TRANSFER_SIZE;
    // transfer index count
    private int indexTransCount = BrokerCfgConst.DEFAULT_INDEX_TRANS_COUNT;
    // rpc read timeout in milliseconds
    private long rpcReadTimeoutMs = BrokerCfgConst.DEFAULT_BROKER_RPC_READ_TIMEOUT_MS;
    // consumer register timeout in milliseconds
    private int consumerRegTimeoutMs = BrokerCfgConst.DEFAULT_CONSUMER_REG_TIMEOUT_MS;
    private boolean updateConsumerOffsets = BrokerCfgConst.DEFAULT_UPDATE_CONSUMER_OFFSETS;
    // heartbeat interval in milliseconds
    private long heartbeatPeriodMs = BrokerCfgConst.DEFAULT_HEARTBEAT_PERIOD_MS;
    // netty write buffer high water mark
    private long nettyWriteBufferHighWaterMark = BrokerCfgConst.DEFAULT_BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK;
    // netty write buffer low water mark
    private long nettyWriteBufferLowWaterMark = BrokerCfgConst.DEFAULT_BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK;
    // log cleanup interval in milliseconds
    private long logClearupDurationMs = BrokerCfgConst.DEFAULT_LOG_CLEAR_UP_DURATION_MS;
    // log flush to disk interval in milliseconds
    private long logFlushDiskDurMs = BrokerCfgConst.DEFAULT_LOG_FLUSH_DISK_DUR_MS;
    // memory flush to disk interval in milliseconds
    private long logFlushMemDurMs = BrokerCfgConst.DEFAULT_LOG_FLUSH_MEM_DUR_MS;
    // socket send buffer
    private long socketSendBuffer = BrokerCfgConst.DEFAULT_BROKER_SOCKET_SEND_BUFFER;
    // socket receive buffer
    private long socketRecvBuffer = BrokerCfgConst.DEFAULT_BROKER_SOCKET_RECV_BUFFER;
    // read io exception max count
    private int allowedReadIOExcptCnt = BrokerCfgConst.DEFAULT_ALLOWED_READ_IO_EXCEPTION_CNT;
    // write io exception max count
    private int allowedWriteIOExcptCnt = BrokerCfgConst.DEFAULT_ALLOWED_WRITE_IO_EXCEPTION_CNT;
    //
    private long visitTokenCheckInValidTimeMs = BrokerCfgConst.DEFAULT_VISIT_TOKEN_CHECK_IN_VALID_TIME_MS;
    private long ioExcptStatsDurationMs = BrokerCfgConst.DEFAULT_IO_EXCEPTION_STATS_DURATION_MS;
    // row lock wait duration
    private int rowLockWaitDurMs =
        TServerConstants.CFG_ROWLOCK_DEFAULT_DURATION;
    // zookeeper config
    private ZKConfig zkConfig = new ZKConfig();
    // tls config
    private Configuration tlsConfiguration = new Configuration();
    private boolean visitMasterAuth = BrokerCfgConst.DEFAULT_VISIT_MASTER_AUTH;
    private String visitName = BrokerCfgConst.DEFAULT_BROKER_VISIT_NAME;
    private String visitPassword = BrokerCfgConst.DEFAULT_BROKER_VISIT_PASSWORD;
    private long authValidTimeStampPeriodMs = TBaseConstants.CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL;

    public BrokerConfig() {
        super();
    }

    public boolean isUpdateConsumerOffsets() {
        return this.updateConsumerOffsets;
    }

    public int getConsumerRegTimeoutMs() {
        return consumerRegTimeoutMs;
    }

    public long getHeartbeatPeriodMs() {
        return heartbeatPeriodMs;
    }

    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }

    /**
     * @return TlsConfig.
     * @deprecated Use {@link #getTlsConfiguration()}
     */
    @Deprecated
    public TLSConfig getTlsConfig() {
        return TLSConfig.fromConfiguration(this.tlsConfiguration);
    }

    public Configuration getTlsConfiguration() {
        return this.tlsConfiguration;
    }

    public void setTlsConfiguration(Configuration newConfiguration) {
        ConfigurationUtils.updateTlsConfiguration(this.tlsConfiguration, newConfiguration);
    }

    /**
     * use {@link #setTlsConfiguration(Configuration)}
     *
     * @param tlsConfig
     */
    @Deprecated
    public void setTlsConfiguration(TLSConfig tlsConfig) {
        setTlsConfiguration(TLSConfig.convertToConfiguration(tlsConfig));
    }

    public int getBrokerId() {
        if (this.brokerId <= 0) {
            try {
                brokerId = abs(AddressUtils.ipToInt(AddressUtils.getIPV4LocalAddress(this.defEthName)));
            } catch (Exception e) {
                logger.error("Get brokerId error!", e);
            }
        }
        return brokerId;
    }

    public String getHostName() {
        return this.hostName;
    }

    public long getRpcReadTimeoutMs() {
        return this.rpcReadTimeoutMs;
    }

    public int getMaxIndexSegmentSize() {
        return maxIndexSegmentSize;
    }

    public long getAuthValidTimeStampPeriodMs() {
        return authValidTimeStampPeriodMs;
    }

    public long getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public long getSocketRecvBuffer() {
        return socketRecvBuffer;
    }


    @Override
    protected void loadFileSectAttributes(final Ini iniConf) {
        this.loadBrokerSectConf(iniConf);
        this.tlsConfiguration = this.loadTlsSectConfiguration(iniConf,
                TBaseConstants.META_DEFAULT_BROKER_TLS_PORT);
        this.zkConfig = loadZKeeperSectConf(iniConf);
    }

    /***
     * Load config from broker.ini by section.
     *
     * @param iniConf
     */
    private void loadBrokerSectConf(final Ini iniConf) {
        // #lizard forgives
        final Section brokerSect = iniConf.get(BrokerCfgConst.SECT_TOKEN_BROKER);
        if (brokerSect == null) {
            throw new IllegalArgumentException("Require broker section in configure file not Blank!");
        }
        this.brokerId = this.getInt(brokerSect, BrokerCfgConst.BROKER_ID);
        this.port = this.getInt(brokerSect, BrokerCfgConst.BROKER_PORT, BrokerCfgConst.DEFAULT_BROKER_PORT);
        if (TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.PRIMARY_PATH))) {
            throw new IllegalArgumentException("Require primaryPath not Blank!");
        }
        this.primaryPath = brokerSect.get(BrokerCfgConst.PRIMARY_PATH).trim();
        if (TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.BROKER_HOST_NAME))) {
            throw new IllegalArgumentException(new StringBuilder(256).append("hostName is null or Blank in ")
                .append(BrokerCfgConst.SECT_TOKEN_BROKER).append(" section!").toString());
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.DEF_ETH_NAME))) {
            this.defEthName = brokerSect.get(BrokerCfgConst.DEF_ETH_NAME).trim();
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_HOST_NAME))) {
            this.hostName = brokerSect.get(BrokerCfgConst.BROKER_HOST_NAME).trim();
        } else {
            try {
                this.hostName = AddressUtils.getIPV4LocalAddress(this.defEthName);
            } catch (Throwable e) {
                throw new IllegalArgumentException(new StringBuilder(256)
                    .append("Get default broker hostName failure : ")
                    .append(e.getMessage()).toString());
            }
        }
        if (TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.MASTER_ADDRESS_LIST))) {
            throw new IllegalArgumentException(new StringBuilder(256).append("masterAddressList is null or Blank in ")
                .append(BrokerCfgConst.SECT_TOKEN_BROKER).append(" section!").toString());
        }
        this.masterAddressList = brokerSect.get(BrokerCfgConst.MASTER_ADDRESS_LIST);
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_WEB_PORT))) {
            this.webPort = this.getInt(brokerSect, BrokerCfgConst.BROKER_WEB_PORT);
        }
        this.maxSegmentSize = this.getInt(brokerSect, BrokerCfgConst.MAX_SEGMENT_SIZE);
        this.transferSize = this.getInt(brokerSect, BrokerCfgConst.TRANSFER_SIZE);
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.INDEX_TRANS_COUNT))) {
            this.indexTransCount = this.getInt(brokerSect, BrokerCfgConst.INDEX_TRANS_COUNT);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.LOG_CLEAR_UP_DURATION_MS))) {
            this.logClearupDurationMs = getLong(brokerSect, BrokerCfgConst.LOG_CLEAR_UP_DURATION_MS);
            if (this.logClearupDurationMs < 1 * 60 * 1000) {
                this.logClearupDurationMs = 1 * 60 * 1000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.LOG_FLUSH_DISK_DUR_MS))) {
            this.logFlushDiskDurMs = getLong(brokerSect, BrokerCfgConst.LOG_FLUSH_DISK_DUR_MS);
            if (this.logFlushDiskDurMs < 10000) {
                this.logFlushDiskDurMs = 10000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.LOG_FLUSH_MEM_DUR_MS))) {
            this.logFlushMemDurMs = getLong(brokerSect, BrokerCfgConst.LOG_FLUSH_MEM_DUR_MS);
            if (this.logFlushMemDurMs < 10000) {
                this.logFlushMemDurMs = 10000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.AUTH_VALID_TIMESTAMP_PERIOD_MS))) {
            long tmpPeriodMs = this.getLong(brokerSect, BrokerCfgConst.AUTH_VALID_TIMESTAMP_PERIOD_MS);
            this.authValidTimeStampPeriodMs =
                tmpPeriodMs < 5000 ? 5000 : tmpPeriodMs > 120000 ? 120000 : tmpPeriodMs;
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.VISIT_TOKEN_CHECK_IN_VALID_TIME_MS))) {
            long tmpPeriodMs = this.getLong(brokerSect, BrokerCfgConst.VISIT_TOKEN_CHECK_IN_VALID_TIME_MS);
            this.visitTokenCheckInValidTimeMs =
                tmpPeriodMs < 60000 ? 60000 : tmpPeriodMs > 300000 ? 300000 : tmpPeriodMs;
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_SOCKET_SEND_BUFFER))) {
            this.socketSendBuffer = getLong(brokerSect, BrokerCfgConst.BROKER_SOCKET_SEND_BUFFER);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_SOCKET_RECV_BUFFER))) {
            this.socketRecvBuffer = getLong(brokerSect, BrokerCfgConst.BROKER_SOCKET_RECV_BUFFER);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.MAX_INDEX_SEGMENT_SIZE))) {
            this.maxIndexSegmentSize = getInt(brokerSect, BrokerCfgConst.MAX_INDEX_SEGMENT_SIZE);
        }
        if (!TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.UPDATE_CONSUMER_OFFSETS))) {
            this.updateConsumerOffsets = getBoolean(brokerSect, BrokerCfgConst.UPDATE_CONSUMER_OFFSETS);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.RPC_READ_TIMEOUT_MS))) {
            this.rpcReadTimeoutMs = getLong(brokerSect, BrokerCfgConst.RPC_READ_TIMEOUT_MS);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK))) {
            this.nettyWriteBufferHighWaterMark =
                getLong(brokerSect, BrokerCfgConst.BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK);
        }

        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK))) {
            this.nettyWriteBufferLowWaterMark =
                getLong(brokerSect, BrokerCfgConst.BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.HEARTBEAT_PERIOD_MS))) {
            this.heartbeatPeriodMs = getLong(brokerSect, BrokerCfgConst.HEARTBEAT_PERIOD_MS);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.TCP_WRITE_SERVICE_THREAD))) {
            this.tcpWriteServiceThread = getInt(brokerSect, BrokerCfgConst.TCP_WRITE_SERVICE_THREAD);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.TCP_READ_SERVICE_THREAD))) {
            this.tcpReadServiceThread = getInt(brokerSect, BrokerCfgConst.TCP_READ_SERVICE_THREAD);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.TLS_WRITE_SERVICE_THREAD))) {
            this.tlsWriteServiceThread = getInt(brokerSect, BrokerCfgConst.TLS_WRITE_SERVICE_THREAD);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.TLS_READ_SERVICE_THREAD))) {
            this.tlsReadServiceThread = getInt(brokerSect, BrokerCfgConst.TLS_READ_SERVICE_THREAD);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.CONSUMER_REG_TIMEOUT_MS))) {
            this.consumerRegTimeoutMs = getInt(brokerSect, BrokerCfgConst.CONSUMER_REG_TIMEOUT_MS);
            if (this.consumerRegTimeoutMs < 20000) {
                this.consumerRegTimeoutMs = 20000;
            }
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.DEFAULT_DEDUCE_READ_SIZE))) {
            this.defaultDeduceReadSize = getLong(brokerSect, BrokerCfgConst.DEFAULT_DEDUCE_READ_SIZE);
            this.defaultDoubleDeduceReadSize = this.defaultDeduceReadSize * 2;
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.BROKER_ROW_LOCK_WAIT_DUR_MS))) {
            this.rowLockWaitDurMs = getInt(brokerSect, BrokerCfgConst.BROKER_ROW_LOCK_WAIT_DUR_MS);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.ALLOWED_READ_IO_EXCEPTION_CNT))) {
            this.allowedReadIOExcptCnt = getInt(brokerSect, BrokerCfgConst.ALLOWED_READ_IO_EXCEPTION_CNT);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.ALLOWED_WRITE_IO_EXCEPTION_CNT))) {
            this.allowedWriteIOExcptCnt = getInt(brokerSect, BrokerCfgConst.ALLOWED_WRITE_IO_EXCEPTION_CNT);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.IO_EXCEPTION_STATS_DURATION_MS))) {
            this.ioExcptStatsDurationMs = getLong(brokerSect, BrokerCfgConst.IO_EXCEPTION_STATS_DURATION_MS);
        }
        if (TStringUtils.isNotBlank(brokerSect.get(BrokerCfgConst.VISIT_MASTER_AUTH))) {
            this.visitMasterAuth = this.getBoolean(brokerSect, BrokerCfgConst.VISIT_MASTER_AUTH);
        }
        if (this.visitMasterAuth) {
            if (TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.BROKER_VISIT_NAME))) {
                throw new IllegalArgumentException(new StringBuilder(256).append("visitName is null or Blank in ")
                    .append(BrokerCfgConst.SECT_TOKEN_BROKER).append(" section!").toString());
            }
            if (TStringUtils.isBlank(brokerSect.get(BrokerCfgConst.BROKER_VISIT_PASSWORD))) {
                throw new IllegalArgumentException(new StringBuilder(256)
                    .append("visitPassword is null or Blank in ").append(BrokerCfgConst.SECT_TOKEN_BROKER)
                    .append(" section!").toString());
            }
            this.visitName = brokerSect.get(BrokerCfgConst.BROKER_VISIT_NAME).trim();
            this.visitPassword = brokerSect.get(BrokerCfgConst.BROKER_VISIT_PASSWORD).trim();
        }
    }

    public long getLogClearupDurationMs() {
        return logClearupDurationMs;
    }

    public long getLogFlushDiskDurMs() {
        return logFlushDiskDurMs;
    }

    public long getLogFlushMemDurMs() {
        return logFlushMemDurMs;
    }

    public boolean isTlsEnable() {
        return this.tlsConfiguration.get(TlsConfItems.TLS_ENABLE);
    }

    public int getTlsPort() {
        return this.tlsConfiguration.get(TlsConfItems.TLS_PORT);
    }

    public long getIoExcptStatsDurationMs() {
        return ioExcptStatsDurationMs;
    }

    public int getAllowedWriteIOExcptCnt() {
        return allowedWriteIOExcptCnt;
    }

    public int getAllowedReadIOExcptCnt() {
        return allowedReadIOExcptCnt;
    }

    public int getRowLockWaitDurMs() {
        return rowLockWaitDurMs;
    }

    public int getPort() {
        return this.port;
    }

    public long getVisitTokenCheckInValidTimeMs() {
        return visitTokenCheckInValidTimeMs;
    }

    public int getTcpWriteServiceThread() {
        return this.tcpWriteServiceThread;
    }

    public int getTlsWriteServiceThread() {
        return tlsWriteServiceThread;
    }

    public int getTlsReadServiceThread() {
        return tlsReadServiceThread;
    }

    public long getDefaultDeduceReadSize() {
        return defaultDeduceReadSize;
    }

    public long getDoubleDefaultDeduceReadSize() {
        return defaultDoubleDeduceReadSize;
    }

    public int getTcpReadServiceThread() {
        return tcpReadServiceThread;
    }

    public int getTransferSize() {
        return transferSize;
    }

    public int getIndexTransCount() {
        return this.indexTransCount;
    }

    public int getMaxSegmentSize() {
        return this.maxSegmentSize;
    }

    public long getNettyWriteBufferLowWaterMark() {
        return nettyWriteBufferLowWaterMark;
    }

    public long getNettyWriteBufferHighWaterMark() {
        return nettyWriteBufferHighWaterMark;
    }

    public boolean isVisitMasterAuth() {
        return visitMasterAuth;
    }

    public String getVisitName() {
        return visitName;
    }

    public String getVisitPassword() {
        return visitPassword;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public String getPrimaryPath() {
        return this.primaryPath;
    }

    public int getWebPort() {
        return webPort;
    }


    public String getMasterAddressList() {
        return masterAddressList;
    }

}

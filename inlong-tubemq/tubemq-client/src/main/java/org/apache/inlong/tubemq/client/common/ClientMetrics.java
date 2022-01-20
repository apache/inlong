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

package org.apache.inlong.tubemq.client.common;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.corebase.TErrCodeConstants;
import org.apache.inlong.tubemq.corebase.metric.AbsMetricItem;
import org.apache.inlong.tubemq.corebase.metric.CountMetricItem;
import org.apache.inlong.tubemq.corebase.metric.TimeDltMetricItem;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientMetrics {
    private static final Logger logger =
            LoggerFactory.getLogger(ClientMetrics.class);
    private final boolean isProducer;
    private final String clientId;
    private final String logPrefix;
    private final TubeClientConfig clientConfig;
    private final AtomicLong lastMetricPrintTime =
            new AtomicLong(0);
    private final AtomicLong lastMetricResetTime =
            new AtomicLong(Clock.systemDefaultZone().millis());
    private final AtomicLong lastResetTime =
            new AtomicLong(Clock.systemDefaultZone().millis());
    // client 2 Master status statistics
    protected final AbsMetricItem regMasterCnt =
            new CountMetricItem("master_reg_cnt");
    protected final AbsMetricItem regMasterFailureCnt =
            new CountMetricItem("master_reg_failure_cnt");
    protected final AbsMetricItem masterNoNodeCnt =
            new CountMetricItem("master_timeout_cnt");
    protected final AbsMetricItem masterHBExceptionCnt =
            new CountMetricItem("hb_master_exception_cnt");
    // client 2 Broker status statistics
    protected final AbsMetricItem regBrokerCnt =
            new CountMetricItem("broker_reg_cnt");
    protected final AbsMetricItem regBrokerFailureCnt =
            new CountMetricItem("broker_reg_failure_cnt");
    protected final AbsMetricItem brokerNoNodeCnt =
            new CountMetricItem("broker_timeout_cnt");
    protected final AbsMetricItem brokerHBExceptionCnt =
            new CountMetricItem("hb_broker_exception_cnt");
    // send or get message statistic
    private final TimeDltMetricItem rpcCallMetric =
            new TimeDltMetricItem("rpc_call", true);
    // statistic count
    private final AbsMetricItem totalMsgCount =
            new CountMetricItem("msg_cnt");
    protected final AbsMetricItem totalMsgSize =
            new CountMetricItem("msg_size");
    // error code map
    private final AbsMetricItem errCodeBadRequest =
            new CountMetricItem("err_400");
    private final AbsMetricItem errCodeNotFound =
            new CountMetricItem("err_404");
    private final AbsMetricItem errCodeOccupied =
            new CountMetricItem("err_410");
    private final AbsMetricItem errCodeNoNode =
            new CountMetricItem("err_411");
    private final AbsMetricItem errCodeOverFlow =
            new CountMetricItem("err_419");
    private final AbsMetricItem errCodeInTopic =
            new CountMetricItem("err_425");
    private final AbsMetricItem errCodeInFilters =
            new CountMetricItem("err_426");
    private final AbsMetricItem errCodeInSourceCnt =
            new CountMetricItem("err_429");
    private final AbsMetricItem errCodeSrvUnavailable =
            new CountMetricItem("err_503");
    private final AbsMetricItem errCodeOther =
            new CountMetricItem("err_other");
    // consumer statistic
    private final TimeDltMetricItem procLatencyMetric =
            new TimeDltMetricItem("process_data", true);

    public ClientMetrics(boolean isProducer, String clientId,
                         TubeClientConfig clientConfig) {
        this.isProducer = isProducer;
        this.clientId = clientId;
        this.clientConfig = clientConfig;
        StringBuilder strBuff = new StringBuilder(512);
        if (isProducer) {
            strBuff.append("[Producer");
        } else {
            strBuff.append("[Consumer");
        }
        this.logPrefix = strBuff.append(" Metrics]: ")
                .append("Client=").append(clientId).toString();
    }

    public void bookReg2Master(boolean isFailure) {
        this.regMasterCnt.incrementAndGet();
        if (isFailure) {
            this.regMasterFailureCnt.incrementAndGet();
        }
    }

    public void bookHB2MasterTimeout() {
        this.masterNoNodeCnt.incrementAndGet();
    }

    public void bookHB2MasterException() {
        this.masterHBExceptionCnt.incrementAndGet();
    }

    public void bookReg2Broker(boolean isFailure) {
        this.regBrokerCnt.incrementAndGet();
        if (isFailure) {
            this.regBrokerFailureCnt.incrementAndGet();
        }
    }

    public void bookHB2BrokerTimeout() {
        this.brokerNoNodeCnt.incrementAndGet();
    }

    public void bookHB2BrokerException() {
        this.brokerHBExceptionCnt.incrementAndGet();
    }

    public void bookConfirmDuration(long dltTime) {
        this.procLatencyMetric.updProcTimeDlt(dltTime);
    }

    public void bookSuccSendMsg(long dltTime, int msgSize) {
        rpcCallMetric.updProcTimeDlt(dltTime);
        totalMsgCount.incrementAndGet();
        totalMsgSize.addAndGet(msgSize);
    }

    public void bookSuccGetMsg(long dltTime, int msgCnt, int msgSize) {
        rpcCallMetric.updProcTimeDlt(dltTime);
        totalMsgCount.addAndGet(msgCnt);
        totalMsgSize.addAndGet(msgSize);
    }

    public void bookFailRpcCall(long dltTime, int errCode) {
        rpcCallMetric.updProcTimeDlt(dltTime);
        switch (errCode) {
            case TErrCodeConstants.BAD_REQUEST: {
                errCodeBadRequest.incrementAndGet();
                break;
            }

            case TErrCodeConstants.NOT_FOUND: {
                errCodeNotFound.incrementAndGet();
                break;
            }

            case TErrCodeConstants.PARTITION_OCCUPIED: {
                errCodeOccupied.incrementAndGet();
                break;
            }

            case TErrCodeConstants.HB_NO_NODE: {
                errCodeNoNode.incrementAndGet();
                break;
            }

            case TErrCodeConstants.SERVER_RECEIVE_OVERFLOW: {
                errCodeOverFlow.incrementAndGet();
                break;
            }

            case TErrCodeConstants.CLIENT_INCONSISTENT_TOPICSET: {
                errCodeInTopic.incrementAndGet();
                break;
            }

            case TErrCodeConstants.CLIENT_INCONSISTENT_FILTERSET: {
                errCodeInFilters.incrementAndGet();
                break;
            }

            case TErrCodeConstants.CLIENT_INCONSISTENT_SOURCECOUNT: {
                errCodeInSourceCnt.incrementAndGet();
                break;
            }

            case TErrCodeConstants.SERVICE_UNAVAILABLE: {
                errCodeSrvUnavailable.incrementAndGet();
                break;
            }

            default: {
                errCodeOther.incrementAndGet();
                break;
            }
        }
    }

    /**
     * print Metric information to log file
     *
     * @param forcePrint    whether force print metric information
     * @param strBuff       string buffer
     */
    public void printMetricInfo(boolean forcePrint, StringBuilder strBuff) {
        if (!clientConfig.isEnableMetricPrint()) {
            return;
        }
        boolean needReset = false;
        long lstPrintTime = lastMetricPrintTime.get();
        long curChkTime = Clock.systemDefaultZone().millis();
        if (forcePrint || (curChkTime - lstPrintTime
                > clientConfig.getMetricInfoPrintPeriodMs())) {
            if (lastMetricPrintTime.compareAndSet(lstPrintTime, curChkTime)) {
                long lstResetTime = lastMetricResetTime.get();
                if ((curChkTime - lstResetTime
                        > clientConfig.getMetricForcedResetPeriodMs())
                        && lastMetricResetTime.compareAndSet(lstResetTime, curChkTime)) {
                    needReset = true;
                }
                if (needReset) {
                    strBuff.append(this.logPrefix).append(", reset value=");
                } else {
                    strBuff.append(this.logPrefix).append(", value=");
                }
                getStrMetrics(strBuff, needReset);
                logger.info(strBuff.toString());
                strBuff.delete(0, strBuff.length());
            }
        }
    }

    /**
     * Get current data encapsulated by Json
     *
     * @param strBuff      string buffer
     * @param resetValue   whether to reset the current data
     */
    public void getStrMetrics(StringBuilder strBuff, boolean resetValue) {
        strBuff.append("{\"").append(regMasterCnt.getName()).append("\":")
                .append(regMasterCnt.getValue(resetValue)).append(",\"")
                .append(regMasterFailureCnt.getName()).append("\":")
                .append(regMasterFailureCnt.getValue(resetValue)).append(",\"")
                .append(masterNoNodeCnt.getName()).append("\":")
                .append(masterNoNodeCnt.getValue(resetValue)).append(",\"")
                .append(masterHBExceptionCnt.getName()).append("\":")
                .append(masterHBExceptionCnt.getValue(resetValue)).append(",\"")
                .append(totalMsgCount.getName()).append("\":")
                .append(totalMsgCount.getValue(resetValue)).append(",\"")
                .append(totalMsgSize.getName()).append("\":")
                .append(totalMsgSize.getValue(resetValue)).append(",");
        rpcCallMetric.getStrMetrics(strBuff, resetValue);
        strBuff.append(",\"err_rsp\":{\"")
                .append(errCodeBadRequest.getName()).append("\":")
                .append(errCodeBadRequest.getValue(resetValue)).append(",\"")
                .append(errCodeNotFound.getName()).append("\":")
                .append(errCodeNotFound.getValue(resetValue)).append(",\"")
                .append(errCodeOccupied.getName()).append("\":")
                .append(errCodeOccupied.getValue(resetValue)).append(",\"")
                .append(errCodeNoNode.getName()).append("\":")
                .append(errCodeNoNode.getValue(resetValue)).append(",\"")
                .append(errCodeOverFlow.getName()).append("\":")
                .append(errCodeOverFlow.getValue(resetValue)).append(",\"")
                .append(errCodeInTopic.getName()).append("\":")
                .append(errCodeInTopic.getValue(resetValue)).append(",\"")
                .append(errCodeInFilters.getName()).append("\":")
                .append(errCodeInFilters.getValue(resetValue)).append(",\"")
                .append(errCodeInSourceCnt.getName()).append("\":")
                .append(errCodeInSourceCnt.getValue(resetValue)).append(",\"")
                .append(errCodeSrvUnavailable.getName()).append("\":")
                .append(errCodeSrvUnavailable.getValue(resetValue)).append(",\"")
                .append(errCodeOther.getName()).append("\":")
                .append(errCodeOther.getValue(resetValue)).append("}");
        if (!isProducer) {
            strBuff.append(",\"").append(regBrokerCnt.getName()).append("\":")
                    .append(regBrokerCnt.getValue(resetValue)).append(",\"")
                    .append(regBrokerFailureCnt.getName()).append("\":")
                    .append(regBrokerFailureCnt.getValue(resetValue)).append(",\"")
                    .append(brokerNoNodeCnt.getName()).append("\":")
                    .append(brokerNoNodeCnt.getValue(resetValue)).append(",\"")
                    .append(brokerHBExceptionCnt.getName()).append("\":")
                    .append(brokerHBExceptionCnt.getValue(resetValue)).append(",");
            procLatencyMetric.getStrMetrics(strBuff, resetValue);
        }
        if (resetValue) {
            long befTime = lastResetTime.getAndSet(Clock.systemDefaultZone().millis());
            strBuff.append(",\"last_reset_time\":\"")
                    .append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(befTime)).append("\"}");
        } else {
            strBuff.append(",\"last_reset_time\":\"")
                    .append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(lastResetTime.get()))
                    .append("\"}");
        }
    }
}


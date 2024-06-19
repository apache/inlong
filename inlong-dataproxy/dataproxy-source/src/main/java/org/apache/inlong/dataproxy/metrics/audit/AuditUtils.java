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

package org.apache.inlong.dataproxy.metrics.audit;

import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.audit.entity.AuditComponent;
import org.apache.inlong.audit.util.AuditConfig;
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.apache.inlong.dataproxy.utils.Constants;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Event;

import java.util.Map;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;

/**
 * Audit utils
 */
public class AuditUtils {

    private static int auditIdReadSuccess = 5;
    private static int auditIdSendSuccess = 6;

    /**
     * Init audit
     */
    public static void initAudit() {
        if (CommonConfigHolder.getInstance().isEnableAudit()) {
            // AuditProxy
            if (CommonConfigHolder.getInstance().isEnableAuditProxysDiscoveryFromManager()) {
                AuditOperator.getInstance().setAuditProxy(AuditComponent.DATAPROXY,
                        CommonConfigHolder.getInstance().getManagerHosts().get(0),
                        CommonConfigHolder.getInstance().getManagerAuthSecretId(),
                        CommonConfigHolder.getInstance().getManagerAuthSecretKey());
            } else {
                AuditOperator.getInstance().setAuditProxy(
                        CommonConfigHolder.getInstance().getAuditProxys());
            }
            // AuditConfig
            AuditConfig auditConfig = new AuditConfig(
                    CommonConfigHolder.getInstance().getAuditFilePath(),
                    CommonConfigHolder.getInstance().getAuditMaxCacheRows());
            AuditOperator.getInstance().setAuditConfig(auditConfig);
            auditIdReadSuccess =
                    AuditOperator.getInstance().buildSuccessfulAuditId(AuditIdEnum.DATA_PROXY_INPUT);
            auditIdSendSuccess =
                    AuditOperator.getInstance().buildSuccessfulAuditId(AuditIdEnum.DATA_PROXY_OUTPUT);
        }
    }

    /**
     * Add input audit data
     *
     * @param event    event to be counted
     */
    public static void addInputSuccess(Event event) {
        if (event == null || !CommonConfigHolder.getInstance().isEnableAudit()) {
            return;
        }
        addAuditData(event, auditIdReadSuccess);
    }

    /**
     * Add output audit data
     *
     * @param event    event to be counted
     */
    public static void addOutputSuccess(Event event) {
        if (event == null || !CommonConfigHolder.getInstance().isEnableAudit()) {
            return;
        }
        addAuditData(event, auditIdSendSuccess);
    }

    private static void addAuditData(Event event, int auditID) {
        Map<String, String> headers = event.getHeaders();
        String pkgVersion = headers.get(ConfigConstants.MSG_ENCODE_VER);
        if (MessageWrapType.INLONG_MSG_V1.getStrId().equalsIgnoreCase(pkgVersion)) {
            String inlongGroupId = DataProxyMetricItem.getInlongGroupId(headers);
            String inlongStreamId = DataProxyMetricItem.getInlongStreamId(headers);
            long logTime = getLogTime(headers);
            long msgCount = 1L;
            if (event.getHeaders().containsKey(ConfigConstants.MSG_COUNTER_KEY)) {
                msgCount = Long.parseLong(event.getHeaders().get(ConfigConstants.MSG_COUNTER_KEY));
            }
            long auditVersion = getAuditVersion(headers);
            AuditOperator.getInstance().add(auditID, DEFAULT_AUDIT_TAG,
                    inlongGroupId, inlongStreamId, logTime, msgCount, event.getBody().length, auditVersion);
        } else {
            String groupId = headers.get(AttributeConstants.GROUP_ID);
            String streamId = headers.get(AttributeConstants.STREAM_ID);
            long dataTime = NumberUtils.toLong(headers.get(AttributeConstants.DATA_TIME));
            long msgCount = NumberUtils.toLong(headers.get(ConfigConstants.MSG_COUNTER_KEY));
            long auditVersion = getAuditVersion(headers);
            AuditOperator.getInstance().add(auditID, DEFAULT_AUDIT_TAG,
                    groupId, streamId, dataTime, msgCount, event.getBody().length, auditVersion);
        }
    }

    /**
     * Get LogTime from headers
     */
    public static long getLogTime(Map<String, String> headers) {
        String strLogTime = headers.get(Constants.HEADER_KEY_MSG_TIME);
        if (strLogTime == null) {
            strLogTime = headers.get(AttributeConstants.DATA_TIME);
        }
        if (strLogTime == null) {
            return System.currentTimeMillis();
        }
        long logTime = NumberUtils.toLong(strLogTime, 0);
        if (logTime == 0) {
            logTime = System.currentTimeMillis();
        }
        return logTime;
    }

    /**
     * Get LogTime from event
     */
    public static long getLogTime(Event event) {
        if (event != null) {
            return getLogTime(event.getHeaders());
        }
        return System.currentTimeMillis();
    }

    /**
     * Get AuditFormatTime
     */
    public static long getAuditFormatTime(long msgTime) {
        return msgTime - msgTime % CommonConfigHolder.getInstance().getAuditFormatInvlMs();
    }

    /**
     * Get Audit version
     *
     * @param headers  the message headers
     *
     * @return audit version
     */
    public static long getAuditVersion(Map<String, String> headers) {
        String strAuditVersion = headers.get(AttributeConstants.AUDIT_VERSION);
        if (StringUtils.isNotBlank(strAuditVersion)) {
            try {
                return Long.parseLong(strAuditVersion);
            } catch (Throwable ex) {
                //
            }
        }
        return -1L;
    }

    /**
     * Send audit data
     */
    public static void send() {
        AuditOperator.getInstance().flush();
    }
}

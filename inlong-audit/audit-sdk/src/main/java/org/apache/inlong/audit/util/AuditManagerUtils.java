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

package org.apache.inlong.audit.util;

import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.MetricIdEnum;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.FlowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Audit item ID generation rules: composed of basic audit item ID + extension bits.
 * Each module is assigned two basic audit item IDs, namely reception and transmission.
 * Based on reception and transmission, and expanded through extension bits,
 * audit item IDs such as success and failure, real-time and non-real-time, retry and discard can be generated.
 */
public class AuditManagerUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditManagerUtils.class);
    public static final int AUDIT_SUFFIX_LENGTH = 16;
    public static final int AUDIT_MAX_PREFIX_LENGTH = 14;
    public static final String AUDIT_DESCRIPTION_JOINER = "_";

    private static String buildSuccessAndFailureFlag(boolean success) {
        return success ? "0" : "1";
    }

    private static String buildRealtimeFlag(boolean isRealtime) {
        return isRealtime ? "0" : "1";
    }

    private static String buildDiscardFlag(boolean discard) {
        return discard ? "1" : "0";
    }

    private static String buildRetryFlag(boolean retry) {
        return retry ? "1" : "0";
    }

    private static String buildAuditIdSuffix(int auditId) {
        StringBuilder auditIdBinaryString = new StringBuilder(Integer.toBinaryString(auditId));
        for (int i = auditIdBinaryString.length(); i < AUDIT_SUFFIX_LENGTH; i++) {
            auditIdBinaryString.insert(0, "0");
        }
        return auditIdBinaryString.toString();
    }

    /**
     * Generate Audit item IDs.
     *
     * @param baseAuditId
     * @param success
     * @param isRealtime
     * @param discard
     * @param retry
     * @return
     */
    public static int buildAuditId(AuditIdEnum baseAuditId,
            boolean success,
            boolean isRealtime,
            boolean discard,
            boolean retry) {
        String auditPreFix = buildSuccessAndFailureFlag(success) +
                buildRealtimeFlag(isRealtime) +
                buildDiscardFlag(discard) +
                buildRetryFlag(retry);
        return Integer.parseInt(auditPreFix + buildAuditIdSuffix(baseAuditId.getValue()), 2);
    }

    public static AuditInformation buildAuditInformation(String auditType,
            FlowType flowType,
            boolean success,
            boolean isRealtime,
            boolean discard,
            boolean retry) {
        String auditPreFix = buildSuccessAndFailureFlag(success) +
                buildRealtimeFlag(isRealtime) +
                buildDiscardFlag(discard) +
                buildRetryFlag(retry);
        AuditIdEnum baseAuditId = AuditIdEnum.getAuditId(auditType, flowType);
        int auditId = Integer.parseInt(auditPreFix + buildAuditIdSuffix(baseAuditId.getValue()), 2);
        StringBuilder nameInEnglish = new StringBuilder()
                .append(baseAuditId.getAuditType().value())
                .append(AUDIT_DESCRIPTION_JOINER)
                .append(flowType.getNameInEnglish())
                .append(AUDIT_DESCRIPTION_JOINER);
        StringBuilder nameInChinese = new StringBuilder()
                .append(baseAuditId.getAuditType().value())
                .append(flowType.getNameInChinese());

        if (discard) {
            nameInEnglish.append("discard").append(AUDIT_DESCRIPTION_JOINER);
            nameInChinese.append("丢弃");
        }
        if (retry) {
            nameInEnglish.append("retry").append(AUDIT_DESCRIPTION_JOINER);
            nameInChinese.append("重试");
        }
        if (success) {
            nameInEnglish.append("success");
            nameInChinese.append("成功");
        } else {
            nameInEnglish.append("failed");
            nameInChinese.append("失败");
        }
        if (!isRealtime) {
            nameInEnglish.append("(CheckPoint)");
            nameInChinese.append("(CheckPoint)");
        }
        return new AuditInformation(auditId, nameInEnglish.toString(), nameInChinese.toString());
    }

    public static List<AuditInformation> getAllAuditInformation() {
        List<AuditInformation> auditInformationList = new LinkedList<>();
        for (AuditIdEnum auditIdEnum : AuditIdEnum.values()) {
            auditInformationList.addAll(combineAuditInformation(auditIdEnum.getAuditType().value(),
                    auditIdEnum.getFlowType()));
        }
        return auditInformationList;
    }

    /**
     * Obtain all metric Audit items.
     * All metric indicators are defined in the MetricIdEnum class.
     * @return List of AuditInformation objects representing the metric Audit items.
     */
    public static List<AuditInformation> getAllMetricInformation() {
        List<AuditInformation> metricInformationList = new LinkedList<>();
        for (MetricIdEnum metricIdEnum : MetricIdEnum.values()) {
            metricInformationList
                    .add(new AuditInformation(metricIdEnum.getValue(), metricIdEnum.getEnglishDescription(),
                            metricIdEnum.getChineseDescription()));
        }
        return metricInformationList;
    }

    public static List<AuditInformation> getAllAuditInformation(String auditType) {
        List<AuditInformation> auditInformationList = new LinkedList<>();
        for (AuditIdEnum auditIdEnum : AuditIdEnum.values()) {
            if (!auditIdEnum.getAuditType().value().equals(auditType)) {
                continue;
            }
            auditInformationList.addAll(combineAuditInformation(auditIdEnum.getAuditType().value(),
                    auditIdEnum.getFlowType()));
        }
        return auditInformationList;
    }

    private static List<AuditInformation> combineAuditInformation(String auditType, FlowType flowType) {
        List<AuditInformation> auditInformationList = new LinkedList<>();
        boolean[] combinations = {true, false};
        for (boolean success : combinations) {
            for (boolean isRealtime : combinations) {
                for (boolean discard : combinations) {
                    for (boolean retry : combinations) {
                        if (discard && retry) {
                            continue;
                        }
                        auditInformationList
                                .add(buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry));
                    }
                }
            }
        }
        return auditInformationList;
    }

    /**
     * Get max Audit ID.
     *
     * @return
     */
    public static int getStartAuditIdForMetric() {
        return 1 << (AUDIT_SUFFIX_LENGTH + AUDIT_MAX_PREFIX_LENGTH);
    }
}

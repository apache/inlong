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
import org.apache.inlong.audit.CdcIdEnum;
import org.apache.inlong.audit.MetricIdEnum;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.CdcType;
import org.apache.inlong.audit.entity.FlowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.inlong.audit.AuditIdEnum.AGENT_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.AGENT_OUTPUT;
import static org.apache.inlong.audit.AuditIdEnum.DATA_PROXY_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.DATA_PROXY_OUTPUT;
import static org.apache.inlong.audit.AuditIdEnum.SDK_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SDK_OUTPUT;
import static org.apache.inlong.audit.entity.AuditRules.START_CDC_INPUT_ID;
import static org.apache.inlong.audit.entity.AuditRules.START_CDC_OUTPUT_ID;
import static org.apache.inlong.audit.entity.AuditRules.START_METRIC_ID;

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
                        if (shouldIncludeCombination(auditType, flowType, success, isRealtime, discard, retry)) {
                            auditInformationList.add(
                                    buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry));
                        }
                    }
                }
            }
        }

        return auditInformationList;
    }

    /**
     * Exclude some uncommon audit scenarios
     * @param auditType
     * @param flowType
     * @param success
     * @param isRealtime
     * @param discard
     * @param retry
     * @return
     */
    private static boolean shouldIncludeCombination(String auditType, FlowType flowType, boolean success,
            boolean isRealtime, boolean discard, boolean retry) {
        // Exclude the situation when retry and discard occur at the same time
        if (discard && retry) {
            return false;
        }

        AuditIdEnum baseAuditId = AuditIdEnum.getAuditId(auditType, flowType);
        // Exclude the situation when non-real-time and one of SDK、Agent、DataProxy occur at the same time
        if (!isRealtime && isExcludedWhenNotRealtime(baseAuditId)) {
            return false;
        }

        // Exclude the situation when failed、input and one of discard and retry occur at the same time
        if (!success && flowType == FlowType.INPUT && (discard || retry)) {
            return false;
        }

        // Exclude the situation when failed、output and discard occur at the same time
        if (!success && flowType == FlowType.OUTPUT && discard) {
            return false;
        }

        // Exclude the situation when success、input and retry occur at the same time
        if (success && flowType == FlowType.INPUT && retry) {
            return false;
        }

        return true;
    }

    private static boolean isExcludedWhenNotRealtime(AuditIdEnum baseAuditId) {
        return baseAuditId == SDK_INPUT || baseAuditId == SDK_OUTPUT || baseAuditId == AGENT_INPUT
                || baseAuditId == AGENT_OUTPUT || baseAuditId == DATA_PROXY_INPUT || baseAuditId == DATA_PROXY_OUTPUT;
    }

    /**
     * Gets the maximum possible audit ID based on the defined bit length constraints.
     *
     * @return int The maximum audit ID calculated as:
     *         1 shifted left by (AUDIT_SUFFIX_LENGTH + AUDIT_MAX_PREFIX_LENGTH)
     *         This represents the upper bound of the regular audit ID space.
     */
    private static int getMaxAuditId() {
        return 1 << (AUDIT_SUFFIX_LENGTH + AUDIT_MAX_PREFIX_LENGTH);
    }

    /**
     * Gets the starting audit ID for metric items.
     *
     * @return int The starting audit ID for metrics, calculated as:
     *         maximum audit ID plus predefined metric ID offset.
     *         This ensures metric IDs are in a separate range from regular audit IDs.
     */
    public static int getStartAuditIdForMetric() {
        return getMaxAuditId() + START_METRIC_ID;
    }

    /**
     * Gets the starting audit ID for CDC (Change Data Capture) items.
     *
     * @return int The starting audit ID for CDC, calculated as:
     * maximum audit ID plus predefined CDC input ID offset.
     * This ensures CDC IDs are in a separate range from regular audit IDs.
     */
    public static int getStartAuditIdForCdcInput() {
        return getMaxAuditId() + START_CDC_INPUT_ID;
    }

    /**
     * Gets the starting audit ID for CDC output items.
     *
     * @return int The starting audit ID for CDC output, calculated as:
     * maximum audit ID plus predefined CDC output ID offset.
     * This ensures CDC output IDs are in a separate range from regular audit IDs.
     */
    public static int getStartAuditIdForCdcOutput() {
        return getMaxAuditId() + START_CDC_OUTPUT_ID;
    }

    /**
     * Get all CDC audit information by combining all flow types with all CDC ID enums.
     * @return List of AuditInformation containing all possible CDC audit combinations
     */
    public static List<AuditInformation> getAllCdcIdInformation() {
        List<AuditInformation> result = new ArrayList<>(FlowType.values().length * CdcIdEnum.values().length);
        for (FlowType flowType : FlowType.values()) {
            for (CdcIdEnum cdcIdEnum : CdcIdEnum.values()) {
                result.add(createAuditInformation(cdcIdEnum, flowType));
            }
        }
        return result;
    }

    /**
     * Get the CDC ID based on the audit type, flow type, and CDC type.
     * @param auditType The type of audit (e.g., MySQL, TDSQL)
     * @param flowType  The flow type (e.g., INPUT, OUTPUT)
     * @param cdcType   The CDC type (e.g., INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER)
     * @return int The corresponding CDC ID
     */
    public static int getCdcId(String auditType, FlowType flowType, CdcType cdcType) {
        return CdcIdEnum.getCdcId(auditType, flowType, cdcType);
    }

    /**
     * Get all CDC audit information based on the audit type.
     * @param auditType The type of audit (e.g., MySQL, TDSQL)
     * @return List of AuditInformation containing all possible CDC audit combinations for the given audit type
     */
    public static List<AuditInformation> getAllCdcIdInformation(String auditType) {
        List<AuditInformation> result = new ArrayList<>(FlowType.values().length * CdcIdEnum.values().length);
        for (FlowType flowType : FlowType.values()) {
            for (CdcIdEnum cdcIdEnum : CdcIdEnum.values()) {
                if (cdcIdEnum.getAuditType().value().equals(auditType)) {
                    result.add(createAuditInformation(cdcIdEnum, flowType));
                }
            }
        }
        return result;
    }

    /**
     * Get all CDC audit information based on audit type and flow type.
     * @param auditType The type of audit (e.g., MySQL, TDSQL)
     * @param flowType  The flow type (e.g., INPUT, OUTPUT)
     * @return List of AuditInformation containing matching CDC audit combinations
     */
    public static List<AuditInformation> getAllCdcIdInformation(String auditType, FlowType flowType) {
        List<AuditInformation> result = new ArrayList<>(CdcIdEnum.values().length);
        for (CdcIdEnum cdcIdEnum : CdcIdEnum.values()) {
            if (cdcIdEnum.getAuditType().value().equals(auditType)) {
                result.add(createAuditInformation(cdcIdEnum, flowType));
            }
        }
        return result;
    }

    /**
     * Get specific CDC audit information based on audit type, flow type and CDC type.
     * @param auditType The type of audit (e.g., MySQL, TDSQL)
     * @param flowType  The flow type (e.g., INPUT, OUTPUT)
     * @param cdcType   The CDC type (e.g., INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER)
     * @return AuditInformation matching the criteria, or null if not found
     */
    public static AuditInformation getCdcIdInformation(String auditType, FlowType flowType, CdcType cdcType) {
        for (CdcIdEnum cdcIdEnum : CdcIdEnum.values()) {
            if (cdcIdEnum.getAuditType().value().equals(auditType) && cdcIdEnum.getCdcType() == cdcType) {
                return createAuditInformation(cdcIdEnum, flowType);
            }
        }
        return null;
    }

    /**
     * Helper method to create AuditInformation from CdcIdEnum and FlowType.
     */
    private static AuditInformation createAuditInformation(CdcIdEnum cdcIdEnum, FlowType flowType) {
        return new AuditInformation(
                cdcIdEnum.getValue(flowType),
                cdcIdEnum.getEnglishDescription(flowType),
                cdcIdEnum.getChineseDescription(flowType));
    }

}

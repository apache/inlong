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

/**
 * Audit item ID generation rules: composed of basic audit item ID + extension bits.
 * Each module is assigned two basic audit item IDs, namely reception and transmission.
 * Based on reception and transmission, and expanded through extension bits,
 * audit item IDs such as success and failure, real-time and non-real-time, retry and discard can be generated.
 */
public class AuditManagerUtils {

    public static final int AUDIT_SUFFIX_LENGTH = 11;

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
     * Generate success and failure, real-time and non-real-time, retry, discard and other Audit item IDs through the baseline Audit ID.
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
}

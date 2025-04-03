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

package org.apache.inlong.audit;

import org.apache.inlong.audit.entity.AuditType;
import org.apache.inlong.audit.entity.CdcType;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.audit.exceptions.AuditTypeNotExistException;
import org.apache.inlong.audit.util.AuditManagerUtils;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.audit.entity.AuditType.MYSQL;
import static org.apache.inlong.audit.entity.AuditType.TDSQL_MYSQL;
import static org.apache.inlong.audit.entity.CdcType.DELETE;
import static org.apache.inlong.audit.entity.CdcType.INSERT;
import static org.apache.inlong.audit.entity.CdcType.UPDATE_AFTER;
import static org.apache.inlong.audit.entity.CdcType.UPDATE_BEFORE;

/**
 * Enumeration for CDC (Change Data Capture) audit identifiers.
 * Defines different audit types for various database operations (INSERT/DELETE/UPDATE)
 * on different database systems (MySQL/TDSQL).
 */
public enum CdcIdEnum {

    MYSQL_INSERT(1, INSERT, MYSQL, "Insert Audit Metrics for MySQL"),
    MYSQL_DELETE(2, DELETE, MYSQL, "Delete Audit Metrics for MySQL"),
    MYSQL_UPDATE_BEFORE(3, UPDATE_BEFORE, MYSQL, "Update before Audit Metrics for MySQL"),
    MYSQL_UPDATE_AFTER(4, UPDATE_AFTER, MYSQL, "Update after Audit Metrics for MySQL"),

    TDSQL_INSERT(101, INSERT, TDSQL_MYSQL, "Insert Audit Metrics for TDSQL"),
    TDSQL_DELETE(102, DELETE, TDSQL_MYSQL, "Delete Audit Metrics for TDSQL"),
    TDSQL_UPDATE_BEFORE(103, UPDATE_BEFORE, TDSQL_MYSQL, "Update before Audit Metrics for TDSQL"),
    TDSQL_UPDATE_AFTER(104, UPDATE_AFTER, TDSQL_MYSQL, "Update after Audit Metrics for TDSQL");

    private static final Logger LOGGER = LoggerFactory.getLogger(CdcIdEnum.class);
    private final int auditId;
    @Getter
    private final CdcType cdcType;
    @Getter
    private final AuditType auditType;
    @Getter
    private final String description;

    CdcIdEnum(int auditId, CdcType cdcType, AuditType auditType, String description) {
        this.auditId = auditId;
        this.cdcType = cdcType;
        this.auditType = auditType;
        this.description = description;
    }

    public int getValue(FlowType flowType) {
        if (flowType == null) {
            LOGGER.error("Invalid flow type: must not be null");
            return -1;
        }

        try {
            return auditId + (flowType == FlowType.INPUT
                    ? AuditManagerUtils.getStartAuditIdForCdcInput()
                    : AuditManagerUtils.getStartAuditIdForCdcOutput());
        } catch (Exception e) {
            LOGGER.error("Failed to get audit ID for flow type: {}", flowType, e);
            return -1;
        }
    }

    public String getEnglishDescription(FlowType flowType) {
        return String.join("",
                auditType.value(),
                flowType.getNameInEnglish(),
                cdcType.getNameInEnglish());
    }

    public String getChineseDescription(FlowType flowType) {
        return String.join("",
                auditType.value(),
                flowType.getNameInChinese(),
                cdcType.getNameInChinese());
    }

    public static CdcIdEnum getCdcIdEnum(String auditType, CdcType cdcType) {
        if (auditType == null || cdcType == null) {
            throw new IllegalArgumentException("Audit type and CDC type must not be null");
        }

        for (CdcIdEnum cdcIdEnum : CdcIdEnum.values()) {
            if (cdcIdEnum.getCdcType() == cdcType &&
                    auditType.equalsIgnoreCase(cdcIdEnum.getAuditType().value())) {
                return cdcIdEnum;
            }
        }

        String errorMsg = String.format("Audit type %s does not exist for cdc type %s", auditType, cdcType);
        LOGGER.error(errorMsg);
        throw new AuditTypeNotExistException(errorMsg);
    }

    public static int getCdcId(String auditType, FlowType flowType, CdcType cdcType) {
        CdcIdEnum cdcIdEnum = getCdcIdEnum(auditType, cdcType);
        return cdcIdEnum.getValue(flowType);
    }
}

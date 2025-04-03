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

import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.audit.CdcIdEnum;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.CdcType;
import org.apache.inlong.audit.entity.FlowType;

import org.junit.Test;

import java.util.List;

import static org.apache.inlong.audit.AuditIdEnum.AGENT_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.DATA_PROXY_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_HIVE_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_STARROCKS_INPUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuditManagerUtilsTest {

    @Test
    public void buildAuditInformation() {
        String auditType = "Agent";
        FlowType flowType = FlowType.INPUT;
        boolean success = true;
        boolean isRealtime = true;
        boolean discard = false;
        boolean retry = false;
        AuditInformation auditInfo =
                AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(AGENT_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("Agent 接收成功", auditInfo.getNameInChinese());

        auditType = "DataProxy";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(DATA_PROXY_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("DataProxy 接收成功", auditInfo.getNameInChinese());

        auditType = "Hive";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(SORT_HIVE_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("Hive 接收成功", auditInfo.getNameInChinese());

        auditType = "StarRocks";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(SORT_STARROCKS_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("StarRocks 接收成功", auditInfo.getNameInChinese());

        // Test the scenario of dataFlow case compatibility.
        auditType = "agent";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(AGENT_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("Agent 接收成功", auditInfo.getNameInChinese());

        auditType = "dataProxy";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(DATA_PROXY_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("DataProxy 接收成功", auditInfo.getNameInChinese());

        auditType = "hive";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(SORT_HIVE_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("Hive 接收成功", auditInfo.getNameInChinese());
        assertEquals(7, auditInfo.getAuditId());

        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, true, false, false, false);
        assertEquals(262151, auditInfo.getAuditId());
        assertEquals("Hive 接收成功(CheckPoint)", auditInfo.getNameInChinese());

        auditType = "STARROCKS";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals(SORT_STARROCKS_INPUT.getValue(), auditInfo.getAuditId());
        assertEquals("StarRocks 接收成功", auditInfo.getNameInChinese());

        // Test send failed audit items.
        auditType = "Agent";
        flowType = FlowType.OUTPUT;
        success = false;
        isRealtime = true;
        discard = false;
        retry = false;
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals("Agent 发送失败", auditInfo.getNameInChinese());

        auditType = "DataProxy";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals("DataProxy 发送失败", auditInfo.getNameInChinese());

        auditType = "Hive";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals("Hive 发送失败", auditInfo.getNameInChinese());

        auditType = "StarRocks";
        auditInfo = AuditManagerUtils.buildAuditInformation(auditType, flowType, success, isRealtime, discard, retry);
        assertEquals("StarRocks 发送失败", auditInfo.getNameInChinese());
    }

    @Test
    public void getAllAuditInformation() {
        List<AuditInformation> auditInfoList = AuditManagerUtils.getAllAuditInformation();
        System.out.println(auditInfoList);
        assertTrue(auditInfoList.size() > 0);

        auditInfoList.clear();
        auditInfoList = AuditManagerUtils.getAllAuditInformation("Agent");

        assertTrue(auditInfoList.size() > 0);
    }

    @Test
    public void getStartAuditIdForMetric() {
        int auditId = AuditManagerUtils.getStartAuditIdForMetric();
        assertTrue(auditId > 0);
        assertTrue(auditId <= (1 << 30));
    }

    @Test
    public void getAllMetricInformation() {
        List<AuditInformation> metricInformationList = AuditOperator.getInstance().getAllMetricInformation();
        System.out.println(metricInformationList);
        assertTrue(metricInformationList.size() > 0);
    }

    @Test
    public void getAllCdcIdInformation() {
        List<AuditInformation> cdcIdInfoList = AuditManagerUtils.getAllCdcIdInformation();
        assertNotNull("CDC ID information list should not be null", cdcIdInfoList);
        assertFalse("CDC ID information list should not be empty", cdcIdInfoList.isEmpty());
    }

    @Test
    public void getCdcIdEnum() {
        CdcType cdcType = CdcType.INSERT;
        String auditType = "TDSQL_MYSQL";
        CdcIdEnum cdcIdEnum = CdcIdEnum.getCdcIdEnum(auditType, cdcType);
        assertNotNull("CDC ID enum should not be null", cdcIdEnum);
        assertTrue("CDC ID value should be greater than 0", cdcIdEnum.getValue(FlowType.INPUT) > 0);
    }

    @Test
    public void getCdcId() {
        int expectedCdcId = 1073841925;
        int actualCdcId = AuditManagerUtils.getCdcId("TDSQL_MYSQL", FlowType.INPUT, CdcType.INSERT);
        assertEquals("CDC ID should match expected value", expectedCdcId, actualCdcId);
    }

    @Test
    public void getAllCdcIdInformationWithAuditType() {
        String auditType = "MYSQL";
        List<AuditInformation> cdcIdInfoList = AuditManagerUtils.getAllCdcIdInformation(auditType);
        assertNotNull("CDC ID information list should not be null", cdcIdInfoList);
        assertFalse("CDC ID information list should not be empty", cdcIdInfoList.isEmpty());

        for (AuditInformation info : cdcIdInfoList) {
            assertTrue("Name should contain audit type",
                    info.getNameInEnglish().toUpperCase().contains(auditType));
        }
    }

    @Test
    public void getAllCdcIdInformationWithAuditTypeAndFlowType() {
        String auditType = "MYSQL";
        FlowType flowType = FlowType.INPUT;
        List<AuditInformation> cdcIdInfoList = AuditManagerUtils.getAllCdcIdInformation(auditType, flowType);
        assertNotNull("CDC ID information list should not be null", cdcIdInfoList);

        for (AuditInformation info : cdcIdInfoList) {
            assertTrue("Name should contain receive for input flow",
                    info.getNameInEnglish().toLowerCase().contains("receive"));
        }
    }

    @Test
    public void getCdcIdInformationWithAuditTypeAndFlowTypeAndCdcType() {
        String auditType = "MYSQL";
        FlowType flowType = FlowType.INPUT;
        CdcType cdcType = CdcType.INSERT;
        int expectedAuditId = 1073841825;
        String expectedName = "MYSQL 接收写入";

        AuditInformation cdcIdInfo = AuditManagerUtils.getCdcIdInformation(auditType, flowType, cdcType);
        assertNotNull("CDC ID information should not be null", cdcIdInfo);
        assertEquals("Audit ID should match expected value", expectedAuditId, cdcIdInfo.getAuditId());
        assertEquals("Name in Chinese should match expected value", expectedName, cdcIdInfo.getNameInChinese());
    }

}

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
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.FlowType;

import org.junit.Test;

import java.util.List;

import static org.apache.inlong.audit.AuditIdEnum.AGENT_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.DATA_PROXY_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_HIVE_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_STARROCKS_INPUT;
import static org.junit.Assert.assertEquals;
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
}

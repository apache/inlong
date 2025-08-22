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

package org.apache.inlong.agent.metrics;

import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.audit.MetricIdEnum;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAuditUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestAuditUtils.class);

    @BeforeClass
    public static void setup() {

    }

    @Test
    public void testAuditId() {
        int expected = AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS;
        int real = AuditOperator.getInstance().buildSuccessfulAuditId(AuditIdEnum.AGENT_INPUT);
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS;
        real = AuditOperator.getInstance().buildSuccessfulAuditId(AuditIdEnum.AGENT_OUTPUT);
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_READ_FAILED;
        real = AuditOperator.getInstance().buildFailedAuditId(AuditIdEnum.AGENT_INPUT);
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_FAILED;
        real = AuditOperator.getInstance().buildFailedAuditId(AuditIdEnum.AGENT_OUTPUT);
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_RESEND;
        real = AuditOperator.getInstance().buildRetryAuditId(AuditIdEnum.AGENT_OUTPUT);
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS_REAL_TIME;
        real = MetricIdEnum.AGENT_READ_SUCCESS_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS_REAL_TIME;
        real = MetricIdEnum.AGENT_SEND_SUCCESS_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_READ_FAILED_REAL_TIME;
        real = MetricIdEnum.AGENT_READ_FAILED_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_FAILED_REAL_TIME;
        real = MetricIdEnum.AGENT_SEND_FAILED_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_TRY_SEND_REAL_TIME;
        real = MetricIdEnum.AGENT_TRY_SEND_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_EXCEPTION_REAL_TIME;
        real = MetricIdEnum.AGENT_SEND_EXCEPTION_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_RESEND_REAL_TIME;
        real = MetricIdEnum.AGENT_RESEND_REAL_TIME.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_SEND_EXCEPTION;
        real = MetricIdEnum.AGENT_SEND_EXCEPTION.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_TRY_SEND;
        real = MetricIdEnum.AGENT_TRY_SEND.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_DB;
        real = MetricIdEnum.AGENT_ADD_INSTANCE_DB.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_DB;
        real = MetricIdEnum.AGENT_DEL_INSTANCE_DB.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM;
        real = MetricIdEnum.AGENT_ADD_INSTANCE_MEM.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM;
        real = MetricIdEnum.AGENT_DEL_INSTANCE_MEM.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_TASK_MGR_HEARTBEAT;
        real = MetricIdEnum.AGENT_TASK_MGR_HEARTBEAT.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_TASK_HEARTBEAT;
        real = MetricIdEnum.AGENT_TASK_HEARTBEAT.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_INSTANCE_MGR_HEARTBEAT;
        real = MetricIdEnum.AGENT_INSTANCE_MGR_HEARTBEAT.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_INSTANCE_HEARTBEAT;
        real = MetricIdEnum.AGENT_INSTANCE_HEARTBEAT.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM_FAILED;
        real = MetricIdEnum.AGENT_ADD_INSTANCE_MEM_FAILED.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);

        expected = AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM_UNUSUAL;
        real = MetricIdEnum.AGENT_DEL_INSTANCE_MEM_UNUSUAL.getValue();
        Assert.assertTrue(real + " != " + expected, expected == real);
    }

}

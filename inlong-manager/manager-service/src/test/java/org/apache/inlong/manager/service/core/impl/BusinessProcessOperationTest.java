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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.view.ProcessView;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.service.BaseTest;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.mocks.MockPlugin;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.apache.inlong.manager.service.workflow.WorkflowResult;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class BusinessProcessOperationTest extends BaseTest {

    private static final String OPERATOR = "operator";

    private static final String BIZ_NAME = "test_biz";

    private static final String GROUP_ID = "b_test_biz";

    @Autowired
    private BusinessService businessService;

    @Autowired
    private BusinessProcessOperation businessProcessOperation;

    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    public void before(int status) {
        MockPlugin mockPlugin = new MockPlugin();
        serviceTaskListenerFactory.acceptPlugin(mockPlugin);
        BusinessInfo businessInfo = new BusinessInfo();
        businessInfo.setInlongGroupId(GROUP_ID);
        businessInfo.setName(BIZ_NAME);
        businessInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        BusinessPulsarInfo pulsarInfo = new BusinessPulsarInfo();
        pulsarInfo.setInlongGroupId(GROUP_ID);
        businessInfo.setMqExtInfo(pulsarInfo);
        businessService.save(businessInfo, OPERATOR);
        businessInfo.setStatus(status);
        businessService.update(businessInfo, OPERATOR);
    }

    @Test
    public void testStartProcess() {
        before(EntityStatus.BIZ_WAIT_SUBMIT.getCode());
        WorkflowResult result = businessProcessOperation.startProcess(GROUP_ID, OPERATOR);
        ProcessView processView = result.getProcessInfo();
        Assert.assertTrue(processView.getState() == ProcessState.PROCESSING);
        BusinessInfo businessInfo = businessService.get(GROUP_ID);
        Assert.assertTrue(businessInfo.getStatus().equals(EntityStatus.BIZ_WAIT_SUBMIT.getCode()));
    }

    @Test
    public void testSuspendProcess() {
        before(EntityStatus.BIZ_APPROVE_PASSED.getCode());
        WorkflowResult result = businessProcessOperation.suspendProcess(GROUP_ID, OPERATOR);
        ProcessView processView = result.getProcessInfo();
        Assert.assertTrue(processView.getState() == ProcessState.COMPLETED);
        BusinessInfo businessInfo = businessService.get(GROUP_ID);
        Assert.assertTrue(businessInfo.getStatus().equals(EntityStatus.BIZ_SUSPEND.getCode()));
    }

    @Test
    public void testRestartProcess() {
        before(EntityStatus.BIZ_SUSPEND.getCode());
        WorkflowResult result = businessProcessOperation.restartProcess(GROUP_ID, OPERATOR);
        ProcessView processView = result.getProcessInfo();
        Assert.assertTrue(processView.getState() == ProcessState.COMPLETED);
        BusinessInfo businessInfo = businessService.get(GROUP_ID);
        Assert.assertTrue(businessInfo.getStatus().equals(EntityStatus.BIZ_RESTART.getCode()));
    }

    @Test
    public void testDeleteProcess() {
        before(EntityStatus.BIZ_RESTART.getCode());
        boolean result = businessProcessOperation.deleteProcess(GROUP_ID, OPERATOR);
        Assert.assertTrue(result);
    }
}

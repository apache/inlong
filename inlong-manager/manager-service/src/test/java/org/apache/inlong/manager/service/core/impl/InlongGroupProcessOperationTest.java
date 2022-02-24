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

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.mocks.MockPlugin;
import org.apache.inlong.manager.service.workflow.ServiceTaskListenerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Rollback
@EnableAutoConfiguration
public class InlongGroupProcessOperationTest extends ServiceBaseTest {

    private static final String OPERATOR = "operator";

    private static final String GROUP_NAME = "test_biz";

    private static final String GROUP_ID = "b_test_biz";

    @Autowired
    private InlongGroupService groupService;

    @Autowired
    private InlongGroupProcessOperation groupProcessOperation;

    @Autowired
    private ServiceTaskListenerFactory serviceTaskListenerFactory;

    public void before(int status) {
        MockPlugin mockPlugin = new MockPlugin();
        serviceTaskListenerFactory.acceptPlugin(mockPlugin);
        InlongGroupRequest groupInfo = new InlongGroupRequest();
        groupInfo.setInlongGroupId(GROUP_ID);
        groupInfo.setName(GROUP_NAME);
        groupInfo.setInCharges(OPERATOR);
        groupInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
        InlongGroupPulsarInfo pulsarInfo = new InlongGroupPulsarInfo();
        pulsarInfo.setInlongGroupId(GROUP_ID);
        groupInfo.setMqExtInfo(pulsarInfo);
        groupService.save(groupInfo, OPERATOR);
        groupInfo.setStatus(status);
        groupService.update(groupInfo, OPERATOR);
    }

    @Test
    public void testStartProcess() {
        before(GroupState.GROUP_WAIT_SUBMIT.getCode());
        WorkflowResult result = groupProcessOperation.startProcess(GROUP_ID, OPERATOR);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.PROCESSING);
        InlongGroupRequest groupInfo = groupService.get(GROUP_ID);
        Assert.assertEquals(groupInfo.getStatus(), GroupState.GROUP_WAIT_APPROVAL.getCode());
    }

    @Test
    public void testSuspendProcess() {
        testStartProcess();
        InlongGroupRequest groupInfo = groupService.get(GROUP_ID);
        groupInfo.setStatus(GroupState.GROUP_APPROVE_PASSED.getCode());
        groupService.update(groupInfo, OPERATOR);
        groupInfo.setStatus(GroupState.GROUP_CONFIG_ING.getCode());
        groupService.update(groupInfo, OPERATOR);
        groupInfo.setStatus(GroupState.GROUP_CONFIG_SUCCESSFUL.getCode());
        groupService.update(groupInfo, OPERATOR);
        groupInfo = groupService.get(GROUP_ID);
        WorkflowResult result = groupProcessOperation.suspendProcess(GROUP_ID, OPERATOR);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        groupInfo = groupService.get(GROUP_ID);
        Assert.assertEquals(groupInfo.getStatus(), GroupState.GROUP_SUSPEND.getCode());
    }

    @Test
    public void testRestartProcess() {
        testSuspendProcess();
        WorkflowResult result = groupProcessOperation.restartProcess(GROUP_ID, OPERATOR);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        InlongGroupRequest groupInfo = groupService.get(GROUP_ID);
        Assert.assertEquals(groupInfo.getStatus(), EntityStatus.GROUP_RESTART.getCode());
    }

    @Test
    public void testDeleteProcess() {
        testRestartProcess();
        boolean result = groupProcessOperation.deleteProcess(GROUP_ID, OPERATOR);
        Assert.assertTrue(result);
    }
}

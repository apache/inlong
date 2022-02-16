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

package org.apache.inlong.manager.service.workflow;

import java.util.List;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.event.task.QueueOperateListener;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.workflow.bussiness.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarResourceTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeTopicTaskListener;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ServiceTaskListenerFactoryTest extends ServiceBaseTest {

    @Autowired
    ServiceTaskListenerFactory serviceTaskListenerFactory;

    @Test
    public void testGetQueueOperateListener() {
        WorkflowContext context = new WorkflowContext();
        BusinessResourceWorkflowForm processForm = new BusinessResourceWorkflowForm();
        BusinessInfo businessInfo = new BusinessInfo();
        //check pulsar listener
        businessInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        processForm.setBusinessInfo(businessInfo);
        context.setProcessForm(processForm);
        List<QueueOperateListener> queueOperateListeners = serviceTaskListenerFactory.getQueueOperateListener(context);
        Assert.assertTrue(queueOperateListeners.size() == 2);
        Assert.assertTrue(queueOperateListeners.get(0) instanceof CreatePulsarResourceTaskListener);
        Assert.assertTrue(queueOperateListeners.get(1) instanceof CreatePulsarGroupTaskListener);
        //check tube listener
        businessInfo.setMiddlewareType(BizConstant.MIDDLEWARE_TUBE);
        queueOperateListeners = serviceTaskListenerFactory.getQueueOperateListener(context);
        Assert.assertTrue(queueOperateListeners.size() == 2);
        Assert.assertTrue(queueOperateListeners.get(0) instanceof CreateTubeTopicTaskListener);
        Assert.assertTrue(queueOperateListeners.get(1) instanceof CreateTubeGroupTaskListener);
    }

}

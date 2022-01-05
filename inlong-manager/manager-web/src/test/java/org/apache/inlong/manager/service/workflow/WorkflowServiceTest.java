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

import com.github.pagehelper.PageInfo;
import java.util.Collections;
import java.util.Date;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.apache.inlong.manager.workflow.core.WorkflowDataAccessor;
import org.apache.inlong.manager.workflow.model.ProcessState;
import org.apache.inlong.manager.workflow.model.instance.ProcessInstance;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class WorkflowServiceTest extends ServiceBaseTest {

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowDataAccessor workflowDataAccessor;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Test
    public void testTransactionManager(){
        System.out.println(platformTransactionManager.getClass().getName());
        DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        transactionDefinition.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);

        TransactionStatus status = platformTransactionManager.getTransaction(transactionDefinition);
        System.out.println(status.getClass().getName());
        platformTransactionManager.commit(status);
    }

    @Test
    public void testListTaskExecuteLogs() {
        // insert process instance
        String groupId = "test_business";
        ProcessInstance process = new ProcessInstance()
                .setId(1)
                .setInlongGroupId(groupId)
                .setName("CREATE_BUSINESS_RESOURCE")
                .setDisplayName("wedata_test")
                .setApplicant("wedata")
                .setStartTime(new Date())
                .setHidden(true)
                .setState(ProcessState.COMPLETED.name());
        workflowDataAccessor.processInstanceStorage().insert(process);

        // insert task instance
        TaskInstance task = new TaskInstance()
                .setId(1)
                .setType("ServiceTask")
                .setProcessName("CREATE_BUSINESS_RESOURCE")
                .setProcessDisplayName("wedata_test")
                .setApplicant("wedata")
                .setName("task")
                .setDisplayName("wedata_test")
                .setApprovers("kipshi")
                .setState("start")
                .setStartTime(new Date())
                .setProcessInstId(1);
        workflowDataAccessor.taskInstanceStorage().insert(task);
        // query execute logs
        WorkflowTaskExecuteLogQuery query = new WorkflowTaskExecuteLogQuery();
        query.setInlongGroupId(groupId);
        query.setProcessNames(Collections.singletonList("CREATE_BUSINESS_RESOURCE"));
        PageInfo<WorkflowTaskExecuteLog> logPageInfo = workflowService.listTaskExecuteLogs(query);

        Assert.assertEquals(1, logPageInfo.getTotal());
    }

}
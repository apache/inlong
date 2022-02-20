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

package org.apache.inlong.manager.service.thirdpart.hive;

import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.workflow.bussiness.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CreateHiveTableEventSelectorTest extends ServiceBaseTest {

    @Autowired
    CreateHiveTableEventSelector createHiveTableEventSelector;

    @Test
    public void testAccept() {
        WorkflowContext workflowContext = new WorkflowContext();
        BusinessResourceWorkflowForm processForm = new BusinessResourceWorkflowForm();
        workflowContext.setProcessForm(processForm);
        Assert.assertFalse(createHiveTableEventSelector.accept(workflowContext));
        processForm.setBusinessInfo(new BusinessInfo());
        Assert.assertFalse(createHiveTableEventSelector.accept(workflowContext));
        processForm.getBusinessInfo().setInlongGroupId("test");
        Assert.assertTrue(createHiveTableEventSelector.accept(workflowContext));
    }

}

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

package org.apache.inlong.manager.service.thirdparty.hive;

import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class HiveSinkEventSelectorTest extends ServiceBaseTest {

    @Autowired
    HiveSinkEventSelector hiveSinkEventSelector;

    @Test
    public void testAccept() {
        WorkflowContext workflowContext = new WorkflowContext();
        GroupResourceProcessForm processForm = new GroupResourceProcessForm();
        workflowContext.setProcessForm(processForm);
        Assert.assertFalse(hiveSinkEventSelector.accept(workflowContext));
        processForm.setGroupInfo(new InlongGroupInfo());
        Assert.assertFalse(hiveSinkEventSelector.accept(workflowContext));
        processForm.getGroupInfo().setInlongGroupId("test");
        Assert.assertTrue(hiveSinkEventSelector.accept(workflowContext));
    }

}

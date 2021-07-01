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

package org.apache.inlong.manager.workflow.model.definition;

import com.google.common.collect.Lists;

import org.apache.inlong.manager.workflow.model.WorkflowContext;

import java.util.List;

/**
 * Fixed approver
 */
public class FixedApproverAssign implements ApproverAssign {

    private List<String> approvers;

    public FixedApproverAssign(List<String> approvers) {
        this.approvers = approvers;
    }

    public static ApproverAssign of(List<String> approvers) {
        return new FixedApproverAssign(approvers);
    }

    public static ApproverAssign of(String... approvers) {
        return new FixedApproverAssign(Lists.newArrayList(approvers));
    }

    @Override
    public List<String> assign(WorkflowContext context) {
        return approvers;
    }

}

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

package org.apache.inlong.manager.common.workflow;

import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.TaskForm;

import java.util.List;

/**
 * Task service interface
 */
public interface TaskService {

    /**
     * Approval and consent
     *
     * @param taskId
     * @param form
     * @param operator
     * @return
     */
    WorkflowContext approve(Integer taskId, String remark, TaskForm form, String operator);

    /**
     * reject
     *
     * @param taskId
     * @param remark
     * @param operator
     * @return
     */
    WorkflowContext reject(Integer taskId, String remark, String operator);

    /**
     * Change approver
     *
     * @param taskId
     * @param to
     * @param operator
     * @return
     */
    WorkflowContext transfer(Integer taskId, String remark, List<String> to, String operator);

    /**
     * Complete tasks-only true automatic tasks for exceptions
     *
     * @param taskId   System task ID
     * @param remark   Remarks
     * @param operator Operator
     * @return
     */
    WorkflowContext complete(Integer taskId, String remark, String operator);

}

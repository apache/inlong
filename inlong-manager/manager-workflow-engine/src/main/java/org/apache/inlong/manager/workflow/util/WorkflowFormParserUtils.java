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

package org.apache.inlong.manager.workflow.util;

import com.fasterxml.jackson.databind.JavaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.workflow.exception.FormParseException;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.definition.ProcessForm;
import org.apache.inlong.manager.workflow.model.definition.Task;
import org.apache.inlong.manager.workflow.model.definition.TaskForm;
import org.apache.inlong.manager.workflow.model.definition.UserTask;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;

/**
 * Process form analysis tool
 */
@Slf4j
public class WorkflowFormParserUtils {

    /**
     * Parse the task form in JSON string format into a Task instance
     */
    public static <T extends TaskForm> T parseTaskForm(TaskInstance taskInstance, Process process)
            throws FormParseException {
        Preconditions.checkNotNull(taskInstance, "taskInstance can't be null");
        Preconditions.checkNotNull(process, "process can't be null");

        if (StringUtils.isEmpty(taskInstance.getFormData())) {
            return null;
        }

        Task task = process.getTaskByName(taskInstance.getName());
        Preconditions.checkNotNull(task, () -> "user task not exist " + taskInstance.getName());
        Preconditions.checkTrue(task instanceof UserTask, () -> "task should be userTask " + taskInstance.getName());

        UserTask userTask = (UserTask) task;
        try {
            JavaType javaType = JsonUtils.MAPPER.constructType(userTask.getFormClass());
            return JsonUtils.parse(taskInstance.getFormData(), javaType);
        } catch (Exception e) {
            log.error("task form parse failed! formData is: {}", taskInstance.getFormData(), e);
            throw new FormParseException("task form parse failed!");
        }
    }

    /**
     * Parse the process form in JSON string format into a Process instance
     */
    public static <T extends ProcessForm> T parseProcessForm(String formDate, Process process)
            throws FormParseException {
        Preconditions.checkNotNull(process, "process can't be null");

        if (StringUtils.isEmpty(formDate)) {
            return null;
        }

        try {
            JavaType javaType = JsonUtils.MAPPER.constructType(process.getFormClass());
            return JsonUtils.parse(formDate, javaType);
        } catch (Exception e) {
            log.error("process form parse failed! formData is: {}", formDate, e);
            throw new FormParseException("process form parse failed!");
        }
    }
}

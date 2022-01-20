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

import io.swagger.annotations.ApiModelProperty;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.model.instance.EventLog;
import org.apache.inlong.manager.common.model.instance.TaskInstance;

/**
 * Workflow system task execution log
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowTaskExecuteLog {

    @ApiModelProperty("Process ID")
    private Integer processInstId;

    @ApiModelProperty("Process name")
    private String processDisplayName;

    @ApiModelProperty("Process status: same task status, such as processing: PROCESSING, "
            + "completed: COMPLETED, rejected: REJECTED, cancelled: CANCELED, terminated: TERMINATED")
    private String state;

    @ApiModelProperty("Start time")
    private Date startTime;

    @ApiModelProperty("End time")
    private Date endTime;

    @ApiModelProperty("Task execution log")
    private List<TaskExecutorLog> taskExecutorLogs;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class TaskExecutorLog {

        @ApiModelProperty("Task type")
        private String taskType;

        @ApiModelProperty("Task ID")
        private Integer taskInstId;

        @ApiModelProperty("Task name")
        private String taskDisplayName;

        @ApiModelProperty("Execution status: same task status, such as "
                + "complete: COMPLETE; failure: FAILED; processing: PENDING")
        private String state;

        @ApiModelProperty("Start time")
        private Date startTime;

        @ApiModelProperty("End time")
        private Date endTime;

        @ApiModelProperty("Listener execution log")
        private List<ListenerExecutorLog> listenerExecutorLogs;

        public static TaskExecutorLog buildFromTaskInst(TaskInstance taskInstance) {
            return TaskExecutorLog.builder()
                    .taskType(taskInstance.getType())
                    .taskInstId(taskInstance.getId())
                    .taskDisplayName(taskInstance.getDisplayName())
                    .state(taskInstance.getState())
                    .startTime(taskInstance.getStartTime())
                    .endTime(taskInstance.getEndTime())
                    .build();
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class ListenerExecutorLog {

        @ApiModelProperty("id")
        private Integer id;

        @ApiModelProperty("Event type")
        private String eventType;

        @ApiModelProperty("Event")
        private String event;

        @ApiModelProperty("Listener name")
        private String listener;

        @ApiModelProperty("Status")
        private Integer state;

        @ApiModelProperty("Is it synchronized")
        private Boolean async;

        @ApiModelProperty("Execute IP")
        private String ip;

        @ApiModelProperty("Start time")
        private Date startTime;

        @ApiModelProperty("End time")
        private Date endTime;

        @ApiModelProperty("Execution result information")
        private String remark;

        @ApiModelProperty("Exception")
        private String exception;

        @ApiModelProperty("Description")
        private String description;

        public static ListenerExecutorLog fromEventLog(EventLog eventLog) {
            ListenerExecutorLog executorLog = ListenerExecutorLog.builder()
                    .id(eventLog.getId())
                    .eventType(eventLog.getEventType())
                    .event(eventLog.getEvent())
                    .listener(eventLog.getListener())
                    .state(eventLog.getState())
                    .async(eventLog.getAsync())
                    .ip(eventLog.getIp())
                    .startTime(eventLog.getStartTime())
                    .endTime(eventLog.getEndTime())
                    .remark(eventLog.getRemark())
                    .exception(eventLog.getException())
                    .build();
            executorLog.buildDescription();
            return executorLog;
        }

        private void buildDescription() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sb = sdf.format(startTime) + " ~ " + sdf.format(endTime) + " [" + listener + "] "
                    + "event: [" + event + "], executed [" + (state == 1 ? "success" : "failed") + "], "
                    + "remark: [" + remark + "], exception: [" + exception + "]";
            this.setDescription(sb);
        }
    }

}

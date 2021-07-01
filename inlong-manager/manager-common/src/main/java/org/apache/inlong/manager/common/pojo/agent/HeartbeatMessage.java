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

package org.apache.inlong.manager.common.pojo.agent;

import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModel;
import java.util.List;
import lombok.Data;

/**
 * Heartbeat message
 */
@Data
@ApiModel("Heartbeat message")
public class HeartbeatMessage {

    @SerializedName("ip")
    private String ip;

    @SerializedName("version")
    private String version;

    @SerializedName("startupTime")
    private String startupTime;

    @SerializedName("time")
    private String time;

    @SerializedName("firstReport")
    private boolean firstReport;

    @SerializedName("bufferUsage")
    private String bufferUsage;

    @SerializedName("sendQueueUsage")
    private String sendQueueUsage;

    @SerializedName("retryQueueUsage")
    private String retryQueueUsage;

    @SerializedName("usedMemoryPercent")
    private String usedMemoryPercent;

    @SerializedName("usedDiskPercent")
    private String usedDiskPercent;

    @SerializedName("taskInfos")
    private List<TaskInfo> taskInfos;

    public static class TaskInfo {

        @SerializedName("taskId")
        private int taskId;

        @SerializedName("delayTime")
        private String delayTime;

        public int getTaskId() {
            return taskId;
        }

        public void setTaskId(int taskId) {
            this.taskId = taskId;
        }

        public String getDelayTime() {
            return delayTime;
        }

        public void setDelayTime(String delayTime) {
            this.delayTime = delayTime;
        }
    }

}

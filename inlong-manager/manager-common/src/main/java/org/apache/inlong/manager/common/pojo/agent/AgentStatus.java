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
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import lombok.Data;

@Data
@ApiModel("Agent status info")
public class AgentStatus {

    @ApiModelProperty(value = "Current time")
    private String time;

    @ApiModelProperty(value = "Agent version")
    private String version;

    @ApiModelProperty(value = "Agent register info")
    private List<RegisterInfo> registerInfo;

    @ApiModelProperty(value = "Information being read")
    private ReadingInfo readingInfo;

    @ApiModelProperty(value = "Retry data list")
    private List<RetryInfo> retryInfo;

    @ApiModelProperty(value = "Documents that have not been completed")
    private List<RetryUnfinishedFile> retryUnfinishedFile;

    public static class RegisterInfo {

        @SerializedName("taskId")
        private long taskId;

        @SerializedName("registerMsg")
        private String registerMsg;

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public String getRegisterMsg() {
            return registerMsg;
        }

        public void setRegisterMsg(String registerMsg) {
            this.registerMsg = registerMsg;
        }
    }

    public static class RetryInfo {

        @SerializedName("taskId")
        private long taskId;

        @SerializedName("fileNum")
        private int fileNum;

        @SerializedName("dataTime")
        private String dataTime;

        @SerializedName("fileList")
        private List<String> fileList;

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public int getFileNum() {
            return fileNum;
        }

        public void setFileNum(int fileNum) {
            this.fileNum = fileNum;
        }

        public String getDataTime() {
            return dataTime;
        }

        public void setDataTime(String dataTime) {
            this.dataTime = dataTime;
        }

        public List<String> getFileList() {
            return fileList;
        }

        public void setFileList(List<String> fileList) {
            this.fileList = fileList;
        }
    }

    public static class RetryUnfinishedFile {

        @SerializedName("taskId")
        private long taskId;

        @SerializedName("ufFileNum")
        private int ufFileNum;

        @SerializedName("dataTime")
        private String dataTime;

        @SerializedName("ufFileList")
        private List<String> ufFileList;

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public int getUfFileNum() {
            return ufFileNum;
        }

        public void setUfFileNum(int ufFileNum) {
            this.ufFileNum = ufFileNum;
        }

        public String getDataTime() {
            return dataTime;
        }

        public void setDataTime(String dataTime) {
            this.dataTime = dataTime;
        }

        public List<String> getUfFileList() {
            return ufFileList;
        }

        public void setUfFileList(List<String> ufFileList) {
            this.ufFileList = ufFileList;
        }
    }

    public static class ReadingInfo {

        @SerializedName("readingCount")
        private int readingCount;

        @SerializedName("readingDetails")
        private ReadingDetails readingDetails;

        public int getReadingCount() {
            return readingCount;
        }

        public void setReadingCount(int readingCount) {
            this.readingCount = readingCount;
        }

        public ReadingDetails getReadingDetails() {
            return readingDetails;
        }

        public void setReadingDetails(ReadingDetails readingDetails) {
            this.readingDetails = readingDetails;
        }
    }

    public static class ReadingDetails {

        @SerializedName("normalFile")
        private List<NormalFile> normalFile;

        @SerializedName("retryFile")
        private List<RetryFile> retryFile;

        public List<NormalFile> getNormalFile() {
            return normalFile;
        }

        public void setNormalFile(List<NormalFile> normalFile) {
            this.normalFile = normalFile;
        }

        public List<RetryFile> getRetryFile() {
            return retryFile;
        }

        public void setRetryFile(List<RetryFile> retryFile) {
            this.retryFile = retryFile;
        }
    }

    public static class NormalFile {

        @SerializedName("dataTime")
        private String dataTime;

        @SerializedName("taskId")
        private long taskId;

        @SerializedName("fileName")
        private String fileName;

        public String getDataTime() {
            return dataTime;
        }

        public void setDataTime(String dataTime) {
            this.dataTime = dataTime;
        }

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
    }

    public static class RetryFile {

        @SerializedName("dataTime")
        private String dataTime;

        @SerializedName("taskId")
        private long taskId;

        @SerializedName("fileNum")
        private int fileNum;

        @SerializedName("readingFileList")
        private List<String> readingFileList;

        public String getDataTime() {
            return dataTime;
        }

        public void setDataTime(String dataTime) {
            this.dataTime = dataTime;
        }

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public int getFileNum() {
            return fileNum;
        }

        public void setFileNum(int fileNum) {
            this.fileNum = fileNum;
        }

        public List<String> getReadingFileList() {
            return readingFileList;
        }

        public void setReadingFileList(List<String> readingFileList) {
            this.readingFileList = readingFileList;
        }
    }

}
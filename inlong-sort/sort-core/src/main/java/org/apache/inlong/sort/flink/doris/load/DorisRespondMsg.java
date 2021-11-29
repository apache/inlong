/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.doris.load;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

public class DorisRespondMsg {
    @SerializedName("TxnId")
    private int txnId;

    @SerializedName("Label")
    private String label;

    @SerializedName("Status")
    private String status;

    @SerializedName("ExistingJobStatus")
    private String existingJobStatus;

    @SerializedName("Message")
    private String message;

    @SerializedName("NumberTotalRows")
    private long numberTotalRows;

    @SerializedName("NumberLoadedRows")
    private long numberLoadedRows;

    @SerializedName("NumberFilteredRows")
    private int numberFilteredRows;

    @SerializedName("NumberUnselectedRows")
    private int numberUnselectedRows;

    @SerializedName("LoadBytes")
    private long loadBytes;

    @SerializedName("LoadTimeMs")
    private int loadTimeMs;

    @SerializedName("BeginTxnTimeMs")
    private int beginTxnTimeMs;

    @SerializedName("StreamLoadPutTimeMs")
    private int streamLoadPutTimeMs;

    @SerializedName("ReadDataTimeMs")
    private int readDataTimeMs;

    @SerializedName("WriteDataTimeMs")
    private int writeDataTimeMs;

    @SerializedName("CommitAndPublishTimeMs")
    private int commitAndPublishTimeMs;

    @SerializedName("ErrorURL")
    private String errorURL;

    public int getTxnId() {
        return txnId;
    }

    public void setTxnId(int txnId) {
        this.txnId = txnId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getExistingJobStatus() {
        return existingJobStatus;
    }

    public void setExistingJobStatus(String existingJobStatus) {
        this.existingJobStatus = existingJobStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getNumberTotalRows() {
        return numberTotalRows;
    }

    public void setNumberTotalRows(long numberTotalRows) {
        this.numberTotalRows = numberTotalRows;
    }

    public long getNumberLoadedRows() {
        return numberLoadedRows;
    }

    public void setNumberLoadedRows(long numberLoadedRows) {
        this.numberLoadedRows = numberLoadedRows;
    }

    public int getNumberFilteredRows() {
        return numberFilteredRows;
    }

    public void setNumberFilteredRows(int numberFilteredRows) {
        this.numberFilteredRows = numberFilteredRows;
    }

    public int getNumberUnselectedRows() {
        return numberUnselectedRows;
    }

    public void setNumberUnselectedRows(int numberUnselectedRows) {
        this.numberUnselectedRows = numberUnselectedRows;
    }

    public long getLoadBytes() {
        return loadBytes;
    }

    public void setLoadBytes(long loadBytes) {
        this.loadBytes = loadBytes;
    }

    public int getLoadTimeMs() {
        return loadTimeMs;
    }

    public void setLoadTimeMs(int loadTimeMs) {
        this.loadTimeMs = loadTimeMs;
    }

    public int getBeginTxnTimeMs() {
        return beginTxnTimeMs;
    }

    public void setBeginTxnTimeMs(int beginTxnTimeMs) {
        this.beginTxnTimeMs = beginTxnTimeMs;
    }

    public int getStreamLoadPutTimeMs() {
        return streamLoadPutTimeMs;
    }

    public void setStreamLoadPutTimeMs(int streamLoadPutTimeMs) {
        this.streamLoadPutTimeMs = streamLoadPutTimeMs;
    }

    public int getReadDataTimeMs() {
        return readDataTimeMs;
    }

    public void setReadDataTimeMs(int readDataTimeMs) {
        this.readDataTimeMs = readDataTimeMs;
    }

    public int getWriteDataTimeMs() {
        return writeDataTimeMs;
    }

    public void setWriteDataTimeMs(int writeDataTimeMs) {
        this.writeDataTimeMs = writeDataTimeMs;
    }

    public int getCommitAndPublishTimeMs() {
        return commitAndPublishTimeMs;
    }

    public void setCommitAndPublishTimeMs(int commitAndPublishTimeMs) {
        this.commitAndPublishTimeMs = commitAndPublishTimeMs;
    }

    public String getErrorURL() {
        return errorURL;
    }

    public void setErrorURL(String errorURL) {
        this.errorURL = errorURL;
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(this);

    }
}

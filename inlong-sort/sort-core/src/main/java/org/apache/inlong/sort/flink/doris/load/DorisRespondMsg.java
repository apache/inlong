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


public class DorisRespondMsg {
	private int TxnId;

	private String Label;

	private String Status;

	private String ExistingJobStatus;

	private String Message;

	private long NumberTotalRows;

	private long NumberLoadedRows;

	private int NumberFilteredRows;

	private int NumberUnselectedRows;

	private long LoadBytes;

	private int LoadTimeMs;

	private int BeginTxnTimeMs;

	private int StreamLoadPutTimeMs;

	private int ReadDataTimeMs;

	private int WriteDataTimeMs;

	private int CommitAndPublishTimeMs;

	private String ErrorURL;

	public int getTxnId() {
		return TxnId;
	}

	public void setTxnId(int txnId) {
		TxnId = txnId;
	}

	public String getLabel() {
		return Label;
	}

	public void setLabel(String label) {
		Label = label;
	}

	public String getStatus() {
		return Status;
	}

	public void setStatus(String status) {
		Status = status;
	}

	public String getExistingJobStatus() {
		return ExistingJobStatus;
	}

	public void setExistingJobStatus(String existingJobStatus) {
		ExistingJobStatus = existingJobStatus;
	}

	public String getMessage() {
		return Message;
	}

	public void setMessage(String message) {
		Message = message;
	}

	public long getNumberTotalRows() {
		return NumberTotalRows;
	}

	public void setNumberTotalRows(long numberTotalRows) {
		NumberTotalRows = numberTotalRows;
	}

	public long getNumberLoadedRows() {
		return NumberLoadedRows;
	}

	public void setNumberLoadedRows(long numberLoadedRows) {
		NumberLoadedRows = numberLoadedRows;
	}

	public int getNumberFilteredRows() {
		return NumberFilteredRows;
	}

	public void setNumberFilteredRows(int numberFilteredRows) {
		NumberFilteredRows = numberFilteredRows;
	}

	public int getNumberUnselectedRows() {
		return NumberUnselectedRows;
	}

	public void setNumberUnselectedRows(int numberUnselectedRows) {
		NumberUnselectedRows = numberUnselectedRows;
	}

	public long getLoadBytes() {
		return LoadBytes;
	}

	public void setLoadBytes(long loadBytes) {
		LoadBytes = loadBytes;
	}

	public int getLoadTimeMs() {
		return LoadTimeMs;
	}

	public void setLoadTimeMs(int loadTimeMs) {
		LoadTimeMs = loadTimeMs;
	}

	public int getBeginTxnTimeMs() {
		return BeginTxnTimeMs;
	}

	public void setBeginTxnTimeMs(int beginTxnTimeMs) {
		BeginTxnTimeMs = beginTxnTimeMs;
	}

	public int getStreamLoadPutTimeMs() {
		return StreamLoadPutTimeMs;
	}

	public void setStreamLoadPutTimeMs(int streamLoadPutTimeMs) {
		StreamLoadPutTimeMs = streamLoadPutTimeMs;
	}

	public int getReadDataTimeMs() {
		return ReadDataTimeMs;
	}

	public void setReadDataTimeMs(int readDataTimeMs) {
		ReadDataTimeMs = readDataTimeMs;
	}

	public int getWriteDataTimeMs() {
		return WriteDataTimeMs;
	}

	public void setWriteDataTimeMs(int writeDataTimeMs) {
		WriteDataTimeMs = writeDataTimeMs;
	}

	public int getCommitAndPublishTimeMs() {
		return CommitAndPublishTimeMs;
	}

	public void setCommitAndPublishTimeMs(int commitAndPublishTimeMs) {
		CommitAndPublishTimeMs = commitAndPublishTimeMs;
	}

	public String getErrorURL() {
		return ErrorURL;
	}

	public void setErrorURL(String errorURL) {
		ErrorURL = errorURL;
	}

	@Override
	public String toString() {
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		return gson.toJson(this);

	}
}

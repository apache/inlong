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

package org.apache.inlong.sdk.dataproxy.common;

/**
 * Process Result class
 *
 * Used to return the result of task processing
 */
public class ProcessResult {

    // error code
    private int errCode = ErrorCode.UNKNOWN_ERROR.getErrCode();
    // error message
    private String errMsg = ErrorCode.UNKNOWN_ERROR.getErrMsg();
    // return data if success
    private Object retData = null;

    public ProcessResult() {
        //
    }

    public ProcessResult(ErrorCode errCode) {
        this.errCode = errCode.getErrCode();
        this.errMsg = errCode.getErrMsg();
    }

    public ProcessResult(ErrorCode errCode, String errMsg) {
        this.errCode = errCode.getErrCode();
        this.errMsg = errMsg;
    }

    public boolean setSuccess() {
        this.errCode = ErrorCode.OK.getErrCode();
        this.errMsg = ErrorCode.OK.getErrMsg();
        this.retData = null;
        return isSuccess();
    }

    public boolean setSuccess(Object retData) {
        this.errCode = ErrorCode.OK.getErrCode();
        this.errMsg = ErrorCode.OK.getErrMsg();
        this.retData = retData;
        return isSuccess();
    }

    public boolean setFailResult(ProcessResult other) {
        this.errCode = other.getErrCode();
        this.errMsg = other.getErrMsg();
        this.retData = other.getRetData();
        return isSuccess();
    }

    public boolean setFailResult(ErrorCode errCode) {
        this.errCode = errCode.getErrCode();
        this.errMsg = errCode.getErrMsg();
        this.retData = null;
        return isSuccess();
    }

    public boolean setFailResult(ErrorCode errCode, String errMsg) {
        this.errCode = errCode.getErrCode();
        this.errMsg = errMsg;
        this.retData = null;
        return isSuccess();
    }

    public boolean isSuccess() {
        return (this.errCode == ErrorCode.OK.getErrCode());
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public Object getRetData() {
        return retData;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProcessResult{");
        sb.append("errCode=").append(errCode);
        sb.append(", errMsg='").append(errMsg).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

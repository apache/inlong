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

import org.apache.commons.lang3.math.NumberUtils;

/**
 * Error Code class
 *
 * Used to identify different types of errors
 */
public enum ErrorCode {

    OK(0, "Ok"),

    SDK_CLOSED(11, "SDK service closed"),
    //
    ILLEGAL_CALL_STATE(21, "Only allowed for meta query"),
    CONFIGURE_NOT_INITIALIZED(22, "Configure not initialized"),
    FREQUENT_RMT_FAILURE_VISIT(23, "Frequent manager failure visit"),

    // file visit
    LOCAL_FILE_NOT_EXIST(31, "Local file not exist"),
    LOCAL_FILE_EXPIRED(32, "Local file expired"),
    READ_LOCAL_FILE_FAILURE(33, "Read local file failure"),
    BLANK_FILE_CONTENT(34, "Blank file content"),
    PARSE_FILE_CONTENT_FAILURE(35, "Parse file content failure"),
    //
    PARSE_FILE_CONTENT_IS_NULL(36, "Parse file content is null"),

    // remote visit
    BUILD_HTTP_CLIENT_EXCEPTION(41, "Build http client exception"),
    HTTP_VISIT_EXCEPTION(42, "Visit http server exception"),
    RMT_RETURN_FAILURE(43, "Http server return failure"),
    RMT_RETURN_BLANK_CONTENT(44, "Http server return blank content"),
    PARSE_RMT_CONTENT_FAILURE(45, "Parse manager content failure"),
    //
    PARSE_RMT_CONTENT_IS_NULL(46, "Parse manager content is null"),
    RMT_RETURN_ERROR(47, "Manager return error info"),
    META_FIELD_DATA_IS_NULL(48, "Field data is null"),
    META_NODE_LIST_IS_EMPTY(49, "Field nodeList is empty"),
    NODE_LIST_RECORD_INVALID(50, "No valid nodeList records"),
    //
    PARSE_PROXY_META_EXCEPTION(51, "No valid nodeList records"),
    PARSE_ENCRYPT_META_EXCEPTION(52, "Parse encrypt content failure"),
    META_REQUIRED_FIELD_NOT_EXIST(53, "Required meta field not exist"),
    META_FIELD_VALUE_ILLEGAL(54, "Meta field value illegal"),

    //
    UNKNOWN_ERROR(9999, "Unknown error");

    public static ErrorCode valueOf(int value) {
        for (ErrorCode errCode : ErrorCode.values()) {
            if (errCode.getErrCode() == value) {
                return errCode;
            }
        }
        return UNKNOWN_ERROR;
    }

    public static String getErrMsg(String errCode) {
        int codeVal = NumberUtils.toInt(errCode, Integer.MAX_VALUE);
        return valueOf(codeVal).errMsg;
    }

    private final int errCode;
    private final String errMsg;

    ErrorCode(int errCode, String errMsg) {
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }
}

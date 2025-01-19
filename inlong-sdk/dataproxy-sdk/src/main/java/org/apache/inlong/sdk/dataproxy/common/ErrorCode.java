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

    ILLEGAL_CALL_STATE(21, "Only allowed for meta query"),
    CONFIGURE_NOT_INITIALIZED(22, "Configure not initialized"),
    FREQUENT_RMT_FAILURE_VISIT(23, "Frequent manager failure visit"),

    // file visit
    // err 31 ~ 35
    LOCAL_FILE_NOT_EXIST(31, "Local file not exist"),
    LOCAL_FILE_EXPIRED(32, "Local file expired"),
    READ_LOCAL_FILE_FAILURE(33, "Read local file failure"),
    BLANK_FILE_CONTENT(34, "Blank file content"),
    PARSE_FILE_CONTENT_FAILURE(35, "Parse file content failure"),
    // err 36 ~
    PARSE_FILE_CONTENT_IS_NULL(36, "Parse file content is null"),
    //
    // remote visit
    // err 41 ~ 45
    BUILD_HTTP_CLIENT_EXCEPTION(41, "Build http client exception"),
    HTTP_VISIT_EXCEPTION(42, "Visit http server exception"),
    RMT_RETURN_FAILURE(43, "Http server return failure"),
    RMT_RETURN_BLANK_CONTENT(44, "Http server return blank content"),
    PARSE_RMT_CONTENT_FAILURE(45, "Parse manager content failure"),
    // err 46 ~ 50
    PARSE_RMT_CONTENT_IS_NULL(46, "Parse manager content is null"),
    RMT_RETURN_ERROR(47, "Manager return error info"),
    META_FIELD_DATA_IS_NULL(48, "Field data is null"),
    META_NODE_LIST_IS_EMPTY(49, "Field nodeList is empty"),
    NODE_LIST_RECORD_INVALID(50, "No valid nodeList records"),
    // err 51 ~ 55
    PARSE_PROXY_META_EXCEPTION(51, "No valid nodeList records"),
    PARSE_ENCRYPT_META_EXCEPTION(52, "Parse encrypt content failure"),
    FETCH_PROXY_META_FAILURE(53, "Fetch dataproxy meta info failure"),
    FETCH_ENCRYPT_META_FAILURE(54, "Fetch encrypt meta info failure"),
    META_REQUIRED_FIELD_NOT_EXIST(55, "Required meta field not exist"),

    META_FIELD_VALUE_ILLEGAL(56, "Meta field value illegal"),

    PARAM_EVENTINFO_IS_NULL(12, "Parameter eventinfo is null"),
    PARAM_GROUP_ID_IS_BLANK(13, "GroupId is blank"),

    REPORT_INFO_EXCEED_MAX_LEN(101, "Report info exceed max allowed length"),
    ENCODE_BODY_EXCEPTION(102, "Encode body exception"),
    COMPRESS_BODY_EXCEPTION(103, "Compress body exception"),
    ENCRYPT_BODY_EXCEPTION(104, "Encrypt body exception"),
    GENERATE_SIGNATURE_EXCEPTION(105, "Generate signature exception"),

    NO_NODE_META_INFOS(111, "No proxy node metadata info in local"),
    EMPTY_ACTIVE_NODE_SET(112, "Empty active node set"),
    EMPTY_WRITABLE_NODE_SET(113, "Empty writable node set"),
    NO_VALID_REMOTE_NODE(114, "No valid remote node set"),
    CONNECTION_UNAVAILABLE(115, "Connection unavailable"),

    CONNECTION_BREAK(116, "Connection break"),
    CONNECTION_UNWRITABLE(117, "Connection unwritable"),
    CONNECTION_WRITE_EXCEPTION(118, "Connection write exception"),
    DUPLICATED_MESSAGE_ID(119, "Duplicated message id"),
    SEND_WAIT_INTERRUPT(120, "Send wait timeout"),

    SEND_WAIT_TIMEOUT(121, "Send wait timeout"),
    SEND_ON_EXCEPTION(122, "Send on exception"),

    DP_SINK_SERVICE_UNREADY(151, "DataProxy sink service unready"),
    DP_INVALID_ATTRS(152, "DataProxy return invalid attributes"),
    DP_EMPTY_BODY(153, "DataProxy return empty body"),
    DP_BODY_EXCEED_MAX_LEN(154, "DataProxy return body length over max"),
    DP_UNCONFIGURED_GROUPID_OR_STREAMID(155, "DataProxy return unconfigured groupId or streamId"),

    HTTP_ASYNC_POOL_FULL(171, "Http async pool full"),
    HTTP_ASYNC_OFFER_FAIL(172, "Http async offer event fail"),
    HTTP_ASYNC_OFFER_EXCEPTION(173, "Http async offer event exception"),
    HTTP_BUILD_CLIENT_EXCEPTION(174, "Http build client exception"),

    BUILD_FORM_CONTENT_EXCEPTION(181, "Build form content exception"),
    DP_RETURN_FAILURE(182, "DataProxy return failure"),
    HTTP_VISIT_DP_EXCEPTION(183, "Http visit exception"),
    DP_RETURN_UNKNOWN_ERROR(184, "DataProxy return unknown error"),

    DP_RECEIVE_FAILURE(191, "DataProxy return message receive failure"),

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

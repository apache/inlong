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

package org.apache.inlong.manager.common.enums;

public enum ErrorCodeEnum {
    AUTHORIZATION_FAILED(2001, "Authentication failed"),
    INVALID_PARAMETER(2002, "The parameter is invalid"),
    PERMISSION_REQUIRED(2003, "The current user does not have operation authority"),
    AUTHENTICATION_REQUIRED(2004, "Authentication failed"),

    USER_IS_NOT_MANAGER(110, "%s is not the manager, please contact %s"),

    GROUP_NOT_FOUND(1001, "Inlong group does not exist/no operation authority"),
    GROUP_DUPLICATE(1002, "Inlong group already exists"),
    GROUP_SAVE_FAILED(1003, "Failed to save/update inlong group information"),
    GROUP_PERMISSION_DENIED(1004, "No access to this inlong group"),
    GROUP_HAS_STREAM(1005, "There are some valid inlong stream for this inlong group"),
    GROUP_UPDATE_NOT_ALLOWED(1006, "The current inlong group status does not support modification"),
    GROUP_DELETE_NOT_ALLOWED(1007, "The current inlong group status does not support deletion"),
    GROUP_ID_UPDATE_NOT_ALLOWED(1008, "The current inlong group status does not support modifying the group id"),
    GROUP_MIDDLEWARE_UPDATE_NOT_ALLOWED(1011,
            "The current inlong group status does not support modifying the middleware type"),

    MIDDLEWARE_TYPE_NOT_SUPPORTED(1021, "MIDDLEWARE_TYPE_NOT_SUPPORTED"),

    CLUSTER_NOT_FOUND(1101, "Cluster information does not exist"),

    STREAM_NOT_FOUND(1201, "Inlong stream does not exist/no operation permission"),
    STREAM_ID_DUPLICATE(1202, "The current inlong group has a inlong stream with the same ID"),
    STREAM_OPT_NOT_ALLOWED(1203,
            "The current inlong group status does not allow adding/modifying/deleting inlong streams"),
    STREAM_ID_UPDATE_NOT_ALLOWED(1205,
            "The current inlong group status does not allow to modify the group or stream id"),
    STREAM_SOURCE_UPDATE_NOT_ALLOWED(1206,
            "The current inlong group status does not allow to modify the stream source type of the inlong stream"),
    STREAM_EXT_SAVE_FAILED(1207, "Failed to save/update inlong stream extension information"),
    STREAM_FIELD_SAVE_FAILED(1208, "Failed to save/update inlong stream field"),
    STREAM_DELETE_HAS_SOURCE(1209, "The inlong stream contains source info and is not allowed to be deleted"),
    STREAM_DELETE_HAS_SINK(1210, "The inlong stream contains data sink info and is not allowed to be deleted"),

    SOURCE_DUPLICATE(1301, "Stream source already exists"),
    SOURCE_BASIC_NOT_FOUND(1302, "The basic information of the stream source does not exist"),
    SOURCE_DETAIL_NOT_FOUND(1303, "Stream source info does not exist"),
    SOURCE_TYPE_NOT_SUPPORTED(1304, "Source type is not supported"),
    SOURCE_BASIC_DELETE_HAS_DETAIL(1305,
            "The stream source contains detailed info and is not allowed to be deleted"),
    SOURCE_OPT_NOT_ALLOWED(1306,
            "The current inlong group status does not allow adding/modifying/deleting stream source info"),

    HIVE_OPERATION_FAILED(1311, "Hive operation failed"),

    SINK_TYPE_IS_NULL(1400, "Sink type is null"),
    SINK_TYPE_NOT_SUPPORT(1401, "Sink type '%s' not support"),
    SINK_INFO_NOT_FOUND(1402, "Sink information does not exist/no operation authority"),
    SINK_INFO_INCORRECT(1402, "Sink information was incorrect"),
    SINK_ALREADY_EXISTS(1403, "Sink already exist with the groupId and streamId"),
    SINK_SAVE_FAILED(1404, "Failed to save or update sink info"),
    SINK_FIELD_SAVE_FAILED(1405, "Failed to save or update sink field"),
    SINK_OPT_NOT_ALLOWED(1406, "Current status does not allow add/modification/delete sink info"),
    SINK_DB_NAME_UPDATE_NOT_ALLOWED(1407, "Current status does not allow modification the database name"),
    SINK_TB_NAME_UPDATE_NOT_ALLOWED(1408, "Current status does not allow modification the table name"),
    SINK_FIELD_UPDATE_NOT_ALLOWED(1409, "Current status not allowed to modification/delete field"),

    WORKFLOW_EXE_FAILED(4000, "Workflow execution exception"),

    CONSUMER_GROUP_NAME_DUPLICATED(2600, "The consumer group already exists in the cluster"),
    CONSUMER_GROUP_CREATE_FAILED(2601, "Failed to create tube consumer group"),
    TUBE_GROUP_CREATE_FAILED(2602, "Create Tube consumer group failed"),
    PULSAR_GROUP_CREATE_FAILED(2603, "Create Pulsar consumer group failed"),
    TUBE_TOPIC_CREATE_FAILED(2604, "CreateTube Topic failed"),
    PULSAR_TOPIC_CREATE_FAILED(2605, "Create Pulsar Topic failed"),
    PULSAR_DLQ_RLQ_ERROR(2606, "Wrong config for the RLQ and DLQ: RLQ was enabled, but the DLQ was disabled"),
    PULSAR_DLQ_DUPLICATED(2607, "DLQ topic already exists under the inlong group"),
    PULSAR_RLQ_DUPLICATED(2608, "RLQ topic already exists under the inlong group"),

    COMMON_FILE_DOWNLOAD_FAIL(6001, "File download failed"),
    COMMON_FILE_UPLOAD_FAIL(6002, "File upload failed"),
    ;

    private final int code;
    private final String message;

    ErrorCodeEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}

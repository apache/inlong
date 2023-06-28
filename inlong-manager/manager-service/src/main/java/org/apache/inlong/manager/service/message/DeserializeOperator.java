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

package org.apache.inlong.manager.service.message;

import org.apache.inlong.common.enums.DataProxyMsgEncType;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import java.util.List;
import java.util.Map;

/**
 * Deserialize of message operator
 */
public interface DeserializeOperator {

    public static final String COMPRESS_TYPE_KEY = "compressType";
    public static final String NODE_IP = "NodeIP";
    public static final String MSG_TIME_KEY = "msgTime";
    public static final char INLONGMSG_ATTR_ENTRY_DELIMITER = '&';
    public static final char INLONGMSG_ATTR_KV_DELIMITER = '=';

    // keys in attributes
    public static final String INLONGMSG_ATTR_STREAM_ID = "streamId";
    public static final String INLONGMSG_ATTR_TIME_T = "t";
    public static final String INLONGMSG_ATTR_TIME_DT = "dt";
    public static final String INLONGMSG_ATTR_ADD_COLUMN_PREFIX = "__addcol";

    public static final String FORMAT_TIME_FIELD_NAME = "format.time-field-name";
    public static final String FORMAT_ATTRIBUTES_FIELD_NAME = "format.attributes-field-name";

    public static final String DEFAULT_TIME_FIELD_NAME = "inlongmsg_time";
    public static final String DEFAULT_ATTRIBUTES_FIELD_NAME = "inlongmsg_attributes";

    /**
     * Determines whether the current instance matches the specified type.
     */
    boolean accept(DataProxyMsgEncType type);

    default List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo,
            byte[] msgBytes, Map<String, String> headers, int index) throws Exception {
        return null;
    }

}

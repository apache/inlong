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

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.sdk.dataproxy.exception.ProxyEventException;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Report Event information class
 *
 * Used to encapsulate the data information reported by the caller, including
 *  grouId, streamId, dt, attributes, and body, auditVerison, msgUUID, etc.
 * This class performs field value validity checks on the reported data and
 *  throws ProxyEventException for data that does not meet the reporting requirements, including
 *  mandatory fields that are empty, attribute sets that contain reserved words, attribute delimiters,
 *  empty message bodies, etc.
 * Since the TCP and HTTP reports supported by the SDK differ only in the
 *  message body type, this class uses a Generics definition
 */
public abstract class EventInfo<T> {

    protected static final Logger logger = LoggerFactory.getLogger(EventInfo.class);
    protected static final LogCounter exceptCnt = new LogCounter(10, 100000, 60 * 1000L);

    private final String groupId;
    private final String streamId;
    private final long dtMs;
    private final Map<String, String> attrs = new HashMap<>();
    protected int msgCnt = 0;
    protected int bodySize = 0;
    protected final List<T> bodyList = new ArrayList<>();

    protected EventInfo(String groupId, String streamId, long dtMs, Long auditId, String msgUUID,
            Map<String, String> attrs, boolean isSingle, List<T> bodyList) throws ProxyEventException {
        // groupId
        if (StringUtils.isBlank(groupId)) {
            throw new ProxyEventException("groupId is blank!");
        }
        this.groupId = groupId.trim();
        // streamId
        if (StringUtils.isBlank(streamId)) {
            throw new ProxyEventException("streamId is blank!");
        }
        this.streamId = streamId.trim();
        // dtMs
        this.dtMs = dtMs <= 0L ? System.currentTimeMillis() : dtMs;
        // attrs
        if (attrs != null && !attrs.isEmpty()) {
            for (Map.Entry<String, String> entry : attrs.entrySet()) {
                if (entry == null
                        || StringUtils.isBlank(entry.getKey())
                        || entry.getValue() == null) {
                    continue;
                }
                innSetAttr(entry.getKey().trim(), entry.getValue().trim());
            }
        }
        if (auditId != null && auditId != -1L) {
            this.attrs.put(AttributeConstants.AUDIT_VERSION, String.valueOf(auditId));
        }
        if (StringUtils.isNotBlank(msgUUID)) {
            this.attrs.put(AttributeConstants.MSG_UUID, msgUUID.trim());
        }
        // body
        setBodyList(isSingle, bodyList);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getDtMs() {
        return dtMs;
    }

    public Map<String, String> getAttrs() {
        return attrs;
    }

    public int getMsgCnt() {
        return msgCnt;
    }

    public int getBodySize() {
        return bodySize;
    }

    protected abstract void setBodyList(boolean isSingle, List<T> bodyList) throws ProxyEventException;

    protected void innSetAttr(String key, String value) throws ProxyEventException {
        if (ProxyUtils.SdkReservedWords.contains(key)) {
            throw new ProxyEventException("Attribute key(" + key + ") is reserved word!");
        }
        if (key.contains(AttributeConstants.SEPARATOR)
                || key.contains(AttributeConstants.KEY_VALUE_SEPARATOR)) {
            if (exceptCnt.shouldPrint()) {
                logger.warn(String.format("Attribute key(%s) include reserved word(%s or %s)",
                        key, AttributeConstants.KEY_VALUE_SEPARATOR, AttributeConstants.KEY_VALUE_SEPARATOR));
            }
            throw new ProxyEventException("Attribute key(" + key + ") include reserved word("
                    + AttributeConstants.KEY_VALUE_SEPARATOR + " or "
                    + AttributeConstants.KEY_VALUE_SEPARATOR + ")!");
        }
        if (value.contains(AttributeConstants.SEPARATOR)
                || value.contains(AttributeConstants.KEY_VALUE_SEPARATOR)) {
            if (exceptCnt.shouldPrint()) {
                logger.warn(String.format("Attribute value(%s) include reserved word(%s or %s)",
                        value, AttributeConstants.KEY_VALUE_SEPARATOR, AttributeConstants.KEY_VALUE_SEPARATOR));
            }
            throw new ProxyEventException("Attribute value(" + value + ") include reserved word("
                    + AttributeConstants.KEY_VALUE_SEPARATOR + " or "
                    + AttributeConstants.KEY_VALUE_SEPARATOR + ")!");
        }
        this.attrs.put(key, value);
    }
}

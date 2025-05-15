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

package org.apache.inlong.sdk.dataproxy.sender.tcp;

import org.apache.inlong.sdk.dataproxy.common.EventInfo;
import org.apache.inlong.sdk.dataproxy.exception.ProxyEventException;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TCP Event Information class
 *
 * Used to encapsulate the data information reported by TCP
 */
public class TcpEventInfo extends EventInfo<byte[]> {

    public TcpEventInfo(String groupId, String streamId, long dtMs,
            Map<String, String> attrs, byte[] body) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, null, attrs, true, Collections.singletonList(body));
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs, long auditId,
            Map<String, String> attrs, byte[] body) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, null, attrs, true, Collections.singletonList(body));
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs, String msgUUID,
            Map<String, String> attrs, byte[] body) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, msgUUID, attrs, true, Collections.singletonList(body));
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs, long auditId, String msgUUID,
            Map<String, String> attrs, byte[] body) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, msgUUID, attrs, true, Collections.singletonList(body));
    }

    public TcpEventInfo(String groupId, String streamId,
            long dtMs, Map<String, String> attrs, List<byte[]> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, null, attrs, false, bodyList);
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs,
            long auditId, Map<String, String> attrs, List<byte[]> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, null, attrs, false, bodyList);
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs,
            String msgUUID, Map<String, String> attrs, List<byte[]> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, msgUUID, attrs, false, bodyList);
    }

    public TcpEventInfo(String groupId, String streamId, long dtMs,
            long auditId, String msgUUID, Map<String, String> attrs, List<byte[]> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, msgUUID, attrs, false, bodyList);
    }

    public List<byte[]> getBodyList() {
        return bodyList;
    }

    public void setAttr(String key, String value) throws ProxyEventException {
        if (StringUtils.isBlank(key)) {
            throw new ProxyEventException("Parameter key is blank!");
        }
        if (value == null) {
            throw new ProxyEventException("Parameter value is null!");
        }
        innSetAttr(key.trim(), value.trim());
    }

    @Override
    protected void setBodyList(boolean isSingle, List<byte[]> bodyList) throws ProxyEventException {
        if (isSingle) {
            if (bodyList.get(0) == null || bodyList.get(0).length == 0) {
                throw new ProxyEventException("body is null or empty!");
            }
            this.bodyList.add(bodyList.get(0));
            this.bodySize = bodyList.get(0).length;
            this.msgCnt = 1;
        } else {
            if (bodyList == null || bodyList.isEmpty()) {
                throw new ProxyEventException("bodyList is null or empty!");
            }
            for (byte[] body : bodyList) {
                if (body == null || body.length == 0) {
                    continue;
                }
                this.bodyList.add(body);
                this.bodySize += body.length;
                this.msgCnt++;
            }
            if (this.bodyList.isEmpty()) {
                throw new ProxyEventException("bodyList no valid content!");
            }
        }
    }
}

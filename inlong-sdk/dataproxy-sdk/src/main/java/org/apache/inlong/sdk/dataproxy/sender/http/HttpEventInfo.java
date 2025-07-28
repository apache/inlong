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

package org.apache.inlong.sdk.dataproxy.sender.http;

import org.apache.inlong.sdk.dataproxy.common.EventInfo;
import org.apache.inlong.sdk.dataproxy.exception.ProxyEventException;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import org.apache.inlong.sdk.transform.encode.EscapeUtils;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfigHolder;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * HTTP Event Information class
 *
 * Used to encapsulate the data information reported by HTTP
 */
public class HttpEventInfo extends EventInfo<String> {

    public HttpEventInfo(String groupId, String streamId,
            long dtMs, String body) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, null, null, true, Collections.singletonList(body));
    }

    public HttpEventInfo(String groupId, String streamId,
            long dtMs, long auditId, String body) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, null, null, true, Collections.singletonList(body));
    }

    public HttpEventInfo(String groupId, String streamId,
            long dtMs, List<String> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, null, null, null, false, bodyList);
    }

    public HttpEventInfo(String groupId, String streamId,
            long dtMs, long auditId, List<String> bodyList) throws ProxyEventException {
        super(groupId, streamId, dtMs, auditId, null, null, false, bodyList);
    }

    public List<String> getBodyList() {
        return bodyList;
    }

    @Override
    protected void setBodyList(boolean isSingle, List<String> bodyList) throws ProxyEventException {
        String tmpValue;
        ProxyClientConfig config = ProxyClientConfigHolder.getConfig();
        boolean autoEscape = config != null && config.isAutoEscape();
        List<String> escapeFields = config != null ? config.getAutoEscapeFields() : null;
        if (isSingle) {
            if (StringUtils.isBlank(bodyList.get(0))) {
                throw new ProxyEventException("body is null or empty!");
            }
            tmpValue = bodyList.get(0).trim();
            if (autoEscape) {
                tmpValue = escapeBody(tmpValue, escapeFields);
            }
            this.bodyList.add(tmpValue);
            this.bodySize = tmpValue.length();
            this.msgCnt = 1;
        } else {
            if (bodyList == null || bodyList.isEmpty()) {
                throw new ProxyEventException("bodyList is null or empty!");
            }
            for (String body : bodyList) {
                if (StringUtils.isBlank(body)) {
                    continue;
                }
                tmpValue = body.trim();
                if (autoEscape) {
                    tmpValue = escapeBody(tmpValue, escapeFields);
                }
                this.bodyList.add(tmpValue);
                this.bodySize += tmpValue.length();
                this.msgCnt++;
            }
            if (this.bodyList.isEmpty()) {
                throw new ProxyEventException("bodyList no valid content!");
            }
        }
    }

    /**
     * 按字段名或整体进行转义
     */
    private String escapeBody(String body, List<String> escapeFields) {
        // 尝试解析为JSON，若成功则按字段名转义，否则整体转义
        try {
            JSONObject json = JSON.parseObject(body);
            if (escapeFields != null && !escapeFields.isEmpty()) {
                for (String field : escapeFields) {
                    if (json.containsKey(field)) {
                        String val = json.getString(field);
                        if (val != null) {
                            json.put(field, EscapeUtils.escapeStringSeparator(val, '|'));
                        }
                    }
                }
                return json.toJSONString();
            }
        } catch (Exception e) {
            // 非JSON结构，忽略
        }
        // 非结构化或未指定字段，整体转义
        return EscapeUtils.escapeStringSeparator(body, '|');
    }
}

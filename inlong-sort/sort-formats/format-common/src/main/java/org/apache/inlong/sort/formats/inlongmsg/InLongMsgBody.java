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

package org.apache.inlong.sort.formats.inlongmsg;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The body deserialized from {@link InLongMsgBody}.
 */
@Data
public class InLongMsgBody implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The body of the record.
     */
    private final byte[] dataBytes;

    /**
     * The body of the record.
     */
    private final String data;

    /**
     * The interface of the record.
     */
    private final String streamId;

    /**
     * The fields extracted from the body.
     */
    private final List<String> fields;

    /**
     * The entries extracted from the body.
     */
    private final Map<String, String> entries;

    public InLongMsgBody(
            byte[] dataBytes,
            String data,
            String streamId,
            List<String> fields,
            Map<String, String> entries) {
        this.dataBytes = dataBytes;
        this.data = data;
        this.streamId = streamId;
        this.fields = fields;
        this.entries = entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InLongMsgBody inLongMsgBody = (InLongMsgBody) o;
        return StringUtils.equals(data, inLongMsgBody.data)
                && Arrays.equals(dataBytes, inLongMsgBody.dataBytes);
    }

    @Override
    public int hashCode() {
        if (dataBytes != null) {
            return Arrays.hashCode(dataBytes);
        }
        return data == null ? super.hashCode() : data.hashCode();
    }

    @Override
    public String toString() {
        return "InLongMsgBody{" + "data=" + (data == null ? new String(dataBytes) : data)
                + ", streamId='" + streamId + '\''
                + ", fields=" + fields + ", entries=" + entries + '}';
    }
}

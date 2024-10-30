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

package org.apache.inlong.sdk.transform.encode;

import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;

/**
 * CsvSinkEncoder
 */
public class CsvSinkEncoder extends SinkEncoder<String> {

    protected CsvSinkInfo sinkInfo;
    protected Charset sinkCharset = Charset.defaultCharset();
    private Character delimiter = '|';
    private Character escapeChar = null;
    private StringBuilder builder = new StringBuilder();

    public CsvSinkEncoder(CsvSinkInfo sinkInfo) {
        super(sinkInfo.getFields());
        this.sinkInfo = sinkInfo;
        if (sinkInfo.getDelimiter() != null) {
            this.delimiter = sinkInfo.getDelimiter();
        }
        if (sinkInfo.getDelimiter() != null) {
            this.escapeChar = sinkInfo.getEscapeChar();
        }
        if (!StringUtils.isBlank(sinkInfo.getCharset())) {
            this.sinkCharset = Charset.forName(sinkInfo.getCharset());
        }
    }

    /**
     * encode
     * @param sinkData
     * @return
     */
    @Override
    public String encode(SinkData sinkData, Context context) {
        builder.delete(0, builder.length());
        if (fields == null || fields.size() == 0) {
            if (escapeChar == null) {
                sinkData.keyList().forEach(k -> builder.append(sinkData.getField(k)).append(delimiter));
            } else {
                for (String fieldName : sinkData.keyList()) {
                    String fieldValue = sinkData.getField(fieldName);
                    if (StringUtils.equals(fieldName, ALL_SOURCE_FIELD_SIGN)) {
                        builder.append(fieldValue);
                    } else {
                        EscapeUtils.escapeContent(builder, delimiter, escapeChar, fieldValue);
                    }
                    builder.append(delimiter);
                }
            }
        } else {
            if (escapeChar == null) {
                fields.forEach(v -> builder.append(sinkData.getField(v.getName())).append(delimiter));
            } else {
                for (FieldInfo field : fields) {
                    String fieldName = field.getName();
                    String fieldValue = sinkData.getField(fieldName);
                    EscapeUtils.escapeContent(builder, delimiter, escapeChar, fieldValue);
                    builder.append(delimiter);
                }
            }
        }
        return builder.substring(0, builder.length() - 1);
    }

}

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

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * KvSourceDecoder
 * 
 */
public class KvSourceDecoder extends SourceDecoder<String> {

    protected KvSourceInfo sourceInfo;
    private Character entryDelimiter = '&';
    private Character kvDelimiter = '=';
    private Character escapeChar = '\\';
    private Character quoteChar = '\"';
    private Character lineDelimiter = '\n';
    private Charset srcCharset = Charset.defaultCharset();

    public KvSourceDecoder(KvSourceInfo sourceInfo) {
        super(sourceInfo.getFields());
        this.sourceInfo = sourceInfo;
        if (!StringUtils.isBlank(sourceInfo.getCharset())) {
            this.srcCharset = Charset.forName(sourceInfo.getCharset());
        }
        if (sourceInfo.getEntryDelimiter() != null) {
            this.entryDelimiter = sourceInfo.getEntryDelimiter();
        }
        if (sourceInfo.getKvDelimiter() != null) {
            this.kvDelimiter = sourceInfo.getKvDelimiter();
        }
        if (sourceInfo.getEscapeChar() != null) {
            this.escapeChar = sourceInfo.getEscapeChar();
        }
        if (sourceInfo.getQuoteChar() != null) {
            this.quoteChar = sourceInfo.getQuoteChar();
        }
        if (sourceInfo.getLineDelimiter() != null) {
            this.lineDelimiter = sourceInfo.getLineDelimiter();
        }
    }

    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        String srcString = new String(srcBytes, srcCharset);
        return this.decode(srcString, context);
    }

    @Override
    public SourceData decode(String srcString, Context context) {
        List<Map<String, String>> rowValues = KvUtils.splitKv(srcString, entryDelimiter, kvDelimiter,
                escapeChar, quoteChar, lineDelimiter);
        KvSourceData sourceData = new KvSourceData();
        if (CollectionUtils.isEmpty(fields)) {
            for (Map<String, String> row : rowValues) {
                sourceData.addRow();
                row.forEach(sourceData::putField);
            }
            return sourceData;
        }
        for (Map<String, String> row : rowValues) {
            sourceData.addRow();
            for (FieldInfo field : fields) {
                String fieldName = field.getName();
                String fieldValue = row.get(fieldName);
                sourceData.putField(fieldName, fieldValue);
            }
        }
        return sourceData;
    }
}

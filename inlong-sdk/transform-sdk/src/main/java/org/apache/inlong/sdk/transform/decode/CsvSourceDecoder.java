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

import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;

/**
 * CsvSourceDecoder
 * 
 */
public class CsvSourceDecoder extends SourceDecoder<String> {

    protected CsvSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private Character delimiter = '|';
    private Character escapeChar = null;

    public CsvSourceDecoder(CsvSourceInfo sourceInfo) {
        super(sourceInfo.getFields());
        this.sourceInfo = sourceInfo;
        if (sourceInfo.getDelimiter() != null) {
            this.delimiter = sourceInfo.getDelimiter();
        }
        if (sourceInfo.getEscapeChar() != null) {
            this.escapeChar = sourceInfo.getEscapeChar();
        }
        if (!StringUtils.isBlank(sourceInfo.getCharset())) {
            this.srcCharset = Charset.forName(sourceInfo.getCharset());
        }
    }

    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        String srcString = new String(srcBytes, srcCharset);
        return this.decode(srcString, context);
    }

    @Override
    public SourceData decode(String srcString, Context context) {
        String[][] rowValues = SplitUtils.splitCsv(srcString, delimiter, escapeChar, '\"', '\n', true);
        CsvSourceData sourceData = new CsvSourceData();
        for (int i = 0; i < rowValues.length; i++) {
            String[] fieldValues = rowValues[i];
            sourceData.addRow();
            if (CollectionUtils.isEmpty(fields)) {
                for (int j = 0; j < fieldValues.length; j++) {
                    String fieldName = SourceData.FIELD_DEFAULT_PREFIX + (j + 1);
                    sourceData.putField(fieldName, fieldValues[j]);
                }
                continue;
            }
            int fieldIndex = 0;
            for (FieldInfo field : fields) {
                String fieldName = field.getName();
                Object fieldValue = null;
                if (fieldIndex < fieldValues.length) {
                    try {
                        fieldValue = field.getConverter().convert(fieldValues[fieldIndex]);
                    } catch (Exception e) {

                    }
                }
                sourceData.putField(fieldName, fieldValue);
                fieldIndex++;
            }
        }
        return sourceData;
    }
}

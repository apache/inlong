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

package org.apache.inlong.sort.formats.inlongmsgtlogcsv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgDeserializationSchema;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgFormatDeserializer;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;

/**
 *  Deserialization schema from  InLongMsg TlogCsv to Flink Table & SQL internal data structures.
 */
public class InLongMsgTlogCsvDeserializationSchema extends AbstractInLongMsgDeserializationSchema {

    public InLongMsgTlogCsvDeserializationSchema(AbstractInLongMsgFormatDeserializer formatDeserializer) {
        super(formatDeserializer);
    }

    /**
     * A builder for creating a {@link InLongMsgTlogCsvDeserializationSchema}.
     */
    @PublicEvolving
    public static class Builder {

        private final RowFormatInfo rowFormatInfo;

        private String timeFieldName = DEFAULT_TIME_FIELD_NAME;
        private String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
        private String charset = DEFAULT_CHARSET;
        private Character delimiter = DEFAULT_DELIMITER;
        private Character escapeChar = DEFAULT_ESCAPE_CHARACTER;
        private Character quoteChar = DEFAULT_QUOTE_CHARACTER;
        private String nullLiteral = DEFAULT_NULL_LITERAL;
        private boolean ignoreErrors = false;
        private List<String> metadataKeys = Collections.emptyList();

        protected Builder(RowFormatInfo rowFormatInfo) {
            this.rowFormatInfo = rowFormatInfo;
        }

        public Builder setTimeFieldName(String timeFieldName) {
            this.timeFieldName = timeFieldName;
            return this;
        }

        public Builder setAttributesFieldName(String attributesFieldName) {
            this.attributesFieldName = attributesFieldName;
            return this;
        }

        public Builder setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setEscapeCharacter(char escapeChar) {
            this.escapeChar = escapeChar;
            return this;
        }

        public Builder setQuoteCharacter(char quoteChar) {
            this.quoteChar = quoteChar;
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            this.nullLiteral = nullLiteral;
            return this;
        }

        public Builder setIgnoreErrors(boolean ignoreErrors) {
            this.ignoreErrors = ignoreErrors;
            return this;
        }

        public Builder setMetadataKeys(List<String> metadataKeys) {
            this.metadataKeys = metadataKeys;
            return this;
        }

        public InLongMsgTlogCsvDeserializationSchema build() {
            AbstractInLongMsgFormatDeserializer formatDeserializer = new InLongMsgTlogCsvFormatDeserializer(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    charset,
                    delimiter,
                    escapeChar,
                    quoteChar,
                    nullLiteral,
                    metadataKeys,
                    ignoreErrors);

            return new InLongMsgTlogCsvDeserializationSchema(formatDeserializer);
        }
    }
}

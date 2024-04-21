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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgDeserializationSchema;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgFormatDeserializer;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;

/**
 * Deserialization schema from inlong-msg-binlog to Flink Table & SQL internal data structures.
 */
public class InLongMsgBinlogRowDataDeserializationSchema extends AbstractInLongMsgDeserializationSchema {

    public InLongMsgBinlogRowDataDeserializationSchema(AbstractInLongMsgFormatDeserializer formatDeserializer) {
        super(formatDeserializer);
    }

    /**
     * A builder for creating a {@link InLongMsgBinlogRowDataDeserializationSchema}.
     */
    @PublicEvolving
    public static class Builder {

        private final RowFormatInfo rowFormatInfo;
        private String timeFieldName = DEFAULT_TIME_FIELD_NAME;
        private String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
        private String metadataFieldName = DEFAULT_METADATA_FIELD_NAME;
        private boolean includeUpdateBefore = DEFAULT_INCLUDE_UPDATE_BEFORE;
        private boolean ignoreErrors = false;

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

        public Builder setMetadataFieldName(String metadataFieldName) {
            this.metadataFieldName = metadataFieldName;
            return this;
        }

        public Builder setIncludeUpdateBefore(boolean includeUpdateBefore) {
            this.includeUpdateBefore = includeUpdateBefore;
            return this;
        }

        public Builder setIgnoreErrors(boolean ignoreErrors) {
            this.ignoreErrors = ignoreErrors;
            return this;
        }

        public InLongMsgBinlogRowDataDeserializationSchema build() {
            AbstractInLongMsgFormatDeserializer formatDeserializer = new InLongMsgBinlogFormatDeserializer(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    metadataFieldName,
                    ignoreErrors,
                    includeUpdateBefore);

            return new InLongMsgBinlogRowDataDeserializationSchema(formatDeserializer);
        }
    }
}

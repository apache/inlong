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

package org.apache.inlong.sort.formats.inlongmsgkv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgMixedFormatConverterBuilder;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converter used to deserialize a mixed row in inlong-msg-kv format.
 */
public class InLongMsgKvMixedFormatConverter extends AbstractInLongMsgMixedFormatConverter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(InLongMsgKvMixedFormatConverter.class);

    /**
     * The schema of the rows.
     */
    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    /**
     * The name of the time field.
     */
    @Nullable
    private final String timeFieldName;

    /**
     * The name of the attributes field.
     */
    @Nullable
    private final String attributesFieldName;

    /**
     * The literal representing null values.
     */
    private final String nullLiteral;

    public InLongMsgKvMixedFormatConverter(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String nullLiteral,
            boolean ignoreErrors) {
        super(ignoreErrors);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.nullLiteral = nullLiteral;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.decorateRowTypeWithNeededHeadFields(timeFieldName, attributesFieldName, rowFormatInfo);
    }

    @Override
    public List<Row> convertRows(
            Map<String, String> attributes,
            byte[] data,
            String streamId,
            Timestamp time,
            List<String> predefinedFields,
            List<String> fields,
            Map<String, String> entries) throws Exception {
        Row dataRow =
                InLongMsgKvUtils.deserializeRow(
                        rowFormatInfo,
                        nullLiteral,
                        predefinedFields,
                        entries);

        Row row = InLongMsgUtils.decorateRowWithNeededHeadFields(
                timeFieldName,
                attributesFieldName,
                time,
                attributes,
                dataRow);

        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgKvMixedFormatConverter}.
     */
    public static class Builder extends InLongMsgMixedFormatConverterBuilder {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public InLongMsgKvMixedFormatConverter build() {
            return new InLongMsgKvMixedFormatConverter(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    nullLiteral,
                    ignoreErrors);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        if (!super.equals(object)) {
            return false;
        }

        InLongMsgKvMixedFormatConverter that = (InLongMsgKvMixedFormatConverter) object;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                Objects.equals(attributesFieldName, that.attributesFieldName) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, timeFieldName, attributesFieldName, nullLiteral);
    }
}
